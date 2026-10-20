#!/usr/bin/env python3
"""Fixed-instance public SDK gateway stage/activate/rollback via Aliyun CLI.

Never sends an SDK API key or private SSH key to Cloud Assistant. Stage adds
only a forwarding-only account. Activate imports a separate Caddy site after
validating it alongside the original sites; rollback restores only our files.
"""
import argparse
import base64
import json
from pathlib import Path
import subprocess
import time
if __package__:
    from scripts.prepare_public_sdk_gateway import render
else:
    from prepare_public_sdk_gateway import render

INSTANCE = "i-2ze3hctkte6xc99wg0l2"
REGION = "cn-beijing"


def cloud(script, name, receipt_dir):
    if len(script.encode()) > 18_000:
        raise ValueError("cloud script budget")
    def cli(*args):
        p = subprocess.run(["aliyun", "ecs", *args, "--RegionId", REGION],
                           check=True, text=True, capture_output=True, timeout=40)
        return json.loads(p.stdout)
    invocation = cli("RunCommand", "--InstanceId.1", INSTANCE, "--Type", "RunShellScript",
                     "--Name", name, "--Timeout", "45", "--ContentEncoding", "Base64",
                     "--CommandContent", base64.b64encode(script.encode()).decode())
    receipt_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    receipt = receipt_dir / (invocation["InvokeId"] + ".json")
    receipt.write_text(json.dumps(invocation, indent=2))
    receipt.chmod(0o600)
    deadline = time.monotonic() + 70
    while time.monotonic() < deadline:
        result = cli("DescribeInvocationResults", "--InvokeId", invocation["InvokeId"],
                     "--ContentEncoding", "PlainText")
        records = result.get("Invocation", {}).get("InvocationResults", {}).get("InvocationResult", [])
        if records and records[0].get("InvocationStatus") in ("Success", "Failed", "Stopped", "Timeout"):
            receipt.write_text(json.dumps({"invocation": invocation, "result": result}, indent=2))
            record = records[0]
            print(record.get("Output", ""), end="")
            print("cloud_receipt", receipt)
            if record.get("ExitCode") != 0 or record.get("InvocationStatus") != "Success":
                raise RuntimeError("cloud action failed; inspect retained receipt, do not infer success")
            return
        time.sleep(1)
    raise TimeoutError(f"cloud result unknown; reuse InvokeId {invocation['InvokeId']}, do not blindly rerun")


def remote_script(action, files, public_key):
    config = {"action": action, "files": {k: v for k, v in files.items()
              if k in ("marketcow-public.caddy", "sshd-marketcow-sdk.conf")}, "public_key": public_key}
    encoded = base64.b64encode(json.dumps(config).encode()).decode()
    return "#!/bin/sh\nset -eu\npython3 - <<'PY'\n" + "CONFIG='" + encoded + "'\n" + r'''
import base64,json,os,pwd,subprocess,stat,urllib.request,urllib.error,fcntl
from pathlib import Path
c=json.loads(base64.b64decode(CONFIG))
base=Path('/var/lib/marketcow-public-gateway-r1')
base.mkdir(mode=0o700,exist_ok=True)
assert not base.is_symlink() and base.stat().st_uid==0
lock=(base/'deployment.lock').open('a')
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
def run(*args,**kw):return subprocess.run(list(args),check=True,capture_output=True,text=True,timeout=20,**kw)
def save(path,data,mode=0o600):
    temporary=path.with_name(path.name+'.marketcow-new')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,mode)
    with os.fdopen(fd,'wb') as f:
        f.write(data);f.flush();os.fsync(f.fileno())
    os.replace(temporary,path)
def backup(path,name):
    target=base/name
    if not target.exists():
        assert path.is_file() and not path.is_symlink()
        with target.open('xb') as f:f.write(path.read_bytes())
        target.chmod(0o600)
    return target.read_bytes()
sshd=Path('/etc/ssh/sshd_config.d/81-marketcow-sdk.conf')
caddy=Path('/etc/caddy/Caddyfile')
fragment=Path('/etc/caddy/marketcow-sdk-public.caddy')
marker=b'import /etc/caddy/marketcow-sdk-public.caddy\n'
action=c['action']
if action=='stage':
    if not (base/'Caddyfile.original').exists():
        assert not fragment.exists() and not sshd.exists(), 'unowned candidate path exists'
    backup(caddy,'Caddyfile.original')
    try:user=pwd.getpwnam('marketcow-sdk')
    except KeyError:
        run('useradd','--system','--create-home','--home-dir','/var/lib/marketcow-sdk','--shell','/bin/sh','marketcow-sdk')
        run('usermod','--password','*','marketcow-sdk')
        user=pwd.getpwnam('marketcow-sdk')
    assert user.pw_dir=='/var/lib/marketcow-sdk' and user.pw_uid!=0
    home=Path(user.pw_dir);ssh=home/'.ssh'
    ssh.mkdir(mode=0o700,exist_ok=True)
    assert not ssh.is_symlink()
    auth=ssh/'authorized_keys'
    key=c['public_key'].strip()
    assert key.startswith('ssh-ed25519 ') and len(key.split())>=2 and '\n' not in key
    body=('restrict,port-forwarding,permitlisten="127.0.0.1:28791",permitlisten="127.0.0.1:28792",permitlisten="127.0.0.1:28793" '+key+'\n').encode()
    if auth.exists():assert auth.read_bytes()==body, 'unowned authorized_keys'
    else:save(auth,body)
    for p in (home,ssh,auth):os.chown(p,user.pw_uid,user.pw_gid)
    ssh.chmod(0o700);auth.chmod(0o600)
    body=c['files']['sshd-marketcow-sdk.conf'].encode()
    if sshd.exists():assert sshd.read_bytes()==body, 'SSH match drift'
    else:save(sshd,body,0o644)
    run('sshd','-t')
    run('systemctl','reload','ssh')
    print('forward_only_account_ready true')
    print('host_public_key',Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().strip())
elif action=='activate':
    assert (base/'Caddyfile.original').is_file(), 'stage required'
    # No public-facing site is enabled until all private origin channels work.
    ports=(28792,28793) if 'reverse_proxy 127.0.0.1:28791' not in c['files']['marketcow-public.caddy'] else (28791,28792,28793)
    for port in ports:
        try:urllib.request.urlopen('http://127.0.0.1:%d/v3/btc/health'%port,timeout=4)
        except urllib.error.HTTPError as e:assert e.code==401, ('origin not SDK auth boundary',port,e.code)
        else:raise RuntimeError('anonymous origin unexpectedly accepted')
    pid=run('systemctl','show','caddy','-p','MainPID','--value').stdout.strip()
    env={}
    for item in Path('/proc/'+pid+'/environ').read_bytes().split(b'\0'):
        if b'=' in item:
            k,v=item.split(b'=',1);env[os.fsdecode(k)]=os.fsdecode(v)
    original=(base/'Caddyfile.original').read_bytes()
    expected=original+b'\n'+marker
    mode=stat.S_IMODE(caddy.stat().st_mode);gid=caddy.stat().st_gid
    assert caddy.read_bytes() in (original,expected), 'existing website config changed since staging'
    body=c['files']['marketcow-public.caddy'].encode()
    # Combined validation in memory; do not expose the existing site's config.
    run('/proc/'+pid+'/exe','adapt','--config','/dev/stdin','--adapter','caddyfile',
        input=(original+b'\n'+body).decode(),env=env)
    if fragment.exists():assert fragment.read_bytes()==body, 'SDK fragment drift'
    else:save(fragment,body,0o640);os.chown(fragment,0,caddy.stat().st_gid)
    if caddy.read_bytes()!=expected:
        mode=stat.S_IMODE(caddy.stat().st_mode);gid=caddy.stat().st_gid
        save(caddy,expected,mode);os.chown(caddy,0,gid)
    try:
        run('/proc/'+pid+'/exe','reload','--config',str(caddy),env=env)
    except Exception:
        save(caddy,original,mode);os.chown(caddy,0,gid);raise
    print('sdk_virtual_host_activated true')
elif action=='rollback':
    original=(base/'Caddyfile.original').read_bytes()
    expected=original+b'\n'+marker
    assert caddy.read_bytes() in (original,expected), 'external Caddy edit; refuse overwrite'
    pid=run('systemctl','show','caddy','-p','MainPID','--value').stdout.strip()
    env={os.fsdecode(k):os.fsdecode(v) for item in Path('/proc/'+pid+'/environ').read_bytes().split(b'\0') if b'=' in item for k,v in [item.split(b'=',1)]}
    mode=stat.S_IMODE(caddy.stat().st_mode);gid=caddy.stat().st_gid
    save(caddy,original,mode);os.chown(caddy,0,gid)
    run('/proc/'+pid+'/exe','reload','--config',str(caddy),env=env)
    print('sdk_virtual_host_removed original_sites_retained')
else:raise ValueError('unknown action')
print('action',action,'no_sdk_key_on_cloud true')
PY
'''


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=('stage','activate','rollback'))
    p.add_argument('--public-key',type=Path)
    p.add_argument('--receipt-dir',type=Path,required=True)
    p.add_argument('--share-price-query',action='store_true',help='explicit two-channel mode; book remains isolated')
    args=p.parse_args()
    key=args.public_key.read_text().strip() if args.public_key else ''
    if args.action=='stage' and (not key.startswith('ssh-ed25519 ') or '\n' in key):
        p.error('stage requires one ed25519 public key, never a private key')
    files=render('marketcow.llmay.cn','47.94.144.23','marketcow-sdk',
                 '/mnt/p44pro/marketcow-shadow-v3-runtime/linux/deployment/private/sdk-cloud-ed25519',
                 '/mnt/p44pro/marketcow-shadow-v3-runtime/linux/deployment/private/sdk-cloud-known-hosts')
    if args.share_price_query:
        files['marketcow-public.caddy']=files['marketcow-public.caddy'].replace('reverse_proxy 127.0.0.1:28791','reverse_proxy 127.0.0.1:28793')
    cloud(remote_script(args.action,files,key),'marketcow-public-sdk-'+args.action,args.receipt_dir)


if __name__=='__main__':main()
