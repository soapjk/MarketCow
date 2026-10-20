#!/usr/bin/env python3
"""Install only the three SDK reverse channels; never restart the collector."""
import argparse
import base64
import json
from pathlib import Path
import re
import subprocess
if __package__:
    from scripts.prepare_public_sdk_gateway import render
else:
    from prepare_public_sdk_gateway import render


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cloud-stage-receipt',type=Path,required=True)
    p.add_argument('--host',default='czx@192.168.124.3')
    p.add_argument('--price-ssh-port',type=int,choices=(22,2222),default=22)
    args=p.parse_args()
    receipt=json.loads(args.cloud_stage_receipt.read_text())
    record=receipt['result']['Invocation']['InvocationResults']['InvocationResult'][0]
    assert record['InvocationStatus']=='Success' and record['ExitCode']==0
    keys=[line.removeprefix('host_public_key ') for line in record['Output'].splitlines() if line.startswith('host_public_key ')]
    if len(keys)!=1 or not re.fullmatch(r'ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]+)?',keys[0]):
        raise ValueError('exact Cloud Assistant host identity required')
    root='/mnt/p44pro/marketcow-shadow-v3-runtime/linux/deployment/private'
    files=render('marketcow.llmay.cn','47.94.144.23','marketcow-sdk',
                 root+'/sdk-cloud-ed25519',root+'/sdk-cloud-known-hosts')
    units={k:v for k,v in files.items() if k.endswith('.service')}
    original_units=units.copy()
    price='marketcow-sdk-tunnel-price.service'
    units[price]=units[price].replace('-p 22 ',f'-p {args.price_ssh_port} ')
    old_host='47.94.144.23 '+keys[0]+'\n'
    new_host=old_host+('[47.94.144.23]:2222 '+keys[0]+'\n' if args.price_ssh_port==2222 else '')
    payload=base64.b64encode(json.dumps({'units':units,'old_units':original_units,'known_host':new_host,'old_host':old_host}).encode()).decode()
    script="CONFIG='"+payload+"'\n"+r'''
import base64,json,os,subprocess
from pathlib import Path
c=json.loads(base64.b64decode(CONFIG))
private=Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/deployment/private')
assert private.is_dir() and not private.is_symlink() and private.stat().st_mode&0o077==0
host=private/'sdk-cloud-known-hosts'
changed=[]
def install(path,body,previous=None):
    if path.exists():
        assert not path.is_symlink() and path.read_text() in (body,previous), 'unowned/drifting file; refuse overwrite'
        if path.read_text()!=body:
            path.write_text(body);changed.append(path.name)
    else:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:f.write(body)
install(host,c['known_host'],c['old_host'])
directory=Path.home()/'.config/systemd/user'
for name,body in c['units'].items():
    assert name in ('marketcow-sdk-tunnel-price.service','marketcow-sdk-tunnel-book.service','marketcow-sdk-tunnel-query.service')
    install(directory/name,body,c['old_units'][name])
subprocess.run(['systemctl','--user','daemon-reload'],check=True)
subprocess.run(['systemctl','--user','enable','--now',*c['units']],check=True)
for name in changed:
    if name.endswith('.service'):subprocess.run(['systemctl','--user','restart',name],check=True)
for name in c['units']:
    print(name)
    subprocess.run(['systemctl','--user','show',name,'-p','MainPID','-p','ActiveState','-p','NRestarts'],check=True)
'''
    subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',args.host,'python3 -'],input=script,text=True,check=True)


if __name__=='__main__':main()
