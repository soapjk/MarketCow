#!/usr/bin/env python3
"""Provision one Binance-only U1 proxy from the Mac's saved FlClash selection.

No subscription URLs or unrelated nodes are uploaded. No global DNS/TUN changes.
"""
import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import yaml


def configuration(root):
    config = yaml.safe_load((root / 'config.yaml').read_text())
    with sqlite3.connect(f'file:{root / "database.sqlite"}?mode=ro', uri=True) as db:
        choices = [json.loads(row[0]) for row in db.execute('SELECT selected_map FROM profiles')]
    selected = {c.get('手动选择') for c in choices if c.get('手动选择')}
    if len(selected) != 1:
        raise RuntimeError('Mac manual selection is absent or ambiguous; do not guess a node')
    name = selected.pop()
    nodes = [n for n in config['proxies'] if n['name'] == name]
    if len(nodes) != 1:
        raise RuntimeError('selected Mac node is not uniquely present')
    dns = config['dns']
    return {
        'mixed-port': 17892, 'bind-address': '127.0.0.1', 'allow-lan': False,
        'mode': 'rule', 'log-level': 'warning', 'ipv6': False,
        'external-controller': '', 'tun': {'enable': False},
        'dns': {'enable': True, 'listen': '127.0.0.1:10592',
                'enhanced-mode': 'redir-host', 'ipv6': False,
                'nameserver': dns['nameserver'],
                'default-nameserver': dns['default-nameserver'],
                'proxy-server-nameserver': dns.get('proxy-server-nameserver') or dns['nameserver']},
        'proxies': nodes,
        'proxy-groups': [{'name': 'BINANCE', 'type': 'select', 'proxies': [name]}],
        'rules': [f'DOMAIN,{host},BINANCE' for host in (
            'data-api.binance.vision', 'data-stream.binance.vision', 'api.binance.com',
            'api-gcp.binance.com', 'api1.binance.com', 'api2.binance.com',
            'api3.binance.com', 'api4.binance.com')] + ['MATCH,REJECT'],
    }, name


REMOTE = r'''
import json,os,pathlib,subprocess,tempfile,time
root=pathlib.Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
if not root.is_mount():raise RuntimeError('runtime volume is not mounted')
config=json.load(__import__('sys').stdin)
directory=root/'binance-proxy'
if directory.is_symlink():raise RuntimeError('proxy directory must not be a symlink')
directory.mkdir(mode=0o700,exist_ok=True);directory.chmod(0o700)
candidate=directory/'config.new.json';target=directory/'config.json'
for path in [candidate,target]:
 if path.is_symlink():raise RuntimeError('unsafe proxy config symlink')
with candidate.open('w') as f:os.fchmod(f.fileno(),0o600);json.dump(config,f,ensure_ascii=False)
binary=root/'polymarket-proxy/mihomo'
result=subprocess.run([str(binary),'-t','-d',str(directory),'-f',str(candidate)],capture_output=True)
if result.returncode:raise RuntimeError('isolated proxy configuration validation failed')
if target.exists():
 backup=directory/('config.prior-'+str(time.time_ns())+'.json')
 backup.write_bytes(target.read_bytes());backup.chmod(0o600)
candidate.replace(target)
unit=pathlib.Path.home()/'.config/systemd/user/marketcow-binance-proxy.service'
if unit.is_symlink():raise RuntimeError('unsafe proxy unit symlink')
unit.write_text('[Unit]\nDescription=MarketCow isolated Binance proxy\nRequiresMountsFor='+str(root)+'\n\n[Service]\nExecStart=/usr/bin/python3 '+str(root/'deployment/current/run_with_bounded_log.py')+' --log '+str(directory/'proxy.log')+' --max-bytes 8388608 -- '+str(binary)+' -d '+str(directory)+' -f '+str(target)+'\nRestart=on-failure\nRestartSec=3\nNoNewPrivileges=yes\n\n[Install]\nWantedBy=default.target\n')
unit.chmod(0o600)
subprocess.run(['systemctl','--user','daemon-reload'],check=True)
subprocess.run(['systemctl','--user','enable','--now','marketcow-binance-proxy.service'],check=True,capture_output=True)
print(json.dumps({'unit':'marketcow-binance-proxy.service','proxy':'http://127.0.0.1:17892','node':config['proxies'][0]['name'],'global_dns_changed':False,'tun_enabled':False}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='czx@192.168.124.3')
    args = parser.parse_args()
    config, _ = configuration(Path.home() / 'Library/Application Support/com.follow.clash')
    subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', args.host,
                    'python3 -c ' + __import__('shlex').quote(REMOTE)],
                   input=json.dumps(config).encode(), check=True)


if __name__ == '__main__':
    main()
