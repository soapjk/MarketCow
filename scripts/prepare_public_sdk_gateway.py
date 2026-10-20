#!/usr/bin/env python3
"""Render isolated Caddy/SSH candidates; never install, reload or contact a host.

The Caddy fragment must be imported by the existing Caddyfile after validation.
Keys, known_hosts and certificates are provisioned separately. No secrets are
embedded here; bearer authentication and quotas remain at the SDK listener.
"""
import argparse
from pathlib import Path
import re


def safe_path(value):
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+", value) or ".." in Path(value).parts:
        raise ValueError("expected absolute path without whitespace or traversal")
    return value


def render(domain, cloud_host, ssh_user, identity, known_hosts, local_port=18793):
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", domain) or "." not in domain:
        raise ValueError("invalid domain")
    if not re.fullmatch(r"[A-Za-z0-9.-]+", cloud_host):
        raise ValueError("invalid cloud host")
    if not re.fullmatch(r"[a-z_][a-z0-9_-]*", ssh_user) or ssh_user == "root":
        raise ValueError("dedicated non-root SSH user required")
    for path in (identity, known_hosts):
        safe_path(path)
    if not 1024 <= local_port <= 65535:
        raise ValueError("invalid local SDK port")
    routes = (
        "/v3/btc/capabilities /v3/btc/health /sdk/v1/usage "
        "/v3/btc/bars/1s /v3/btc/bars/1m "
        "/v3/btc/trade-history /v3/btc/trade-history/resolve /v3/btc/trade-history/session "
        "/v3/btc/observation-history /v3/btc/observation-history/resolve /v3/btc/observation-history/session "
        "/v7/polymarket/btc-5m/market-streams /v7/polymarket/btc-5m/rules "
        "/v1/facts/polymarket/reference /v1/facts/polymarket/finality"
    )
    # No catch-all proxy, cache, encoding, request-body logs, query token or
    # redirects to any legacy API. Incoming Authorization passes unchanged.
    def proxy(port):
        return f"""reverse_proxy 127.0.0.1:{port} {{
            flush_interval -1
            transport http {{
                dial_timeout 3s
                response_header_timeout 150s
            }}
        }}"""
    caddy = f"""# Import from the existing Caddyfile; do not replace its other sites.
{domain} {{
    header Cache-Control no-store
    request_body {{
        max_size 64KB
    }}
    @price path /v3/btc/price-stream
    handle @price {{
        {proxy(28791)}
    }}
    @book path /v7/polymarket/btc-5m/market-stream
    handle @book {{
        {proxy(28792)}
    }}
    @facts path {routes}
    handle @facts {{
        {proxy(28793)}
    }}
    handle {{
        respond 403
    }}
}}
"""
    files = {"marketcow-public.caddy": caddy}
    for name, port in (("price", 28791), ("book", 28792), ("query", 28793)):
        files[f"marketcow-sdk-tunnel-{name}.service"] = f"""[Unit]
Description=MarketCow SDK {name} private reverse channel
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=120
StartLimitBurst=10

[Service]
Type=simple
# Install as the U1 runtime user's systemd --user service, not as root.
ExecStart=/usr/bin/ssh -NT -p 22 -i {identity} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile={known_hosts} -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o ConnectTimeout=10 -o TCPKeepAlive=no -o ControlMaster=no -o Compression=no -R 127.0.0.1:{port}:127.0.0.1:{local_port} {ssh_user}@{cloud_host}
Restart=on-failure
RestartSec=5
NoNewPrivileges=yes
PrivateTmp=yes
UMask=0077

[Install]
WantedBy=default.target
"""
    files["sshd-marketcow-sdk.conf"] = f"""# Include from cloud /etc/ssh/sshd_config.d; validate with sshd -t first.
Match User {ssh_user}
    AuthenticationMethods publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    AllowTcpForwarding remote
    PermitListen 127.0.0.1:28791 127.0.0.1:28792 127.0.0.1:28793
    GatewayPorts no
    AllowAgentForwarding no
    X11Forwarding no
    PermitTTY no
    PermitTunnel no
    MaxSessions 0
Match all
"""
    return files


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--domain", default="marketcow.llmay.cn")
    p.add_argument("--cloud-host", default="47.94.144.23")
    p.add_argument("--ssh-user", default="marketcow-sdk")
    p.add_argument("--identity", required=True)
    p.add_argument("--known-hosts", required=True)
    p.add_argument("--sdk-port", type=int, default=18793)
    args = p.parse_args()
    files = render(args.domain, args.cloud_host, args.ssh_user, args.identity,
                   args.known_hosts, args.sdk_port)
    args.output.mkdir(mode=0o700)  # A fresh candidate, never overwrite live config.
    for name, content in files.items():
        with (args.output / name).open("x") as f:
            f.write(content)
    print(f"rendered {len(files)} candidate files; nothing installed or activated")


if __name__ == "__main__":
    main()
