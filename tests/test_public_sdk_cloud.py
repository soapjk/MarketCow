import ast
import unittest
from scripts.deploy_public_sdk_gateway import remote_script
from scripts.prepare_public_sdk_gateway import render


class CloudTests(unittest.TestCase):
    def test_all_actions_parse_and_never_ship_customer_or_private_key(self):
        files=render('marketcow.llmay.cn','47.94.144.23','marketcow-sdk','/private/id','/private/hosts')
        for action in ('stage','activate','rollback'):
            s=remote_script(action,files,'ssh-ed25519 AAAA test-public')
            body=s.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
            ast.parse(body)
            self.assertNotIn('api-key',s)
            self.assertNotIn('BEGIN OPENSSH PRIVATE KEY',s)
            self.assertNotIn('marketcow-polymarket-collector.service',s)
            self.assertIn('external Caddy edit; refuse overwrite',s)
            self.assertIn('Gateway',files['sshd-marketcow-sdk.conf'])


if __name__=='__main__':unittest.main()
