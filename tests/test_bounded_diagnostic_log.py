import pathlib
import subprocess
import sys
import tempfile
import unittest


class BoundedLogTest(unittest.TestCase):
    def test_rotation_and_child_failure_are_preserved(self):
        script = pathlib.Path(__file__).resolve().parents[1] / 'scripts/run_with_bounded_log.py'
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / 'test.log'
            result = subprocess.run([sys.executable,str(script),'--log',str(path),'--max-bytes','65536','--',
                sys.executable,'-c','import os; os.write(1,b"x"*1048576); raise SystemExit(7)'])
            self.assertEqual(result.returncode,7)
            logs = list(path.parent.iterdir())
            self.assertEqual(len(logs),3)
            self.assertTrue(all(p.stat().st_size <= 65536 for p in logs))
