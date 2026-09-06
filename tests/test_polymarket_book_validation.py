import os
from pathlib import Path
import subprocess
import sys
import unittest

from marketcow.polymarket_book_validation import _validate_book


class BookValidationTests(unittest.TestCase):
    def test_complete_and_empty_books_keep_existing_rules(self):
        _validate_book({"tick_size":"0.01","bids":{"0.4":"10"},"asks":{"0.6":"20"}})
        _validate_book({"tick_size":"0.01","bids":{},"asks":{}})

    def test_invalid_levels_are_rejected(self):
        for bid, ask, size in [("0.405","0.6","10"),("0.6","0.6","10"),("0.4","0.6","-1")]:
            with self.subTest(bid=bid, ask=ask, size=size), self.assertRaises(ValueError):
                _validate_book({"tick_size":"0.01","bids":{bid:size},"asks":{ask:"10"}})

    def test_live_read_import_does_not_load_offline_arrow(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        subprocess.run([sys.executable, "-c", "import sys; import marketcow.polymarket_live; assert 'pyarrow' not in sys.modules"],
                       check=True, env=env, timeout=30)
