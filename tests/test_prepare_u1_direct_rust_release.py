import unittest
from scripts.prepare_u1_direct_rust_release import replace_one


class UnitBindingTests(unittest.TestCase):
    def test_exact_binding_only(self):
        self.assertEqual(replace_one('before old after', 'old', 'new'), 'before new after')
        for text in ('no occurrence', 'old old'):
            with self.assertRaises(ValueError):
                replace_one(text, 'old', 'new')


if __name__ == '__main__':
    unittest.main()
