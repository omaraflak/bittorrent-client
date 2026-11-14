import unittest
from bittorrent.bencode import bencode, decode_bencode, decode_bencode_with_raw


class TestBencode(unittest.TestCase):
    def test_string(self):
        self.assertEqual(bencode(b'omar'), b'4:omar')

    def test_empty_string(self):
        self.assertEqual(bencode(b''), b'0:')

    def test_unicode_string(self):
        self.assertEqual(bencode('café'), b'5:caf\xc3\xa9')

    def test_int(self):
        self.assertEqual(bencode(12345), b'i12345e')

    def test_int_negative(self):
        self.assertEqual(bencode(-12345), b'i-12345e')

    def test_int_zero(self):
        self.assertEqual(bencode(0), b'i0e')

    def test_bool(self):
        self.assertEqual(bencode(True), b'i1e')
        self.assertEqual(bencode(False), b'i0e')

    def test_list(self):
        self.assertEqual(bencode([1, 2, 3]), b'li1ei2ei3ee')

    def test_list_empty(self):
        self.assertEqual(bencode([]), b'le')

    def test_list_multiple_types(self):
        self.assertEqual(bencode([1, b'a', [2]]), b'li1e1:ali2eee')

    def test_dict(self):
        self.assertEqual(bencode({'a': b'omar'}), b'd1:a4:omare')

    def test_dict_empty(self):
        self.assertEqual(bencode({}), b'de')

    def test_dict_multiple_types(self):
        self.assertEqual(bencode({'a': 1, 'b': [1, {'c': 0}]}), b'd1:ai1e1:bli1ed1:ci0eeee')

    def test_dict_keys_sorted(self):
        # BEP 3 requirement: dictionary keys must be sorted lexicographically
        data = {'z': 1, 'b': 2, 'a': 3}
        self.assertEqual(bencode(data), b'd1:ai3e1:bi2e1:zi1ee')


class TestDecodeBencode(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(decode_bencode(b''), b'')

    def test_string(self):
        self.assertEqual(decode_bencode(b'4:omar'), b'omar')

    def test_empty_string(self):
        self.assertEqual(decode_bencode(b'0:'), b'')

    def test_int(self):
        self.assertEqual(decode_bencode(b'i12345e'), 12345)

    def test_negative_int(self):
        self.assertEqual(decode_bencode(b'i-12345e'), -12345)

    def test_empty_int(self):
        self.assertEqual(decode_bencode(b'ie'), 0)

    def test_list(self):
        self.assertEqual(decode_bencode(b'li1ei2ei3ee'), [1, 2, 3])

    def test_empty_list(self):
        self.assertEqual(decode_bencode(b'le'), [])

    def test_list_multiple_types(self):
        self.assertEqual(decode_bencode(b'li1ei2ei3e4:omarl1:a1:b0:ee'), [1, 2, 3, b'omar', [b'a', b'b', b'']])

    def test_list_of_list(self):
        self.assertEqual(decode_bencode(b'llee'), [[]])

    def test_dict(self):
        self.assertEqual(decode_bencode(b'd4:omari12345ee'), {'omar': 12345})

    def test_empty_dict(self):
        self.assertEqual(decode_bencode(b'de'), {})

    def test_dict_multiple_types(self):
        self.assertEqual(decode_bencode(b'd4:omari12345e1:ali1ei2eee'), {'omar': 12345, 'a': [1, 2]})

    def test_dict_of_dict(self):
        self.assertEqual(decode_bencode(b'd1:adee'), {'a': {}})

    def test_dict_without_str_key(self):
        self.assertRaises(ValueError, lambda: decode_bencode(b'di1ei2ee'))

    def test_raw_slices(self):
        raw = b'd4:infod4:name4:test12:piece lengthi16384ee4:spam4:eggse'
        decoded, raw_slices = decode_bencode_with_raw(raw)
        self.assertEqual(decoded['spam'], b'eggs')
        self.assertEqual(raw_slices['info'], b'd4:name4:test12:piece lengthi16384ee')

    def test_malformed_truncated_string(self):
        self.assertRaises(ValueError, lambda: decode_bencode(b'10:short'))

    def test_malformed_unterminated_list(self):
        self.assertRaises(ValueError, lambda: decode_bencode(b'li1ei2e'))

    def test_malformed_unterminated_dict(self):
        self.assertRaises(ValueError, lambda: decode_bencode(b'd1:a1:b'))


if __name__ == '__main__':
    unittest.main()
