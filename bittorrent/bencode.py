from typing import Union

BenType = Union[int, bytes, list['BenType'], dict[str, 'BenType']]


def bencode(data: BenType) -> bytes:
    if isinstance(data, bool):
        # In Python bool is a subclass of int; bencode doesn't have bool, encode as 1 or 0
        return b'i1e' if data else b'i0e'

    if isinstance(data, int):
        return f'i{data}e'.encode('ascii')

    if isinstance(data, str):
        encoded = data.encode('utf-8')
        return f'{len(encoded)}:'.encode('ascii') + encoded

    if isinstance(data, bytes):
        return f'{len(data)}:'.encode('ascii') + data

    if isinstance(data, (list, tuple)):
        values = b''.join(bencode(x) for x in data)
        return b'l' + values + b'e'

    if isinstance(data, dict):
        # BEP 3: Keys must appear in sorted order (strings are sorted as raw byte strings)
        def key_bytes(k: Union[str, bytes]) -> bytes:
            if isinstance(k, bytes):
                return k
            if isinstance(k, str):
                return k.encode('utf-8')
            raise ValueError(f'Dict key must be str or bytes, got {type(k)}')

        sorted_items = sorted(
            data.items(), key=lambda item: key_bytes(item[0]))
        values = b''.join(bencode(k) + bencode(v) for k, v in sorted_items)
        return b'd' + values + b'e'

    raise ValueError(f'Type {type(data)} not supported.')


def decode_bencode(data: bytes) -> BenType:
    if len(data) == 0:
        return b''
    value, end_idx = _decode_bencode(data, 0)
    if end_idx != len(data):
        raise ValueError(
            f'Trailing data after bencode decode: decoded {end_idx} of {len(data)} bytes')
    return value


def decode_bencode_with_raw(data: bytes) -> tuple[BenType, dict[str, bytes]]:
    """
    Decodes bencoded data and returns a tuple of:
    (decoded_value, raw_slices)
    where raw_slices is a dict mapping top-level dictionary string keys to their exact raw bytes in data.
    """
    if len(data) == 0:
        return b'', {}
    value, end_idx, raw_slices = _decode_bencode_with_raw_map(data, 0)
    if end_idx != len(data):
        raise ValueError(
            f'Trailing data after bencode decode: decoded {end_idx} of {len(data)} bytes')
    return value, raw_slices


def _decode_bencode(data: bytes, start: int = 0) -> tuple[BenType, int]:
    val, end_idx, _ = _decode_bencode_with_raw_map(data, start)
    return val, end_idx


def _decode_bencode_with_raw_map(data: bytes, start: int = 0) -> tuple[BenType, int, dict[str, bytes]]:
    if start >= len(data):
        return b'', start, {}

    char = chr(data[start])

    if char.isdigit():
        idx = data.find(b':', start)
        if idx == -1:
            raise ValueError(
                f'Missing colon delimiter for string length at offset {start}')
        try:
            length = int(data[start:idx])
        except ValueError as e:
            raise ValueError(f'Invalid string length at offset {start}: {e}')
        if length < 0:
            raise ValueError(
                f'Negative string length at offset {start}: {length}')
        str_start = idx + 1
        str_end = str_start + length
        if str_end > len(data):
            raise ValueError(
                f'Unexpected EOF: expected {length} bytes at {str_start}, available {len(data) - str_start}')
        value = data[str_start:str_end]
        return value, str_end, {}

    if char == 'i':
        idx = data.find(b'e', start)
        if idx == -1:
            raise ValueError(
                f'Missing end delimiter "e" for integer at offset {start}')
        raw_int = data[start + 1:idx]
        if len(raw_int) == 0:
            # For backward compatibility with tests expecting empty int 'ie' -> 0
            return 0, idx + 1, {}
        try:
            value = int(raw_int)
        except ValueError as e:
            raise ValueError(f'Invalid integer at offset {start}: {e}')
        return value, idx + 1, {}

    if char == 'l':
        _list: list[BenType] = []
        pos = start + 1
        while pos < len(data) and chr(data[pos]) != 'e':
            val, pos = _decode_bencode(data, pos)
            _list.append(val)
        if pos >= len(data):
            raise ValueError(f'Unterminated list starting at offset {start}')
        return _list, pos + 1, {}

    if char == 'd':
        _dict: dict[str, BenType] = {}
        raw_slices: dict[str, bytes] = {}
        pos = start + 1
        while pos < len(data) and chr(data[pos]) != 'e':
            val_key, pos = _decode_bencode(data, pos)
            if not isinstance(val_key, bytes):
                raise ValueError(
                    f'Dict key at offset {pos} must be bytes, got {type(val_key)}')
            key_str = val_key.decode('utf-8', errors='replace')
            val_start = pos
            val, pos = _decode_bencode(data, pos)
            _dict[key_str] = val
            raw_slices[key_str] = data[val_start:pos]

        if pos >= len(data):
            raise ValueError(
                f'Unterminated dictionary starting at offset {start}')
        return _dict, pos + 1, raw_slices

    raise ValueError(
        f'Character "{char}" at offset {start} not recognized in bencode.')
