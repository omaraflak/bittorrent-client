from typing import Union

BenType = Union[int, bytes, list['BenType'], dict[str, 'BenType']]


def bencode(data: BenType) -> bytes:
    if isinstance(data, int):
        return f'i{data}e'.encode()

    if isinstance(data, str):
        return f'{len(data)}:{data}'.encode()

    if isinstance(data, bytes):
        return f'{len(data)}:'.encode() + data

    if isinstance(data, list):
        values = b''.join(bencode(x) for x in data)
        return b'l' + values + b'e'

    if isinstance(data, dict):
        values = b''.join(bencode(k) + bencode(v) for k, v in data.items())
        return b'd' + values + b'e'

    raise ValueError(f'Type {type(data)} not supported.')


def decode_bencode(data: bytes) -> BenType:
    value, size = _decode_bencode(data)
    assert size == len(data)
    return value


def _decode_bencode(data: bytes, start: int = 0) -> tuple[BenType, int]:
    if len(data) == 0:
        return (b'', 0)

    char = chr(data[start])

    if char.isdigit():
        idx = data.find(':'.encode(), start)
        length = int(data[start:idx])
        value = data[idx + 1:idx + 1 + length]
        return (value, idx + length + 1)

    if char == 'i':
        idx = data.find('e'.encode(), start)
        value = int(data[start + 1:idx] or 0)
        return (value, idx + 1)

    if char == 'l':
        _list: list[BenType] = list()
        start += 1
        while chr(data[start]) != 'e':
            value, start = _decode_bencode(data, start)
            _list.append(value)
        return (_list, start + 1)

    if char == 'd':
        _dict: dict[str, BenType] = {}
        start += 1
        while chr(data[start]) != 'e':
            key, start = _decode_bencode(data, start)
            if not isinstance(key, bytes):
                raise ValueError('Dict key should be a bytes string.')
            value, start = _decode_bencode(data, start)
            _dict[key.decode()] = value
        return (_dict, start + 1)

    raise ValueError(f'Character "{char}" not recognized.')
