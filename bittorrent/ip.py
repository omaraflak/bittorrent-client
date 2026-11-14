from dataclasses import dataclass


@dataclass(frozen=True)
class IpAndPort:
    ip: str
    port: int

    def __hash__(self) -> int:
        return hash((self.ip, self.port))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IpAndPort):
            return False
        return self.ip == other.ip and self.port == other.port

    def __str__(self) -> str:
        return f'{self.ip}:{self.port}'
