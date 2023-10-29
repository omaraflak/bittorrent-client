from dataclasses import dataclass


@dataclass
class IpAndPort:
    ip: str
    port: int

    def __hash__(self) -> int:
        return hash((self.ip, self.port))