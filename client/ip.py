from dataclasses import dataclass


@dataclass
class IpAndPort:
    ip: str
    port: int