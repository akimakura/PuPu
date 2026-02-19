import socket
from typing import Optional


def get_ip_address_by_dns_name(dns_name: str) -> Optional[str]:
    """
    Получить ip адресс по имени домена
    """
    try:
        ip_address = socket.gethostbyname(dns_name)
    except Exception:  # noqa
        ip_address = "unknown"
    return ip_address
