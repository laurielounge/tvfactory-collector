# utils/ip.py

import ipaddress
from ipaddress import ip_address

from config.settings import settings
from core.logger import logger


def format_ipv4_as_mapped_ipv6(ip: str) -> str:
    """
    Converts an IPv4 address into an IPv6-mapped IPv4 address format as a string.
    Returns the string representation of the IP address.

    Example:
        Input:  '192.168.1.1'
        Output: '::ffff:192.168.1.1'
    """
    try:
        # Use ip_address to handle both IPv4 and IPv6 addresses
        ip_obj = ipaddress.ip_address(ip)

        # If it's an IPv4 address, return the IPv6-mapped string format
        if ip_obj.version == 4:
            return f"::ffff:{ip_obj}"
        # If it's already an IPv6 address, return it as a string
        return str(ip_obj)
    except ValueError:
        raise ValueError(f"Invalid IP address format: {ip}")


def format_ip_as_binary(ip: str) -> bytes:
    """
    Converts an IPv4 or IPv6 address to its binary format (16 bytes for IPv6 or IPv6-mapped IPv4).
    This binary representation is suitable for storing in MariaDB's BINARY(16) fields.

    Example:
        Input:  '192.168.1.1'
        Output: b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\xa8\x01\x01'
    """
    try:
        # Use ip_address to handle both IPv4 and IPv6 addresses
        ip_obj = ipaddress.ip_address(ip)

        # Return the binary packed version of the IP address (always 16 bytes)
        return ip_obj.packed
    except ValueError:
        raise ValueError(f"Invalid IP address format: {ip}")


def is_bogon_ip(ip):
    ip_addr = ip_address(ip)
    for network in settings.BOGON_NETWORKS:
        if ip_addr in network:
            logger.debug(f"IP {ip} is in bogon network {network}")
            return True
    return False
