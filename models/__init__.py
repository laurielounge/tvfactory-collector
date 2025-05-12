# models/__init__.py
from ipaddress import ip_address, IPv6Address

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, BINARY

Base = declarative_base()


class INET6(TypeDecorator):
    """Custom type to interact with the INET6 data type in MariaDB."""
    impl = BINARY(16)
    cache_ok = True  # Indicate that it's safe to use in cache key

    def process_bind_param(self, value, dialect):
        if isinstance(value, (IPv6Address, str)):
            return ip_address(value).packed  # Ensure binary format
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return ip_address(value)  # Convert binary back to IPv6Address object
        return value
