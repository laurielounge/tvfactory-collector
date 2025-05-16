from abc import ABC, abstractmethod
from typing import TypeVar, Type

T = TypeVar("T", bound="BaseAsyncFactory")


class BaseAsyncFactory(ABC):
    """
    Abstract base class for any service requiring async instantiation.

    Subclasses must implement the `async_setup()` method,
    which is invoked after construction.
    """

    @classmethod
    async def create(cls: Type[T]) -> T:
        instance = cls()
        await instance.async_setup()
        return instance

    @abstractmethod
    async def async_setup(self):
        """
        Async initialisation hook. Override in subclasses.
        """
        ...
