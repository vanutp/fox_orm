from abc import ABC, abstractmethod

try:
    from pydantic import BaseModel
except ImportError:

    class BaseModel(ABC):
        @abstractmethod
        def json(self, *args, **kwargs):
            ...


__all__ = ['BaseModel']
