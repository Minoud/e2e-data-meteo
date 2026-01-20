from abc import ABC, abstractmethod

class BaseCollector(ABC):

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def save_raw(self, data):
        pass