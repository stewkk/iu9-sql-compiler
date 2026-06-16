from abc import ABC, abstractmethod


class PlanExtractor(ABC):
    def __init__(self, dataset: str):
        self.dataset = dataset
        self.load_dataset()

    @abstractmethod
    def load_dataset(self) -> None:
        pass

    @abstractmethod
    def extract(self, request) -> str:
        pass

