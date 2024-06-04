from abc import ABC, abstractmethod


class Stage(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def execute(self):
        """
        Método abstrato que define a execução do estágio.
        Cada estágio concreto deve implementar este método.
        """
        pass
