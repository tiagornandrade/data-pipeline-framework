from .stage import Stage


class TransformStage(Stage):
    def __init__(self, name):
        super().__init__(name)

    def execute(self, data):
        """
        Método para executar a lógica do estágio de transformação de dados.
        Este método recebe os dados de entrada, realiza a transformação e retorna os dados transformados.
        """
        print(f"Executando transformação no estágio {self.name}")
        transformed_data = []

        for item in data:
            transformed_item = (int(item[0]), int(item[1]))
            transformed_data.append(transformed_item)

        return transformed_data
