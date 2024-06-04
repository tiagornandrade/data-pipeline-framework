class Pipeline:
    def __init__(self, *stages):
        self.stages = stages

    def run(self):
        try:
            for stage in self.stages:
                stage.execute()
        except Exception as e:
            print(f"Erro durante a execução do pipeline: {e}")
