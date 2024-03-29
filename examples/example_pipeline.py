from data_pipeline.pipeline import Pipeline
from data_pipeline.stages import SourceStage, TransformStage, SinkStage
from data_pipeline.utils.helper_functions import load_config, validate_config, create_pipeline_from_config


def main():
    config = load_config('config.json')
    if not validate_config(config):
        print("Configurações inválidas. Abortando...")
        return

    pipeline = create_pipeline_from_config(config)

    pipeline.run()


if __name__ == "__main__":
    main()
