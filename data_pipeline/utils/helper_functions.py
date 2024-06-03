import json
from data_pipeline.pipeline import Pipeline
from data_pipeline.stages import SourceStage, TransformStage, SinkStage


def load_config(config_file):
    """
    Função para carregar as configurações do arquivo de configuração.
    """
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"Arquivo de configuração '{config_file}' não encontrado.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o arquivo de configuração '{config_file}': {e}")
        return None


def validate_config(config):
    """
    Função para validar as configurações carregadas.
    """
    required_fields = ['source', 'transform', 'sink']
    for field in required_fields:
        if field not in config:
            print(f"Campo '{field}' ausente nas configurações.")
            return False

    source_config = config.get('source')
    transform_config = config.get('transform')
    sink_config = config.get('sink')

    if not validate_source_config(source_config):
        return False

    if not validate_transform_config(transform_config):
        return False

    if not validate_sink_config(sink_config):
        return False

    return True


def validate_source_config(config):
    """
    Função para validar as configurações do estágio de origem.
    """
    if not isinstance(config, dict):
        print("Configurações de origem inválidas. Esperava-se um dicionário.")
        return False

    required_fields = ['type', 'connection_params']
    for field in required_fields:
        if field not in config:
            print(f"Campo '{field}' ausente nas configurações de origem.")
            return False

    if config['type'] not in ['postgres', 'mssql', 'mysql']:
        print("Tipo de origem inválido. Tipos válidos são 'postgres', 'mssql' e 'mysql'.")
        return False

    if not isinstance(config['connection_params'], dict):
        print("Parâmetros de conexão inválidos para a origem. Esperava-se um dicionário.")
        return False

    return True


def validate_transform_config(config):
    """
    Função para validar as configurações do estágio de transformação.
    """
    if not isinstance(config, dict):
        print("Configurações de transformação inválidas. Esperava-se um dicionário.")
        return False

    return True


def validate_sink_config(config):
    """
    Função para validar as configurações do estágio de destino.
    """
    if not isinstance(config, dict):
        print("Configurações de destino inválidas. Esperava-se um dicionário.")
        return False

    required_fields = ['type', 'connection_params']
    for field in required_fields:
        if field not in config:
            print(f"Campo '{field}' ausente nas configurações de destino.")
            return False

    if config['type'] not in ['postgres', 'mssql', 'mysql']:
        print("Tipo de destino inválido. Tipos válidos são 'postgres', 'mssql' e 'mysql'.")
        return False

    if not isinstance(config['connection_params'], dict):
        print("Parâmetros de conexão inválidos para o destino. Esperava-se um dicionário.")
        return False

    return True


def create_pipeline_from_config(config):
    """
    Função para criar um pipeline com base nas configurações fornecidas.
    """
    try:
        stages = []

        source_config = config.get('source')
        source_stage = SourceStage(name='Source', **source_config)
        stages.append(source_stage)

        transform_config = config.get('transform')
        transform_stage = TransformStage(name='Transform', **transform_config)
        stages.append(transform_stage)

        sink_config = config.get('sink')
        sink_stage = SinkStage(name='Sink', **sink_config)
        stages.append(sink_stage)

        pipeline = Pipeline(*stages)
        return pipeline
    except Exception as e:
        print(f"Erro ao criar o pipeline: {e}")
        return None
