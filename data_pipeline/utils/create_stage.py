from data_pipeline.utils.load_config import load_config
from data_pipeline.pipeline import Pipeline
from data_pipeline.stages.sink_stage import PostgresSinkStage
from data_pipeline.stages.source_stage import PostgresSourceStage
from data_pipeline.stages.transform_stage import TransformStage


def create_stage(stage_config):
    stage_type = stage_config.get('type')
    name = stage_config['name']
    connection_params = stage_config.get('connection_params', {})

    if stage_type == 'postgres':
        schema = stage_config['schema']
        table = stage_config['table']
        columns = stage_config['columns']
        return PostgresSinkStage(name, connection_params, schema, table, columns)
    else:
        raise ValueError(f"Tipo de estágio desconhecido: {stage_type}")


def create_pipeline():
    config = load_config('../config/config.json')
    pipeline_config = config['pipeline'][0]

    source_config = pipeline_config['source']
    transform_config = pipeline_config['transform']
    sink_config = pipeline_config['sink']

    source = PostgresSourceStage(source_config['name'], source_config['connection_params'], source_config['query_file'])
    transform = TransformStage(transform_config['name'])
    sink = create_stage(sink_config)

    pipeline = Pipeline(source, transform, sink)
    initial_data = source.execute()

    if initial_data:
        transformed_data = transform.execute(initial_data)
        sink.execute(transformed_data)

    return pipeline
