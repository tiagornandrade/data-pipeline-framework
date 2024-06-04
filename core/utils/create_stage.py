from core.operators.sink import PostgresSink


def create_stage(stage_config):
    stage_type = stage_config.get('type')
    name = stage_config['name']
    connection_params = stage_config.get('connection_params', {})

    if stage_type == 'postgres':
        schema = stage_config['schema']
        table = stage_config['table']
        columns = stage_config['columns']
        return PostgresSink(name, connection_params, schema, table, columns)
    else:
        raise ValueError(f"Tipo de estágio desconhecido: {stage_type}")


