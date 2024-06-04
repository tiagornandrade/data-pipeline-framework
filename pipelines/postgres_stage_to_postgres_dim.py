from core.operators.source import data_pipeline


db_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

config_file = '../config/config.json'
data_pipeline(config_file, db_params)
