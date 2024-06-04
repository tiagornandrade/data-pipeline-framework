import json


def load_config(config_path):
    with open(config_path) as f:
        config = json.load(f)
    return config
