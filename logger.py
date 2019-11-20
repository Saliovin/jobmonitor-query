import logging
import logging.config
import os
import yaml


def ini_logger(name):
    path = os.path.abspath(os.path.dirname(__file__))
    yaml_dir = f"{path}{os.sep}logging.yaml"
    with open(yaml_dir, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

    return logging.getLogger(name)