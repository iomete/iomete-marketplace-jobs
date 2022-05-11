from pyhocon import ConfigFactory


def get_config(application_config_path):
    return ConfigFactory.parse_file(application_config_path)
