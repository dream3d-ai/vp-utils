import os

import yaml
from loguru import logger


def get_env_var(key: str, env_file: str | None = None) -> str:
    env = dict(os.environ)
    if env_file is not None:
        try:
            with open(env_file, "r") as file:
                env.update(yaml.safe_load(file))
        except FileNotFoundError:
            logger.warning(f"Runtime environment file not found: {env_file}.")
            pass

    if var := env.get(key):
        return var

    raise Exception(f"Please provide {key} in environment variables")
