import os
from pathlib import Path

import dotenv


def load_local_env_config():
    local_env_conf_path = Path(__file__).parent.parent / 'env_config' / 'local.env'
    local_env_conf = {
        **dotenv.dotenv_values(local_env_conf_path),
        **os.environ
    }
    return local_env_conf
