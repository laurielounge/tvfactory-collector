import os
from typing import Dict, Any

import yaml


def load_configuration(config_path: str) -> Dict[str, Dict[str, Any]]:
    with open(os.path.expanduser(config_path), 'r') as f:
        return yaml.safe_load(f)
