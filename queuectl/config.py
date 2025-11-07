# queuectl/config.py
from .db import set_config, get_config

def set_kv(key: str, value: str):
    set_config(key, value)

def get_kv(key: str, default: str=None) -> str:
    return get_config(key, default)
