import yaml
import os

def load_openai_api_key(yaml_path="../credentials/api_keys.yaml"):
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"No se encontró el archivo: {yaml_path}")
    
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    api_key = data.get("api_key_openia")
    if not api_key:
        raise KeyError("La clave 'api_key_openia' no fue encontrada en el YAML.")
    
    return api_key