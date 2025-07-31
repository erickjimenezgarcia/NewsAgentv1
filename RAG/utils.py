import yaml
import os

def load_openai_api_key():
    base_path = os.path.dirname(os.path.abspath(__file__))  # ubicación real de utils.py
    yaml_path = os.path.join(base_path, "..", "credentials", "api_keys.yaml")
    yaml_path = os.path.abspath(yaml_path)

    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"No se encontró el archivo: {yaml_path}")
    
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    api_key = data.get("api_key_openia")
    if not api_key:
        raise KeyError("La clave 'api_key_openia' no fue encontrada en el YAML.")
    
    return api_key



def load_llama3_api_key():
    base_path = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(base_path, "..", "credentials", "api_keys.yaml")
    yaml_path = os.path.abspath(yaml_path)

    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"No se encontró el archivo: {yaml_path}")
    
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    api_key = data.get("api_llama3")
    if not api_key:
        raise KeyError("La clave 'api_llama3' no fue encontrada en el YAML.")
    
    return api_key
