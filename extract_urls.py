import csv
import os

# Ruta al archivo CSV
csv_path = os.path.join('input', 'In', 'links_extracted_16042025.csv')

# Extraer URLs
urls = []
try:
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'URL' in row and row['URL']:
                urls.append(row['URL'])
    
    # Mostrar estadísticas
    unique_urls = set(urls)
    print(f'Total URLs: {len(urls)}')
    print(f'URLs únicas: {len(unique_urls)}')
    
    # Guardar URLs únicas en un archivo
    with open('unique_urls.txt', 'w', encoding='utf-8') as f:
        for url in sorted(unique_urls):
            f.write(f"{url}\n")
    
    print("URLs únicas guardadas en unique_urls.txt")
    
except Exception as e:
    print(f"Error: {e}")
