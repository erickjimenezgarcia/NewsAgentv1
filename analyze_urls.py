import os
import csv
from collections import defaultdict
from urllib.parse import urlparse

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
    
    # Categorizar URLs por dominio
    domain_urls = defaultdict(list)
    for url in urls:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            domain_urls[domain].append(url)
        except:
            # Si hay algún error al parsear, poner en "otros"
            domain_urls["otros"].append(url)
    
    # Imprimir estadísticas por dominio
    print(f"Total URLs: {len(urls)}")
    print(f"URLs únicas: {len(set(urls))}")
    print("\nDistribución por dominio:")
    
    # Ordenar dominios por cantidad de URLs (de mayor a menor)
    sorted_domains = sorted(domain_urls.items(), key=lambda x: len(x[1]), reverse=True)
    
    for domain, domain_urls_list in sorted_domains:
        print(f"{domain}: {len(domain_urls_list)} URLs")
    
    # Guardar clasificación en archivo
    with open('classified_urls.txt', 'w', encoding='utf-8') as f:
        for domain, domain_urls_list in sorted_domains:
            f.write(f"\n\n== {domain} ({len(domain_urls_list)} URLs) ==\n")
            for url in domain_urls_list:
                f.write(f"{url}\n")
    
    print("\nClasificación guardada en classified_urls.txt")
    
except Exception as e:
    print(f"Error: {e}")
