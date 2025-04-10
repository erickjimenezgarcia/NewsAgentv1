# codigo/lib/pdf_processor.py
import fitz  # PyMuPDF
import logging
import os

logger = logging.getLogger(__name__)

def get_text_around_link(page, link_dict):
    """
    Intenta extraer texto alrededor del rectángulo del enlace para dar contexto.
    """
    try:
        if 'from' in link_dict: # PyMuPDF >= 1.19 usa 'from' para el Rect
             rect = link_dict['from']
        elif 'rect' in link_dict: # Versiones anteriores
            rect = link_dict['rect']
        else:
            return "" # No hay información de ubicación

        # Amplía un poco el área para capturar contexto
        # Ajusta estos valores según necesites más o menos contexto
        h_margin = 30 # Margen horizontal
        v_margin = 10 # Margen vertical
        expanded_rect = fitz.Rect(rect.x0 - h_margin, rect.y0 - v_margin,
                                  rect.x1 + h_margin, rect.y1 + v_margin)

        # Asegurarse de que el rectángulo expandido no se salga de la página
        page_rect = page.rect
        expanded_rect.intersect(page_rect)

        if expanded_rect.is_empty or expanded_rect.width <= 0 or expanded_rect.height <= 0:
             return "" # Área inválida

        text = page.get_text("text", clip=expanded_rect, sort=True).strip()
        # Limpieza simple: reemplazar saltos de línea múltiples por espacio
        text = ' '.join(text.split())
        return text

    except Exception as e:
        logger.warning(f"Error extrayendo contexto para enlace {link_dict.get('uri', 'N/A')}: {e}")
        return ""

def extract_links_from_pdf(pdf_path):
    """
    Abre un archivo PDF y extrae todos los enlaces URI (http, https, ftp),
    junto con su página, ubicación (rectángulo) y texto de contexto.
    Excluye explícitamente los enlaces 'mailto:'.
    """
    links = []
    if not os.path.exists(pdf_path):
        logger.error(f"Archivo PDF no encontrado: {pdf_path}")
        return links
    if not pdf_path.lower().endswith(".pdf"):
         logger.error(f"El archivo no parece ser un PDF: {pdf_path}")
         return links

    try:
        doc = fitz.open(pdf_path)
        logger.info(f"Abriendo PDF: {pdf_path} ({doc.page_count} páginas)")
    except Exception as e:
        # Captura errores específicos de fitz si es posible, o genéricos
        logger.error(f"Error al abrir o procesar el archivo PDF '{pdf_path}': {e}")
        return links # Retorna lista vacía si no se puede abrir

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        page_links = page.get_links() # Obtiene todos los tipos de enlaces

        for link_dict in page_links:
            # Verificar que sea un enlace URI y no un mailto
            if link_dict.get('kind') == fitz.LINK_URI:
                uri = link_dict.get('uri')
                if uri and not uri.lower().startswith('mailto:'):
                     context = get_text_around_link(page, link_dict)
                     rect = link_dict.get('from') # PyMuPDF >= 1.19
                     if rect:
                         rect_tuple = (rect.x0, rect.y0, rect.x1, rect.y1)
                     else: # Compatibilidad con versiones anteriores
                         rect_compat = link_dict.get('rect')
                         rect_tuple = tuple(rect_compat) if rect_compat else None

                     links.append({
                        "Page": page_num + 1,
                        "URL": uri,
                        "Rect": rect_tuple, # Guardar como tupla simple
                        "Context": context
                    })
            # Podrías añadir lógica aquí para otros tipos de enlaces si fuera necesario
            # elif link_dict.get('kind') == fitz.LINK_GOTO:
            #     # Enlace interno
            #     pass

    try:
        doc.close()
    except Exception as e:
         logger.warning(f"Error menor al cerrar el PDF '{pdf_path}': {e}")


    logger.info(f"Se extrajeron {len(links)} enlaces URI (no mailto) de {os.path.basename(pdf_path)}.")
    return links