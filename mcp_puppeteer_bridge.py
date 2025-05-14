"""
Puente de implementación entre Python y Puppeteer para extracción de contenido.
Este módulo utiliza las herramientas MCP de Puppeteer para acceder a las URLs
y extraer su contenido real.
"""

import asyncio
import json
import logging
import time
from datetime import datetime

logger = logging.getLogger("mcp_puppeteer_bridge")

async def _navigate_to_url(url, timeout=30000, wait_time=3):
    """
    Navega a una URL utilizando Puppeteer.
    
    Args:
        url: URL a la que navegar
        timeout: Tiempo máximo de espera para navegación en milisegundos
        wait_time: Tiempo adicional de espera después de la carga en segundos
        
    Returns:
        bool: True si la navegación fue exitosa
    """
    try:
        from __main__ import mcp0_puppeteer_navigate
        
        # Configuración de opciones para navegación
        launch_options = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-notifications",
                "--disable-gpu",
                "--disable-dev-shm-usage"
            ]
        }
        
        # Navegar a la URL con Puppeteer
        result = await mcp0_puppeteer_navigate(
            url=url,
            launchOptions=launch_options,
            allowDangerous=False
        )
        
        # Esperar a que la página termine de cargar completamente
        await asyncio.sleep(wait_time)
        
        logger.info(f"Navegación exitosa a URL: {url}")
        return True
    
    except Exception as e:
        logger.error(f"Error durante navegación a {url}: {e}")
        return False

async def _execute_script(script):
    """
    Ejecuta un script JavaScript en la página actual.
    
    Args:
        script: Código JavaScript a ejecutar
        
    Returns:
        object: Resultado de la ejecución del script
    """
    try:
        from __main__ import mcp0_puppeteer_evaluate
        
        # Ejecutar script en la página
        result = await mcp0_puppeteer_evaluate(script=script)
        return result
    
    except Exception as e:
        logger.error(f"Error al ejecutar script: {e}")
        return None

async def _take_screenshot(name="screenshot", selector=None, width=1280, height=800):
    """
    Toma una captura de pantalla de la página actual o un elemento específico.
    
    Args:
        name: Nombre para la captura
        selector: Selector CSS del elemento (opcional)
        width, height: Dimensiones de la captura
        
    Returns:
        str: Nombre de la captura si fue exitosa
    """
    try:
        from __main__ import mcp0_puppeteer_screenshot
        
        # Tomar captura
        result = await mcp0_puppeteer_screenshot(
            name=name,
            selector=selector,
            width=width,
            height=height,
            encoded=False
        )
        
        logger.info(f"Captura de pantalla tomada: {name}")
        return name
    
    except Exception as e:
        logger.error(f"Error al tomar captura: {e}")
        return None

async def extract_facebook_content(url):
    """
    Extrae contenido real de una publicación de Facebook.
    
    Args:
        url: URL de la publicación de Facebook
        
    Returns:
        dict: Contenido extraído de la publicación
    """
    try:
        # Iniciar navegación a la URL
        success = await _navigate_to_url(url, wait_time=5)
        if not success:
            return {"error": "Navegación fallida a la URL de Facebook"}
        
        # Verificar si requiere inicio de sesión
        login_check_script = """
        () => {
            const loginElements = document.querySelectorAll('form[action*="login"], input[name="email"], input[placeholder*="correo"], button[name="login"]');
            const isLoginPage = loginElements.length > 0;
            
            return {
                isLoginPage: isLoginPage,
                pageType: isLoginPage ? 'login' : 'content'
            };
        }
        """
        
        login_check = await _execute_script(login_check_script)
        
        # Si es página de login, extraer información básica
        if login_check and login_check.get("isLoginPage", False):
            logger.warning(f"Facebook requiere inicio de sesión para URL: {url}")
            
            # Extraer datos básicos disponibles
            basic_data_script = """
            () => {
                const ogElements = document.querySelectorAll('meta[property^="og:"]');
                const data = {};
                
                ogElements.forEach(el => {
                    const property = el.getAttribute('property').replace('og:', '');
                    data[property] = el.getAttribute('content');
                });
                
                return {
                    title: data.title || document.title,
                    description: data.description || '',
                    url: data.url || window.location.href,
                    contentLimited: true
                };
            }
            """
            
            basic_data = await _execute_script(basic_data_script)
            await _take_screenshot(name="facebook_login_required")
            
            return {
                "title": basic_data.get("title", ""),
                "text": basic_data.get("description", ""),
                "content_limited": True,
                "login_required": True
            }
        
        # Si no requiere login, extraer contenido completo
        extract_content_script = """
        () => {
            // Función para limpiar texto
            const cleanText = (text) => {
                if (!text) return '';
                return text.replace(/\\s+/g, ' ').trim();
            };
            
            // Analizar tipo de contenido
            const isVideoPost = !!document.querySelector('video, [data-sigil*="inlineVideo"]');
            const hasImages = !!document.querySelector('[data-ft*="photo"]') || !!document.querySelector('a[href*="photo.php"]');
            
            // Extraer autor y fecha
            let author = '';
            const authorElement = document.querySelector('h3, [data-ft*="author"], strong.actor');
            if (authorElement) {
                author = cleanText(authorElement.textContent);
            }
            
            let date = '';
            const dateElement = document.querySelector('abbr');
            if (dateElement) {
                date = dateElement.textContent || dateElement.getAttribute('title') || '';
            }
            
            // Extraer texto principal
            let postText = '';
            const textSelectors = [
                'div[data-ft*="content_owner_id_new"]', 
                '.userContent', 
                '[data-ad-preview="message"]',
                '[data-testid="post_message"]'
            ];
            
            for (const selector of textSelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    postText = cleanText(element.textContent);
                    break;
                }
            }
            
            // Si no encontramos texto por selectores específicos, intentar con contenido general
            if (!postText) {
                const contentElement = document.querySelector('#contentArea, article');
                if (contentElement) {
                    postText = cleanText(contentElement.textContent);
                }
            }
            
            // Buscar URLs de imágenes
            const imageUrls = [];
            const imgElements = document.querySelectorAll('a[href*="photo.php"] img, [data-ft*="photo"] img');
            imgElements.forEach(img => {
                const src = img.getAttribute('src');
                if (src && !src.includes('data:image')) {
                    imageUrls.push(src);
                }
            });
            
            // Contar comentarios
            let commentsCount = 0;
            const commentsElement = document.querySelector('[data-testid="UFI2CommentsCount/root"]');
            if (commentsElement) {
                const commentsText = commentsElement.textContent || '';
                // Extraer número de comentarios del texto
                const commentsMatch = commentsText.match(/\\d+/);
                if (commentsMatch) {
                    commentsCount = parseInt(commentsMatch[0]);
                }
            }
            
            // Contar reacciones
            let reactionsCount = 0;
            const reactionsElements = document.querySelectorAll('[data-testid="UFI2TopReactions/tooltip"] span[aria-hidden="true"]');
            if (reactionsElements.length > 0) {
                const reactionsText = reactionsElements[0].textContent || '';
                const reactionsMatch = reactionsText.match(/\\d+/);
                if (reactionsMatch) {
                    reactionsCount = parseInt(reactionsMatch[0]);
                }
            }
            
            return {
                title: document.title || '',
                text: postText,
                author,
                date,
                has_images: hasImages,
                has_video: isVideoPost,
                images: imageUrls,
                comments_count: commentsCount,
                reactions_count: reactionsCount
            };
        }
        """
        
        content = await _execute_script(extract_content_script)
        
        # Tomar captura de pantalla de la publicación
        await _take_screenshot(name="facebook_post")
        
        return content or {"error": "No se pudo extraer el contenido"}
    
    except Exception as e:
        logger.error(f"Error extrayendo contenido de Facebook {url}: {e}")
        return {"error": str(e)}

async def extract_youtube_content(url):
    """
    Extrae contenido real de un video de YouTube.
    
    Args:
        url: URL del video de YouTube
        
    Returns:
        dict: Contenido extraído del video
    """
    try:
        # Iniciar navegación a la URL
        success = await _navigate_to_url(url, wait_time=4)
        if not success:
            return {"error": "Navegación fallida a la URL de YouTube"}
        
        # Extraer contenido del video
        extract_content_script = """
        () => {
            // Intentar varias veces (usando selectores tanto del YouTube móvil como desktop)
            const waitForElement = (selector, maxAttempts = 10, interval = 500) => {
                return new Promise((resolve) => {
                    let attempts = 0;
                    const check = () => {
                        const element = document.querySelector(selector);
                        if (element || attempts >= maxAttempts) {
                            resolve(element);
                        } else {
                            attempts++;
                            setTimeout(check, interval);
                        }
                    };
                    check();
                });
            };
            
            // Esperar un poco para asegurar que el contenido está cargado
            return new Promise(async (resolve) => {
                // Esperar los elementos principales
                await waitForElement('h1.title');
                
                // Extraer datos del video
                const title = document.querySelector('h1.title, [id="title"] h1, [id="container"] h1')?.textContent?.trim() || document.title;
                
                let description = '';
                const descriptionElement = document.querySelector('#description, #description-text, [id="description"] yt-formatted-string');
                if (descriptionElement) {
                    description = descriptionElement.textContent.trim();
                }
                
                let channel = '';
                const channelElement = document.querySelector('#owner-name a, #channel-name, .ytd-channel-name');
                if (channelElement) {
                    channel = channelElement.textContent.trim();
                }
                
                let publishDate = '';
                const dateElement = document.querySelector('#info-strings yt-formatted-string, #upload-info span.date');
                if (dateElement) {
                    publishDate = dateElement.textContent.trim();
                }
                
                let viewCount = '';
                const viewElement = document.querySelector('.view-count, #count .short-view-count');
                if (viewElement) {
                    viewCount = viewElement.textContent.trim();
                }
                
                resolve({
                    title,
                    description,
                    channel,
                    publish_date: publishDate,
                    view_count: viewCount
                });
            });
        }
        """
        
        content = await _execute_script(extract_content_script)
        
        # Tomar captura de pantalla
        await _take_screenshot(name="youtube_video")
        
        return content or {"error": "No se pudo extraer el contenido del video"}
    
    except Exception as e:
        logger.error(f"Error extrayendo contenido de YouTube {url}: {e}")
        return {"error": str(e)}

async def extract_news_content(url):
    """
    Extrae contenido real de un sitio de noticias.
    
    Args:
        url: URL del artículo de noticias
        
    Returns:
        dict: Contenido extraído del artículo
    """
    try:
        # Iniciar navegación a la URL
        success = await _navigate_to_url(url, wait_time=3)
        if not success:
            return {"error": "Navegación fallida a la URL de noticias"}
        
        # Extraer contenido del artículo
        extract_content_script = """
        () => {
            // Función para limpiar texto
            const cleanText = (text) => {
                if (!text) return '';
                return text.replace(/\\s+/g, ' ').trim();
            };
            
            // Detectar estructura de la página
            const isArticle = !!document.querySelector('article, .article, .post, .nota, .entry');
            
            // Extraer título
            let title = document.title;
            const titleSelectors = [
                'h1', 
                '.article-title', 
                '.post-title', 
                '.entry-title', 
                'article h1',
                '[property="og:title"]'
            ];
            
            for (const selector of titleSelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    title = cleanText(element.textContent);
                    break;
                }
            }
            
            // Extraer resumen
            let summary = '';
            const summarySelectors = [
                '.article-summary', 
                '.entry-summary', 
                '.post-excerpt', 
                '.bajada', 
                '.summary',
                '[property="og:description"]'
            ];
            
            for (const selector of summarySelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    summary = cleanText(element.textContent);
                    break;
                }
            }
            
            // Extraer cuerpo
            let body = '';
            const bodySelectors = [
                'article .content', 
                '.article-body', 
                '.post-content', 
                '.entry-content', 
                '.article-text'
            ];
            
            for (const selector of bodySelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    body = cleanText(element.textContent);
                    break;
                }
            }
            
            // Si no encontramos el cuerpo con selectores específicos
            if (!body && isArticle) {
                const articleElement = document.querySelector('article, .article, .post, .nota, .entry');
                if (articleElement) {
                    body = cleanText(articleElement.textContent);
                }
            }
            
            // Extraer autor
            let author = '';
            const authorSelectors = [
                '.author', 
                '.article-author', 
                '.byline', 
                '[rel="author"]'
            ];
            
            for (const selector of authorSelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    author = cleanText(element.textContent);
                    break;
                }
            }
            
            // Extraer fecha
            let publishDate = '';
            const dateSelectors = [
                '.date', 
                '.article-date', 
                '.post-date', 
                '[property="article:published_time"]',
                'time'
            ];
            
            for (const selector of dateSelectors) {
                const element = document.querySelector(selector);
                if (element) {
                    publishDate = element.textContent.trim() || element.getAttribute('datetime') || '';
                    break;
                }
            }
            
            // Verificar si tiene imágenes
            const hasImages = !!document.querySelector('article img, .article img, .post img');
            
            return {
                title,
                summary,
                body,
                author,
                publish_date: publishDate,
                has_images: hasImages
            };
        }
        """
        
        content = await _execute_script(extract_content_script)
        
        # Tomar captura de pantalla
        await _take_screenshot(name="news_article")
        
        return content or {"error": "No se pudo extraer el contenido de la noticia"}
    
    except Exception as e:
        logger.error(f"Error extrayendo contenido de noticias {url}: {e}")
        return {"error": str(e)}

async def extract_document_content(url):
    """
    Extrae contenido de un documento (PDF, etc.).
    
    Args:
        url: URL del documento
        
    Returns:
        dict: Contenido extraído del documento
    """
    try:
        # Para PDF y documentos, intentar navegar
        success = await _navigate_to_url(url, wait_time=5)
        if not success:
            return {"error": "Navegación fallida a la URL del documento"}
        
        # Para PDFs incrustados o visibles en el navegador
        extract_content_script = """
        () => {
            // Detectar si es un PDF incrustado
            const isPdf = document.querySelector('embed[type="application/pdf"], object[type="application/pdf"]');
            
            // Si es PDF incrustado, obtener título y metadatos
            if (isPdf) {
                return {
                    title: document.title,
                    text: "Documento PDF detectado - texto no extraíble directamente",
                    page_count: 0,
                    is_pdf: true
                };
            }
            
            // Si no es PDF incrustado, intentar extraer texto visible
            const bodyText = document.body.textContent.replace(/\\s+/g, ' ').trim();
            
            return {
                title: document.title,
                text: bodyText,
                page_count: 1,
                is_pdf: false
            };
        }
        """
        
        content = await _execute_script(extract_content_script)
        
        # Tomar captura de pantalla
        await _take_screenshot(name="document_content")
        
        return content or {"error": "No se pudo extraer el contenido del documento"}
    
    except Exception as e:
        logger.error(f"Error extrayendo contenido de documento {url}: {e}")
        return {"error": str(e)}

async def extract_generic_content(url):
    """
    Extrae contenido de una URL genérica.
    
    Args:
        url: URL genérica
        
    Returns:
        dict: Contenido extraído de la página
    """
    try:
        # Iniciar navegación a la URL
        success = await _navigate_to_url(url, wait_time=3)
        if not success:
            return {"error": "Navegación fallida a la URL"}
        
        # Extraer contenido general
        extract_content_script = """
        () => {
            // Extraer texto principal, excluyendo cosas como menús, pies de página, etc.
            const getMainContent = () => {
                // Buscar el elemento con más texto (probable contenido principal)
                const contentElements = [
                    'main',
                    'article',
                    '#content',
                    '.content',
                    '.main',
                    '.post',
                    '.page'
                ];
                
                let mainElement = null;
                let maxLength = 0;
                
                for (const selector of contentElements) {
                    const element = document.querySelector(selector);
                    if (element) {
                        const text = element.textContent.trim();
                        if (text.length > maxLength) {
                            maxLength = text.length;
                            mainElement = element;
                        }
                    }
                }
                
                // Si no encontramos contenido principal específico, usar todo el body
                if (!mainElement) {
                    mainElement = document.body;
                }
                
                return mainElement.textContent.replace(/\\s+/g, ' ').trim();
            };
            
            return {
                title: document.title,
                text: getMainContent()
            };
        }
        """
        
        content = await _execute_script(extract_content_script)
        
        # Tomar captura de pantalla
        await _take_screenshot(name="generic_content")
        
        return content or {"error": "No se pudo extraer el contenido de la página"}
    
    except Exception as e:
        logger.error(f"Error extrayendo contenido genérico {url}: {e}")
        return {"error": str(e)}
