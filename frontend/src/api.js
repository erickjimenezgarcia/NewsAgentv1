
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

/**
 * Uploads a PDF file to the backend.
 * @param {File} file - The PDF file to upload.
 * @returns {Promise<Response>} - The response from the server.
 */
export async function upload_pdf(file) {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(`${API_BASE_URL}/upload_pdf/`, {
    method: "POST",
    body: formData,
  });
  return await response.json();
}

/**
 * Sube un archivo PDF al backend con soporte de progreso.
 * @param {File} file - El archivo PDF a subir.
 * @param {function} onProgress - Callback para actualizar el progreso (recibe porcentaje 0-100).
 * @returns {Promise<Object>} - La respuesta del servidor.
 */
export function upload_pdf_with_progress(file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append("file", file);

    xhr.open("POST", `${API_BASE_URL}/upload_pdf/`, true);

    xhr.upload.onprogress = function (event) {
      if (event.lengthComputable && typeof onProgress === "function") {
        const percent = Math.round((event.loaded / event.total) * 100);
        onProgress(percent);
      }
    };

    xhr.onload = function () {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(JSON.parse(xhr.responseText));
      } else {
        reject(new Error("Error al subir el archivo PDF"));
      }
    };

    xhr.onerror = function () {
      reject(new Error("Error de red al subir el archivo PDF"));
    };

    xhr.send(formData);
  });
}

/**
 * Uploads a text file to the backend to process.
 * @param {File} file - The text file to upload.
 * @returns {Promise<Response>} - The response from the server.
 */
export async function procesar_pdf({ filename, prompt, batchSize, pauseSeconds }) {
  
  // Quita la extensión .pdf si existe
  const nombreSinExtension = filename.replace(/\.pdf$/i, "");

  const response = await fetch(`${API_BASE_URL}/procesar_pdf/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ 
      filename: nombreSinExtension,
      prompt, 
      batchSize, 
      pauseSeconds 
    }),
  });
  return await response.json();
}


/**
 * Descarga un archivo Markdown (.md) desde el backend.
 * @param {string} filename - El nombre base del archivo (sin extensión).
 * @returns {Promise<Blob>} - El archivo Markdown como Blob.
 */
export async function descargarMarkdown(filename) {
  const response = await fetch(`${API_BASE_URL}/download_md/${filename}`);
  if (!response.ok) throw new Error("No se pudo descargar el archivo Markdown");
  return await response.blob();
}

/**
 * Consulta el estado/progreso de la generación del archivo Markdown (.md).
 * @param {string} filename - El nombre base del archivo (sin extensión).
 * @returns {Promise<{progress: number, status: string}>}
 */
export async function obtenerEstadoMd(filename) {
  const response = await fetch(`${API_BASE_URL}/estado_md/${filename}`);
  if (!response.ok) throw new Error("No se pudo obtener el estado del archivo Markdown");
  return await response.json();
}

/**
 * Obtiene la lista de URLs extraídas desde el backend.
 * @param {string} filename - El nombre base del archivo (sin extensión).
 * @returns {Promise<string[]>} - Un array de URLs extraídas.
 */
export async function obtenerUrlsExtraidas(filename) {
  const response = await fetch(`${API_BASE_URL}/urls_extraidas/${filename}`);
  if (!response.ok) throw new Error("No se pudo obtener la lista de URLs");
  const data = await response.json();
  return data.urls || [];
}















export async function obtenerEstado() {
  const response = await fetch(`${API_BASE_URL}/estado/`);
  return await response.json();
}