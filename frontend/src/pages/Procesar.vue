<template>
  <div class="container-process pt-4 sm:ml-64">
    <div class="container mx-auto px-4 py-8">
      <h1 class="text-xl md:text-2xl font-semibold text-gray-600 mb-6 pb-4">
        PROCESAMIENTO DE DOCUMENTOS
      </h1>

      <div v-if="!processed" class="bg-white rounded-lg shadow-sm p-6 mb-8">
        <h2 class="text-lg font-medium text-gray-600 mb-4">
          AÑADIR NUEVOS DOCUMENTOS
        </h2>

        <div
          class="border-2 border-dashed border-gray-300 rounded-lg p-6 flex flex-col items-center justify-center text-center bg-gray-50"
          @drop.prevent="handleDrop"
          @dragover.prevent
        >
          <p class="text-gray-500 mb-4">Arrastre archivos aquí o ...</p>

          <input
            ref="fileInput"
            type="file"
            class="hidden"
            @change="handleFileChange"
            accept=".pdf"
            :disabled="fileSelected"
          />

          <button
            class="bg-blue-500 hover:bg-blue-600 text-white font-medium py-2 px-6 rounded-md transition duration-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
            @click="selectFile"
            :disabled="fileSelected || processing"
          >
            Seleccionar archivos
          </button>

          <div v-if="loading" class="w-full mt-6">
            <div class="w-full bg-gray-200 rounded-full h-4">
              <div
                class="bg-blue-500 h-4 rounded-full transition-all duration-300"
                :style="{ width: '100%' }"
              ></div>
            </div>
            <p class="text-blue-500 mt-2">Cargando archivo...</p>
          </div>

          <div v-if="fileName" class="mt-4 text-gray-700">
            Archivo seleccionado: <strong>{{ fileName }}</strong>
          </div>

          <button
            v-if="fileSelected && !processing && !processed && !loading"
            class="mt-4 bg-green-500 hover:bg-green-600 text-white font-medium py-2 px-6 rounded-md transition duration-300"
            @click="iniciarProcesamiento"
          >
            Procesar
          </button>

          <div v-if="processing" class="w-full mt-6">
            <div class="w-full bg-gray-200 rounded-full h-4">
              <div
                class="bg-green-500 h-4 rounded-full transition-all duration-300"
                :style="{ width: progress + '%' }"
              ></div>
            </div>
            <p class="text-green-500 mt-2">Procesando... {{ progress }}%</p>
          </div>
        </div>
        <p class="text-gray-500 mt-4 text-sm md:text-base text-center">
          Formatos soportados : PDF...
        </p>
      </div>

      <!-- configuration of selecters of parameters -->
      <!-- <div
        class="mt-4 flex flex-col md:flex-row gap-4 items-center bg-gray-200"
      >
        <label>
          Prompt:
          <select v-model="prompt" class="ml-2 border rounded px-2 py-1">
            <option value="simple">Simple</option>
            <option value="detallado">Detallado</option>
            <option value="estructurado">Estructurado</option>
            <option value="anti-ruido">Anti-ruido</option>
          </select>
        </label>
        <label>
          Batch size:
          <input
            type="number"
            v-model.number="batchSize"
            min="1"
            max="10"
            class="ml-2 border rounded px-2 py-1 w-16"
          />
        </label>
        <label>
          Pausa (seg):
          <input
            type="number"
            v-model.number="pauseSeconds"
            min="1"
            max="300"
            class="ml-2 border rounded px-2 py-1 w-20"
          />
        </label>
      </div> -->
      <!-- ----------- -->

      <div v-if="processed" class="bg-white rounded-lg shadow-sm p-6">
        <div class="text-center">
          <p class="text-gray-600 font-medium mb-4">¡Documento procesado!</p>
          <span class="text-blue-700 font-semibold"
            >procesado-{{ fileName }}</span
          ><br />
          <button
            @click="handleDescargarMarkdown"
            class="inline-flex items-center justify-center bg-green-500 hover:bg-green-600 text-white font-medium py-3 px-6 rounded-md transition duration-300 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50 mt-4"
            style="margin-left: 10px"
          >
            <span class="mr-2 text-blue-200">
              <img
                src="../assets/descargar.png"
                alt="download-icon"
                class="w-8 h-8"
              />
            </span>
            <span class="text-center">Descargar</span>
          </button>
        </div>

        <!-- Procesar un nuevo documento -->
        <div class="mt-6 border-t-1 border-gray-200 pt-2">
          <button
            class="bg-white hover:bg-gray-200 text-gray-800 font-medium py-2 px-4 rounded transition duration-200"
            @click="resetProcess"
          >
            Procesar otro PDF
          </button>
        </div>
        <!-- ----------- -->
      </div>

      <!-- show urls extracted -->
      <div v-if="processed" class="bg-white rounded-lg shadow-sm p-6 mt-6">
        <div v-if="estado.urls && estado.urls.length">
          <h3 class="text-lg font-semibold text-gray-700 mb-3">
            URLs extraídas:
          </h3>
          <ul class="list-disc list-inside space-y-1">
            <li
              v-for="(url, idx) in estado.urls"
              :key="idx"
              class="text-blue-700 hover:underline break-all"
            >
              <a :href="url" target="_blank" rel="noopener noreferrer">{{
                url
              }}</a>
            </li>
          </ul>
        </div>
        <div v-else>
          <pre
            class="bg-gray-100 rounded p-4 text-sm text-gray-700 overflow-x-auto"
            >{{ estado }}</pre
          >
        </div>
      </div>
      <!-- show error message -->
      <div v-if="estado && estado.status === 'error'" class="mt-4 text-red-700">
        {{ estado.message }}
      </div>


      <!-- codigo de testeo -->
      <div v-if="estado && estado.message" class="mt-4 text-blue-700">
        {{ estado.message }} <br />
        <button
          @click="consultarEstado"
          class="ml-4 bg-blue-500 hover:bg-blue-600 text-white font-medium py-2 px-4 rounded-md transition duration-300"
        >
          Consultar Estado
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import { upload_pdf, procesar_pdf, descargarMarkdown, obtenerUrlsExtraidas } from "../api"; // importing modules from API.js

//reactives variables
const prompt = ref("simple");
const batchSize = ref(3);
const pauseSeconds = ref(30);
const estado = ref("");
const fileInput = ref(null);
const fileName = ref("");
const fileSelected = ref(false);
const loading = ref(false);
const processing = ref(false);
const processed = ref(false);
const progress = ref(0);

/**
 * File upload event
 * @param {Event} event - The file input change event
 * @returns {Promise<void>}
 */
async function handleFileChange(event) {
  const file = event.target.files[0];
  if (file && file.type === "application/pdf") {
    loading.value = true;
    try {
      const res = await upload_pdf(file);
      fileName.value = res.filename;
      fileSelected.value = true;
    } catch {
      // Manejo de error
    } finally {
      loading.value = false;
    }
  }
}

/**
 * Function to process the PDF
 * @param {Object} params - Parameters for processing
 * @returns {Promise<void>}
 */
async function iniciarProcesamiento() {
  processing.value = true;
  try {
    const resultado = await procesar_pdf({
      filename: fileName.value,
      prompt: prompt.value,
      batchSize: batchSize.value,
      pauseSeconds: pauseSeconds.value,
    });
    estado.value = resultado;
    // Obtenr las URLs extraídas despues de procesar el PDF
    const urls = await obtenerUrlsExtraidas(fileName.value.replace(/\.pdf$/i, ""));
    estado.value.urls = urls;
    processed.value = true;
  } catch {
    estado.value = { status: "error", message: "Error al procesar el PDF" };
  }
  processing.value = false;
}

/**
 * Function to download the processed markdown file
 * @returns {Promise<void>}
 */
async function handleDescargarMarkdown() {
  const blob = await descargarMarkdown(fileName.value.replace(/\.pdf$/i, ""));
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `procesado-${fileName.value.replace(/\.pdf$/i, "")}.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/**
 * Function to get upload file using drag and drop
 * @param {DragEvent} event - The drag event
 * @returns {Promise<void>}
 */
async function handleDrop(event) {
  const file = event.dataTransfer.files[0];
  if (file && file.type === "application/pdf") {
    loading.value = true;
    try {
      const res = await upload_pdf(file);
      fileName.value = res.filename;
      fileSelected.value = true;
    } catch (err) {
      alert("Error al subir el archivo PDF. Intenta nuevamente.");
      fileName.value = "";
      fileSelected.value = false;
    } finally {
      loading.value = false;
    }
  } else {
    alert("Solo se permiten archivos PDF.");
  }
}





/**
 * Function to get the status of the process
 * @returns {Promise<void>}
 */
async function consultarEstado() {
  estado.value = await obtenerEstado();
}

/**
 * Function to select the file
 */
function selectFile() {
  if (!fileSelected.value) fileInput.value.click();
}

/**
 * Function to reset the process
 */
function resetProcess() {
  fileName.value = "";
  fileSelected.value = false;
  loading.value = false;
  processing.value = false;
  processed.value = false;
  progress.value = 0;
  downloadUrl.value = "";
}
</script>
