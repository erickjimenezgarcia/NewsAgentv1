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
              <div class="bg-blue-500 h-4 rounded-full transition-all duration-300" :style="{ width: '100%' }"></div>
            </div>
            <p class="text-blue-500 mt-2">Cargando archivo...</p>
          </div>

          <div v-if="fileName" class="mt-4 text-gray-700">
            Archivo seleccionado: <strong>{{ fileName }}</strong>
          </div>

          <button
            v-if="fileSelected && !processing && !processed && !loading"
            class="mt-4 bg-green-500 hover:bg-green-600 text-white font-medium py-2 px-6 rounded-md transition duration-300"
            @click="startProcessing"
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

      <div v-if="processed" class="bg-white rounded-lg shadow-sm p-6">
        <div class="text-center">
          <p class="text-gray-600 font-medium mb-4">¡Documento procesado!</p>
          <span class="text-blue-700 font-semibold">procesado-{{ fileName }}</span><br>
          <a
            :href="downloadUrl"
            :download="`procesado-${fileName}`"
            class="inline-flex items-center justify-center bg-blue-500 hover:bg-blue-600 text-white font-medium py-3 px-6 rounded-md transition duration-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
          >
            <span class="mr-2 text-blue-200">
              <img
                src="../assets/descargar.png"
                alt="download-icon"
                class="w-8 h-8"
              />
            </span>
            <span class="text-center">Descargar</span>
          </a>
        </div>
    
        <!-- Procesar un nuevo documento -->
        <div class="mt-6 border-t-1 border-gray-200 pt-2">
            <button
            class="bg-white hover:bg-gray-200 text-gray-800 font-medium py-2 px-4 rounded transition duration-200 "
            @click="resetProcess"
            >
                Procesar otro PDF
            </button>
        </div>
        <!-- ----------- -->
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const fileInput = ref(null)
const fileName = ref('')
const fileSelected = ref(false)
const loading = ref(false)
const processing = ref(false)
const processed = ref(false)
const progress = ref(0)
const downloadUrl = ref('')

function selectFile() {
  if (!fileSelected.value) fileInput.value.click()
}

function handleFileChange(event) {
  const file = event.target.files[0]
  if (file && file.type === 'application/pdf') {
    loading.value = true
    setTimeout(() => {
      fileName.value = file.name
      fileSelected.value = true
      loading.value = false
    }, 1200)
  }
}

function handleDrop(event) {
  const file = event.dataTransfer.files[0]
  if (file && file.type === 'application/pdf') {
    loading.value = true
    setTimeout(() => {
      fileName.value = file.name
      fileSelected.value = true
      loading.value = false
    }, 1200)
  }
}

function startProcessing() {
  processing.value = true
  progress.value = 0
  processed.value = false
  const interval = setInterval(() => {
    if (progress.value < 100) {
      progress.value += 10
    } else {
      clearInterval(interval)
      processing.value = false
      processed.value = true
      // Simulación: crea un blob vacío como ejemplo
      const blob = new Blob(['Procesado: ' + fileName.value], { type: 'application/pdf' })
      downloadUrl.value = URL.createObjectURL(blob)
    }
  }, 200)
}

function resetProcess() {
  fileName.value = ''
  fileSelected.value = false
  loading.value = false
  processing.value = false
  processed.value = false
  progress.value = 0
  downloadUrl.value = ''
}
</script>
