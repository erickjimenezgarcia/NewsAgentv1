<template>
  <div class="pt-10 sm:ml-64">
    <!-- Header Section -->
    <div
      class="bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-xl shadow-lg p-6 mb-8"
    >
      <div class="flex items-center justify-between flex-wrap">
        <div class="flex items-center space-x-4">
          <div class="bg-white/20 p-3 rounded-lg">
            <svg class="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
              <path
                d="M12 2L2 7v10c0 5.55 3.84 9.93 9 11 5.16-1.07 9-5.45 9-11V7l-10-5z"
              />
            </svg>
          </div>
          <div>
            <h1 class="text-3xl font-bold">SUNASS Dashboard</h1>
            <p class="text-blue-100 text-sm">
              Sistema de Monitoreo de Noticias
            </p>
          </div>
        </div>
        <div class="text-right mt-4 lg:mt-0">
          <p class="text-sm opacity-90">Viernes, 23 de mayo de 2025</p>
          <p class="text-xs opacity-75">
            Última actualización: 22/05/2025 19:33
          </p>
        </div>
      </div>
    </div>

    <!-- Stats Overview -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
      <!-- Stat Card 1 -->
      <div
        class="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-all duration-300 hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div>
            <p class="text-gray-600 text-sm font-medium">URLs Totales</p>
            <p class="text-3xl font-bold text-gray-800 mt-2">15</p>
            <p class="text-sm mt-2 text-blue-600">
              <span
                class="inline-block w-2 h-2 bg-green-500 rounded-full mr-2"
              ></span>
              URLs en PDF
            </p>
          </div>
          <div
            class="bg-gradient-to-br from-blue-500 to-purple-600 p-3 rounded-lg text-white"
          >
            <svg class="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
              <path
                d="M3.9 12c0-1.71 1.39-3.1 3.1-3.1h4V7H6.99c-2.76 0-5 2.24-5 5s2.24 5 5 5h4v-1.9H7c-1.71 0-3.1-1.39-3.1-3.1zM8 13h8v-2H8v2zm9-6h-4v1.9h4c1.71 0 3.1 1.39 3.1 3.1s-1.39 3.1-3.1 3.1h-4V17h4c2.76 0 5-2.24 5-5s-2.24-5-5-5z"
              />
            </svg>
          </div>
        </div>
      </div>

      <!-- Stat Card 2 -->
      <div
        class="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-all duration-300 hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div>
            <p class="text-gray-600 text-sm font-medium">HTML Procesado</p>
            <p class="text-3xl font-bold text-gray-800 mt-2">6/7</p>
            <p class="text-sm mt-2 text-green-600">
              <span
                class="inline-block w-2 h-2 bg-green-500 rounded-full mr-2"
              ></span>
              86% éxito
            </p>
          </div>
          <div
            class="bg-gradient-to-br from-green-500 to-emerald-600 p-3 rounded-lg text-white"
          >
            <svg class="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
              <path
                d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"
              />
            </svg>
          </div>
        </div>
      </div>

      <!-- Stat Card 3 -->
      <div
        class="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-all duration-300 hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div>
            <p class="text-gray-600 text-sm font-medium">Imágenes</p>
            <p class="text-3xl font-bold text-gray-800 mt-2">5/7</p>
            <p class="text-sm mt-2 text-yellow-600">
              <span
                class="inline-block w-2 h-2 bg-yellow-500 rounded-full mr-2"
              ></span>
              71% descargadas
            </p>
          </div>
          <div
            class="bg-gradient-to-br from-yellow-500 to-orange-600 p-3 rounded-lg text-white"
          >
            <svg class="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
              <path
                d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"
              />
            </svg>
          </div>
        </div>
      </div>

      <!-- Stat Card 4 -->
      <div
        class="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-all duration-300 hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div>
            <p class="text-gray-600 text-sm font-medium">Relevancia</p>
            <p class="text-3xl font-bold text-gray-800 mt-2">43%</p>
            <p class="text-sm mt-2 text-purple-600">
              <span
                class="inline-block w-2 h-2 bg-purple-500 rounded-full mr-2"
              ></span>
              Promedio
            </p>
          </div>
          <div
            class="bg-gradient-to-br from-purple-500 to-pink-600 p-3 rounded-lg text-white"
          >
            <svg class="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
              <path
                d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"
              />
            </svg>
          </div>
        </div>
      </div>
    </div>

    <!-- Processing Overview -->
    <div class="bg-white rounded-xl shadow-md p-6 mb-8">
      <h2 class="text-xl font-bold text-gray-800 mb-6 flex items-center">
        <svg
          class="w-6 h-6 mr-2 text-blue-600"
          fill="currentColor"
          viewBox="0 0 24 24"
        >
          <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z" />
        </svg>
        Resumen de Procesamiento
      </h2>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
        <!-- Progress Circle 1 - HTML -->
        <div class="text-center">
          <div class="relative w-24 h-24 mx-auto mb-4">
            <svg class="w-24 h-24 transform -rotate-90" viewBox="0 0 100 100">
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#e5e7eb"
                stroke-width="8"
                fill="none"
              />
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#3b82f6"
                stroke-width="8"
                fill="none"
                stroke-linecap="round"
                stroke-dasharray="216 251.2"
              />
            </svg>
            <div class="absolute inset-0 flex items-center justify-center">
              <span class="text-lg font-bold text-gray-800">86%</span>
            </div>
          </div>
          <h3 class="font-semibold text-gray-800">HTML</h3>
          <p class="text-sm text-gray-600">6/7</p>
        </div>

        <!-- Progress Circle 2 - Imágenes -->
        <div class="text-center">
          <div class="relative w-24 h-24 mx-auto mb-4">
            <svg class="w-24 h-24 transform -rotate-90" viewBox="0 0 100 100">
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#e5e7eb"
                stroke-width="8"
                fill="none"
              />
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#10b981"
                stroke-width="8"
                fill="none"
                stroke-linecap="round"
                stroke-dasharray="178 251.2"
              />
            </svg>
            <div class="absolute inset-0 flex items-center justify-center">
              <span class="text-lg font-bold text-gray-800">71%</span>
            </div>
          </div>
          <h3 class="font-semibold text-gray-800">Imágenes</h3>
          <p class="text-sm text-gray-600">5/7</p>
        </div>

        <!-- Progress Circle 3 - Facebook -->
        <div class="text-center">
          <div class="relative w-24 h-24 mx-auto mb-4">
            <svg class="w-24 h-24 transform -rotate-90" viewBox="0 0 100 100">
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#e5e7eb"
                stroke-width="8"
                fill="none"
              />
              <circle
                cx="50"
                cy="50"
                r="40"
                stroke="#8b5cf6"
                stroke-width="8"
                fill="none"
                stroke-linecap="round"
                stroke-dasharray="251.2 251.2"
              />
            </svg>
            <div class="absolute inset-0 flex items-center justify-center">
              <span class="text-lg font-bold text-gray-800">100%</span>
            </div>
          </div>
          <h3 class="font-semibold text-gray-800">Facebook</h3>
          <p class="text-sm text-gray-600">1/1</p>
        </div>
      </div>
    </div>

    <!-- Content Sections Grid -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
      <!-- News Section -->
      <div class="bg-white rounded-xl shadow-md overflow-hidden">
        <div class="bg-gradient-to-r from-blue-500 to-cyan-500 p-4 text-white">
          <h2 class="text-xl font-bold flex items-center">
            <svg class="w-6 h-6 mr-2" fill="currentColor" viewBox="0 0 24 24">
              <path d="M4 6h16v2H4zm0 5h16v2H4zm0 5h16v2H4z" />
            </svg>
            Noticias SUNASS (2)
          </h2>
        </div>
        <div class="p-6 max-h-96 overflow-y-auto">
          <!-- News Item 1 -->
          <div
            class="border-b border-gray-200 pb-4 mb-4 hover:bg-gray-50 p-2 rounded transition-colors"
          >
            <h3 class="font-semibold text-gray-800 mb-2">
              Junta Administradora El Castillo reconocida por cloración
            </h3>
            <p class="text-gray-600 text-sm mb-2 line-clamp-2">
              La JAAP El Castillo fue reconocida por SUNASS como organización
              modelo por su adecuado proceso de cloración del agua para consumo
              humano, beneficiando a 470 habitantes.
            </p>
            <div
              class="flex items-center justify-between text-xs text-gray-500"
            >
              <span class="bg-blue-100 text-blue-800 px-2 py-1 rounded-full"
                >SUNASS</span
              >
              <span>Página 1</span>
            </div>
          </div>

          <!-- News Item 2 -->
          <div
            class="border-b border-gray-200 pb-4 mb-4 last:border-b-0 last:mb-0 hover:bg-gray-50 p-2 rounded transition-colors"
          >
            <h3 class="font-semibold text-gray-800 mb-2">
              Especialización de alumnos en saneamiento
            </h3>
            <p class="text-gray-600 text-sm mb-2 line-clamp-2">
              SUNASS contribuye a la formación especializada de 60 estudiantes a
              través del Programa de Extensión Universitaria (PEU).
            </p>
            <div
              class="flex items-center justify-between text-xs text-gray-500"
            >
              <span class="bg-blue-100 text-blue-800 px-2 py-1 rounded-full"
                >SUNASS</span
              >
              <span>Página 1</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Alerts Section -->
      <div class="bg-white rounded-xl shadow-md overflow-hidden">
        <div class="bg-gradient-to-r from-orange-500 to-red-500 p-4 text-white">
          <h2 class="text-xl font-bold flex items-center">
            <svg class="w-6 h-6 mr-2" fill="currentColor" viewBox="0 0 24 24">
              <path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z" />
            </svg>
            Alertas (2)
          </h2>
        </div>
        <div class="p-6 max-h-96 overflow-y-auto">
          <!-- Alert Item 1 -->
          <div
            class="border-l-4 border-orange-400 bg-orange-50 p-4 mb-4 rounded-r"
          >
            <h3 class="font-semibold text-gray-800 mb-2">
              Vigencia del Consejo Directivo de Sedachimbote
            </h3>
            <p class="text-gray-600 text-sm mb-2">
              La vigencia del consejo directivo ya se cumplió según D.L. 1280,
              debe renovarse totalmente por cumplir 3 años.
            </p>
            <div class="flex items-center justify-between text-xs">
              <span class="bg-orange-100 text-orange-800 px-2 py-1 rounded-full"
                >Alta Prioridad</span
              >
              <span class="text-gray-500">Página 1</span>
            </div>
          </div>

          <!-- Alert Item 2 -->
          <div
            class="border-l-4 border-red-400 bg-red-50 p-4 mb-4 last:mb-0 rounded-r"
          >
            <h3 class="font-semibold text-gray-800 mb-2">
              Fallas en obra de agua en Tacna
            </h3>
            <p class="text-gray-600 text-sm mb-2">
              Detectadas deficiencias en tuberías y buzones que comprometen la
              durabilidad del proyecto en Promuvi Señor de los Milagros.
            </p>
            <div class="flex items-center justify-between text-xs">
              <span class="bg-red-100 text-red-800 px-2 py-1 rounded-full"
                >Crítica</span
              >
              <span class="text-gray-500">Página 2</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Sector Information -->
    <div class="bg-white rounded-xl shadow-md overflow-hidden mb-8">
      <div class="bg-gradient-to-r from-green-500 to-teal-500 p-4 text-white">
        <h2 class="text-xl font-bold flex items-center">
          <svg class="w-6 h-6 mr-2" fill="currentColor" viewBox="0 0 24 24">
            <path
              d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"
            />
          </svg>
          Información del Sector (2)
        </h2>
      </div>
      <div class="p-6">
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <!-- Sector Item 1 -->
          <div
            class="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
          >
            <h3 class="font-semibold text-gray-800 mb-3">
              Convenio para sistema de captación Sedaloreto
            </h3>
            <p class="text-gray-600 text-sm mb-3 line-clamp-3">
              GOREL gestiona convenio con SENAMHI y Marina para fortalecer el
              sistema de captación de agua potable en Loreto.
            </p>
            <div class="flex items-center justify-between text-xs">
              <span class="bg-green-100 text-green-800 px-2 py-1 rounded-full"
                >Convenio</span
              >
              <span class="text-gray-500">Página 2</span>
            </div>
          </div>

          <!-- Sector Item 2 -->
          <div
            class="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
          >
            <h3 class="font-semibold text-gray-800 mb-3">
              Foro Gerencial OTASS
            </h3>
            <p class="text-gray-600 text-sm mb-3 line-clamp-3">
              Fortalecimiento de capacidades de gestión para reflotamiento de
              EPS bajo Régimen de Apoyo Transitorio (RAT).
            </p>
            <div class="flex items-center justify-between text-xs">
              <span class="bg-green-100 text-green-800 px-2 py-1 rounded-full"
                >Capacitación</span
              >
              <span class="text-gray-500">Página 2</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Processing Times -->
    <div class="bg-white rounded-xl shadow-md p-6 mb-8">
      <h2 class="text-xl font-bold text-gray-800 mb-6 flex items-center">
        <svg
          class="w-6 h-6 mr-2 text-purple-600"
          fill="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            d="M12,20A8,8 0 0,0 20,12A8,8 0 0,0 12,4A8,8 0 0,0 4,12A8,8 0 0,0 12,20M12,2A10,10 0 0,1 22,12A10,10 0 0,1 12,22C6.47,22 2,17.5 2,12A10,10 0 0,1 12,2M12.5,7V12.25L17,14.92L16.25,16.15L11,13V7H12.5Z"
          />
        </svg>
        Tiempos de Procesamiento
      </h2>
      <div class="space-y-4">
        <!-- Processing Time 1 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">
            Extracción PDF
          </div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-1"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">0.38s</div>
        </div>

        <!-- Processing Time 2 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">Texto PDF</div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-1"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">0.49s</div>
        </div>

        <!-- Processing Time 3 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">
            Descarga Imágenes
          </div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-1/5"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">25.45s</div>
        </div>

        <!-- Processing Time 4 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">Web Scraping</div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-1/12"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">4.65s</div>
        </div>

        <!-- Processing Time 5 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">API Imágenes</div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-full"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">142.67s</div>
        </div>

        <!-- Processing Time 6 -->
        <div class="flex items-center">
          <div class="w-40 text-sm font-medium text-gray-700">Facebook</div>
          <div class="flex-1 mx-4">
            <div class="bg-gray-200 rounded-full h-2 relative overflow-hidden">
              <div
                class="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full w-1/6"
              ></div>
            </div>
          </div>
          <div class="w-16 text-sm text-gray-600 text-right">23.85s</div>
        </div>
      </div>
    </div>

    <!-- URL Processing Details -->
    <div class="bg-white rounded-xl shadow-md p-6">
      <h2 class="text-xl font-bold text-gray-800 mb-6 flex items-center">
        <svg
          class="w-6 h-6 mr-2 text-indigo-600"
          fill="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            d="M16.36,14C16.44,13.34 16.5,12.68 16.5,12C16.5,11.32 16.44,10.66 16.36,10H19.74C19.9,10.64 20,11.31 20,12C20,12.69 19.9,13.36 19.74,14M14.59,19.56C15.19,18.45 15.65,17.25 15.97,16H18.92C17.96,17.65 16.43,18.93 14.59,19.56M14.34,14H9.66C9.56,13.34 9.5,12.68 9.5,12C9.5,11.32 9.56,10.65 9.66,10H14.34C14.43,10.65 14.5,11.32 14.5,12C14.5,12.68 14.43,13.34 14.34,14M12,19.96C11.17,18.76 10.5,17.43 10.09,16H13.91C13.5,17.43 12.83,18.76 12,19.96M8,8H5.08C6.03,6.34 7.57,5.06 9.4,4.44C8.8,5.55 8.35,6.75 8,8M5.08,16H8C8.35,17.25 8.8,18.45 9.4,19.56C7.57,18.93 6.03,17.65 5.08,16M4.26,14C4.1,13.36 4,12.69 4,12C4,11.31 4.1,10.64 4.26,10H7.64C7.56,10.66 7.5,11.32 7.5,12C7.5,12.68 7.56,13.34 7.64,14M12,4.03C12.83,5.23 13.5,6.57 13.91,8H10.09C10.5,6.57 11.17,5.23 12,4.03M18.92,8H15.97C15.65,6.75 15.19,5.55 14.59,4.44C16.43,5.07 17.96,6.34 18.92,8M12,2C6.47,2 2,6.5 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z"
          />
        </svg>
        URLs Procesadas (2)
      </h2>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <!-- URL Card 1 -->
        <div
          class="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-all hover:border-blue-300"
        >
          <div class="flex items-start justify-between mb-2">
            <h3 class="font-medium text-gray-800 text-sm line-clamp-2">
              Acuerdo sobre designación de Subgerente EMAPICA
            </h3>
            <span
              class="text-xs px-2 py-1 rounded-full bg-yellow-100 text-yellow-800"
              >20%</span
            >
          </div>
          <p class="text-gray-600 text-xs mb-3 line-clamp-2">
            Acuerdo adoptado sobre designación de Subgerente de Recursos Humanos
            de EPS EMAPICA S.A.
          </p>
          <a
            href="#"
            class="text-blue-600 hover:text-blue-800 text-xs break-all"
          >
            https://busquedas.elperuano.pe/dispositivo/NL/2398634-1
          </a>
        </div>

        <!-- URL Card 2 -->
        <div
          class="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-all hover:border-blue-300"
        >
          <div class="flex items-start justify-between mb-2">
            <h3 class="font-medium text-gray-800 text-sm line-clamp-2">
              Junta El Castillo reconocida por cloración del agua
            </h3>
            <span
              class="text-xs px-2 py-1 rounded-full bg-green-100 text-green-800"
              >90%</span
            >
          </div>
          <p class="text-gray-600 text-xs mb-3 line-clamp-2">
            La JAAP El Castillo fue reconocida por SUNASS como organización
            modelo por su adecuado proceso de cloración.
          </p>
          <a
            href="#"
            class="text-blue-600 hover:text-blue-800 text-xs break-all"
          >
            https://chimbotenlinea.com/distritos/12/05/2025/chimbote-junta...
          </a>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup></script>
