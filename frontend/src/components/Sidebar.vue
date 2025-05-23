// --------------script code
<script setup></script>

// --------------html code
<template>
  <aside
    id="logo-sidebar"
    class="fixed top-5 left-0 z-40 w-64 h-screen pt-20 transition-transform -translate-x-full bg-white border-r border-gray-200 sm:translate-x-0 dark:bg-gray-800 dark:border-gray-700"
    aria-label="Sidebar"
  >
    <div class="h-full px-3 pb-4 overflow-y-auto bg-gray-800">
      <ul class="space-y-2 text-xl">
        <li>
          <router-link
            to="/procesar"
            class="flex items-center p-3  rounded-lg text-white hover:bg-gray-100 dark:hover:bg-blue-700 group"
          >
            <img src="../assets/procesar.png" class="shrink-0 w-9 h-9" alt="icon-procesar">
            <span class="ms-3">Extracción</span>
          </router-link>
        </li>

        <li>
          <router-link
            to="/dashboard"
            class="flex items-center p-3  rounded-lg text-white hover:bg-blue-700 group"
          >
            <img src="../assets/dashboard.png" class="shrink-0 w-10 h-10" alt="icon-dashboard">
            <span class="ms-3">Dashboard</span>
          </router-link>
        </li>

        <li>
          <router-link
            to="/chat"
            class="flex items-center p-3 rounded-lg text-white hover:bg-blue-700 group"
          >
            <img src="../assets/chat.png" class="shrink-0 w-9 h-9" alt="icon-chat">
            <span class="ms-3">ChatBot</span>
          </router-link>
        </li>
        <li>
          <router-link
            to="/colecciones"
            class="flex items-center p-2 rounded-lg text-white hover:bg-blue-700 group"
          >
            <img src="../assets/coleccion.png" class="shrink-0 w-9 h-9" alt="icon-coleccion">
            <span class="ms-3">Colecciones</span>
          </router-link>
        </li>
      </ul>
    </div>
  </aside>
</template>
