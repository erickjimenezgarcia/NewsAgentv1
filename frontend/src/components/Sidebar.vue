// --------------script code
<script setup></script>

// --------------html code
<template>
  <aside
    id="logo-sidebar"
    class="fixed top-5 left-0 z-40 w-64 h-screen pt-20 transition-transform -translate-x-full bg-white border-r border-gray-200 sm:translate-x-0 dark:bg-gray-800 dark:border-gray-700"
    aria-label="Sidebar"
  >
    <div class="h-full px-3 pb-4 overflow-y-auto bg-white dark:bg-gray-800">
      <ul class="space-y-2 text-xl">
        <li>
          <a
            href="#"
            class="flex items-center p-3 text-gray-900 rounded-lg dark:text-white hover:bg-gray-100 dark:hover:bg-blue-700 group"
          >
            <img src="../assets/dashboard.png" class="shrink-0 w-10 h-10" alt="icon-dashboard">
            <span class="ms-3">Dashboard</span>
          </a>
        </li>
        <li>
          <a
            href="#"
            class="flex items-center p-3 text-gray-900 rounded-lg dark:text-white hover:bg-gray-100 dark:hover:bg-blue-700 group"
          >
            <img src="../assets/procesar.png" class="shrink-0 w-9 h-9" alt="icon-procesar">
            <span class="ms-3">Procesar</span>
          </a>
        </li>
        <li>
          <a
            href="#"
            class="flex items-center p-3 text-gray-900 rounded-lg dark:text-white hover:bg-gray-100 dark:hover:bg-blue-700 group"
          >
            <img src="../assets/chat.png" class="shrink-0 w-9 h-9" alt="icon-chat">
            <span class="ms-3">Chat</span>
          </a>
        </li>
        <li>
          <a
            href="#"
            class="flex items-center p-2 text-gray-900 rounded-lg dark:text-white hover:bg-gray-100 dark:hover:bg-blue-700 group"
          >
            <img src="../assets/coleccion.png" class="shrink-0 w-9 h-9" alt="icon-coleccion">
            <span class="ms-3">Colecciones</span>
          </a>
        </li>
      </ul>
    </div>
  </aside>
</template>
