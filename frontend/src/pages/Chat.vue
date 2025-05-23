<template>
  <div class="p-4 sm:ml-64">
    <div class="bg-white p-6 rounded-lg shadow">
      <h2 class="text-xl font-semibold mb-4 text-gray-800">
        Asistente Virtual
      </h2>
      <div
        class="h-64 overflow-y-auto border rounded p-4 mb-4 bg-gray-50 space-y-2"
      >
        <!-- Mensajes simulados -->
        <div class="text-sm text-gray-600">
          <strong>Usuario:</strong> ¿Cuántas URLs hay?
        </div>
        <div class="text-sm text-gray-600">
          <strong>Bot:</strong> Se han encontrado 88 URLs en total.
        </div>
      </div>
      <form class="flex items-center space-x-2">
        <input
          type="text"
          placeholder="Escribe tu mensaje..."
          class="flex-1 border border-gray-300 rounded px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-400"
        />
        <button
          type="submit"
          class="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
        >
          Enviar
        </button>
      </form>
    </div>
  </div>
</template>

<script setup></script>
