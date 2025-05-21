<template>
  <div class="flex h-screen">
    <Sidebar />
    <div class="flex-1 flex flex-col bg-gray-100">
      <Topbar />
      <main class="p-10 overflow-y-auto">
        <router-view></router-view>
      </main>
    </div>
  </div>
</template>

<script setup>
    import Topbar from "../components/Topbar.vue";
    import Sidebar from "../components/Sidebar.vue";
</script>
