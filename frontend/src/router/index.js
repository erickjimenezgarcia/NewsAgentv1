import { createRouter, createWebHistory } from 'vue-router'
import MainView from '../views/MainView.vue'
import Procesar from '../pages/Procesar.vue'
import Dashboard from '../pages/Dashboard.vue'
import Chat from '../pages/Chat.vue'
import Colecciones from '../pages/Colecciones.vue'

const routes = [
  {
    path: '/',
    component: MainView,
    children: [
      { path: '', redirect: '/dashboard' },
      { path: 'procesar', component: Procesar },
      { path: 'dashboard', component: Dashboard },
      { path: 'chat', component: Chat },
      { path: 'colecciones', component: Colecciones }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router