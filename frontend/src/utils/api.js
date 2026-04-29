import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_BACKEND_URL || ''

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add auth token to requests
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('jwt_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Handle 401 errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('jwt_token')
      localStorage.removeItem('api_key')
      window.location.reload()
    }
    return Promise.reject(error)
  }
)

// Auth endpoints
export const authAPI = {
  login: (email, password) => api.post('/api/v1/auth/login', { email, password }),
  logout: () => api.post('/api/v1/auth/logout'),
  getDefaultKey: () => api.get('/api/v1/auth/default-key'),
  getToken: (apiKey) => api.post('/api/v1/auth/token', null, {
    headers: { 'X-API-Key': apiKey }
  }),
  getCurrentUser: () => api.get('/api/v1/auth/me'),
  listApiKeys: () => api.get('/api/v1/auth/keys'),
  createApiKey: (name) => api.post('/api/v1/auth/keys', { name }),
  deleteApiKey: (id) => api.delete(`/api/v1/auth/keys/${id}`),
  registerUser: (email, password, role) => api.post('/api/v1/auth/register', { email, password, role }),
}

// Activity endpoints (admin only)
export const activityAPI = {
  getLogs: (params) => api.get('/api/v1/activity/logs', { params }),
  getStats: () => api.get('/api/v1/activity/stats'),
}

// Task endpoints
export const taskAPI = {
  list: (params) => api.get('/api/v1/tasks/', { params }),
  get: (id) => api.get(`/api/v1/tasks/${id}`),
  create: (data) => api.post('/api/v1/tasks/', data),
  cancel: (id) => api.post(`/api/v1/tasks/${id}/cancel`),
  summary: () => api.get('/api/v1/tasks/summary'),
}

// Monitoring endpoints
export const monitoringAPI = {
  health: () => api.get('/api/v1/monitoring/health'),
  tasksSummary: () => api.get('/api/v1/monitoring/tasks/summary'),
  scrapers: () => api.get('/api/v1/monitoring/scrapers'),
}

// Export endpoints
export const exportAPI = {
  stats: (params) => api.get('/api/v1/export/stats', { params }),
  csvColumns: () => api.get('/api/v1/export/csv/columns'),
  previewData: (params) => api.get('/api/v1/export/preview', { params }),
  downloadProductsCsv: (params) => api.get('/api/v1/export/csv/products', {
    params,
    responseType: 'blob'
  }),
  downloadResultsCsv: (params) => api.get('/api/v1/export/csv/results', {
    params,
    responseType: 'blob'
  }),
  changeStats: (params) => api.get('/api/v1/export/changes/stats', { params }),
  downloadChangesCsv: (params) => api.get('/api/v1/export/csv/changes', {
    params,
    responseType: 'blob',
  }),
}

export default api
