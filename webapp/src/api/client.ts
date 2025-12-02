import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || '/api'

export const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add init data to requests
api.interceptors.request.use((config) => {
  const tg = window.Telegram?.WebApp
  if (tg?.initData) {
    config.headers['X-Telegram-Init-Data'] = tg.initData
  }
  return config
})

// Handle errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Redirect to auth or show error
      console.error('Unauthorized')
    }
    return Promise.reject(error)
  }
)

// API functions
export const authApi = {
  verify: (initData: string) => api.post('/auth/verify', { initData }),
  me: () => api.get('/auth/me'),
}

export const requestsApi = {
  list: (params?: { page?: number; limit?: number; status?: string }) =>
    api.get('/requests', { params }),

  get: (id: string) => api.get(`/requests/${id}`),

  create: (data: any) => api.post('/requests', data),

  update: (id: string, data: any) => api.patch(`/requests/${id}`, data),

  delete: (id: string) => api.delete(`/requests/${id}`),

  updateStatus: (id: string, status: string) =>
    api.patch(`/requests/${id}/status`, { status }),

  archive: (id: string) => api.post(`/requests/${id}/archive`),

  generate: (id: string) => api.post(`/requests/${id}/generate`),

  uploadPhotos: (id: string, formData: FormData) =>
    api.post(`/requests/${id}/photos`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),

  deletePhoto: (id: string, photoId: string) =>
    api.delete(`/requests/${id}/photos/${photoId}`),
}

export const adminApi = {
  dashboard: () => api.get('/admin/dashboard'),

  managers: {
    list: () => api.get('/admin/managers'),
    get: (id: string) => api.get(`/admin/managers/${id}`),
    block: (id: string) => api.post(`/admin/managers/${id}/block`),
    unblock: (id: string) => api.post(`/admin/managers/${id}/unblock`),
    delete: (id: string) => api.delete(`/admin/managers/${id}`),
  },

  pending: {
    list: () => api.get('/admin/pending'),
    approve: (id: string) => api.post(`/admin/pending/${id}/approve`),
    reject: (id: string, reason: string) =>
      api.post(`/admin/pending/${id}/reject`, { reason }),
  },

  requests: {
    list: (params?: { page?: number; limit?: number; status?: string }) =>
      api.get('/admin/requests', { params }),
    search: (query: string) => api.get('/admin/requests/search', { params: { q: query } }),
    massArchive: (ids: string[]) => api.post('/admin/requests/mass-archive', { ids }),
    massDelete: (ids: string[]) => api.post('/admin/requests/mass-delete', { ids }),
  },

  broadcast: (data: { message: string; photo?: string; recipient_ids?: string[] }) =>
    api.post('/admin/broadcast', data),

  stats: {
    overview: () => api.get('/admin/stats/overview'),
    byStatus: () => api.get('/admin/stats/by-status'),
    byDay: (days?: number) => api.get('/admin/stats/by-day', { params: { days } }),
    managers: () => api.get('/admin/stats/managers'),
  },

  export: {
    excel: () => api.get('/admin/export/excel', { responseType: 'blob' }),
    pdf: () => api.get('/admin/export/pdf', { responseType: 'blob' }),
  },
}

export const profileApi = {
  get: () => api.get('/profile'),
  update: (data: { contact?: string }) => api.patch('/profile', data),
  stats: () => api.get('/profile/stats'),
}
