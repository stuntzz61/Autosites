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

  deletePhoto: (id: string, photoUrl: string) =>
    api.delete(`/requests/${id}/photos`, { params: { url: photoUrl } }),
}

export const adminApi = {
  dashboard: () => api.get('/admin/dashboard'),

  managers: {
    list: () => api.get('/admin/managers'),
    get: (id: string) => api.get(`/admin/managers/${id}`),
    block: (id: string) => api.post(`/admin/managers/${id}/block`),
    unblock: (id: string) => api.post(`/admin/managers/${id}/unblock`),
    delete: (id: string) => api.delete(`/admin/managers/${id}`),
    makeAdmin: (id: string) => api.post(`/admin/managers/${id}/make-admin`),
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

// Additional Services API
export const servicesApi = {
  // List all available additional services
  list: () => api.get('/additional-services'),

  // Get services for a specific request
  getForRequest: (requestId: string) => api.get(`/requests/${requestId}/services`),

  // Add service to request
  add: (requestId: string, data: { service_id: string; notes?: string; price?: string }) =>
    api.post(`/requests/${requestId}/services`, data),

  // Update service on request
  update: (requestId: string, serviceId: string, data: { status?: string; notes?: string; price?: string }) =>
    api.patch(`/requests/${requestId}/services/${serviceId}`, data),

  // Remove service from request
  remove: (requestId: string, serviceId: string) =>
    api.delete(`/requests/${requestId}/services/${serviceId}`),
}

// Manager Feedback API
export const feedbackApi = {
  // Create new feedback
  create: (data: {
    subject: string
    message: string
    category?: string
    priority?: string
    request_id?: string
  }) => api.post('/feedback', data),

  // List own feedback
  list: (params?: { status?: string; page?: number; limit?: number }) =>
    api.get('/feedback', { params }),

  // Get feedback details
  get: (id: string) => api.get(`/feedback/${id}`),

  // Admin: list all feedback
  adminList: (params?: { status?: string; page?: number; limit?: number }) =>
    api.get('/admin/feedback', { params }),

  // Admin: respond to feedback
  adminRespond: (id: string, data: { response: string; status?: string }) =>
    api.post(`/admin/feedback/${id}/respond`, data),

  // Admin: update status
  adminUpdateStatus: (id: string, status: string) =>
    api.patch(`/admin/feedback/${id}/status`, { status }),

  // Admin: get new count
  adminCount: () => api.get('/admin/feedback/count'),
}

// Sites API
export const sitesApi = {
  // List sites (for manager)
  list: (params?: { deploy_status?: string; hosting_plan?: string; page?: number; limit?: number }) =>
    api.get('/sites', { params }),

  // Get site details
  get: (id: string) => api.get(`/sites/${id}`),

  // Get site by request ID
  getByRequest: (requestId: string) => api.get(`/sites/by-request/${requestId}`),

  // Get deployment history
  getHistory: (id: string, limit?: number) =>
    api.get(`/sites/${id}/history`, { params: { limit } }),

  // Create site from request
  create: (data: { request_id: string; company_name: string; hosting_plan?: string }) =>
    api.post('/sites', data),

  // Update site
  update: (id: string, data: any) => api.patch(`/sites/${id}`, data),

  // Deploy site
  deploy: (id: string) => api.post(`/sites/${id}/deploy`),

  // Stop site
  stop: (id: string) => api.post(`/sites/${id}/stop`),

  // Assign domain
  assignDomain: (id: string, domain: string, enableSsl?: boolean) =>
    api.post(`/sites/${id}/domain`, { domain, enable_ssl: enableSsl }),

  // Extend hosting
  extendHosting: (id: string, plan: string, months: number) =>
    api.post(`/sites/${id}/extend`, { plan, months }),

  // Delete site
  delete: (id: string) => api.delete(`/sites/${id}`),

  // Admin: list all sites
  adminList: (params?: { deploy_status?: string; manager_id?: string; page?: number; limit?: number }) =>
    api.get('/sites/admin/all', { params }),

  // Admin: force redeploy
  adminForceDeploy: (id: string) => api.post(`/sites/admin/${id}/force-deploy`),

  // Get hosting plans
  getPlans: () => api.get('/sites/plans'),
}

// Payment API
export const paymentApi = {
  // Create payment
  create: (data: { site_id: string; plan: string; months: number }) =>
    api.post('/payments', data),

  // Get payment details
  get: (id: string) => api.get(`/payments/${id}`),

  // Get payment QR code
  getQR: (id: string) => api.get(`/payments/${id}/qr`),

  // Verify payment status
  verify: (id: string) => api.post(`/payments/${id}/verify`),

  // List payments for site
  listBySite: (siteId: string) => api.get('/payments', { params: { site_id: siteId } }),
}
