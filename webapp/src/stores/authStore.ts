import { create } from 'zustand'
import { api } from '@/api/client'

interface User {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  contact?: string
  role: 'guest' | 'manager' | 'admin'
  approval_status: 'pending' | 'approved' | 'rejected'
  is_blocked: boolean
  created_at: string
  full_name?: string
  phone?: string
  email?: string
  registration_completed_at?: string | null
  stats?: {
    total_requests: number
    completed_requests: number
    pending_requests: number
    this_week: number
    today: number
  }
}

interface AuthState {
  user: User | null
  isLoading: boolean
  isAdmin: boolean
  error: string | null

  // Actions
  init: (initData: string) => Promise<void>
  logout: () => void
  refreshUser: () => Promise<void>
  setAdmin: (isAdmin: boolean) => void
}

export const useAuthStore = create<AuthState>((set, get) => ({
  user: null,
  isLoading: true,
  isAdmin: false,
  error: null,

  init: async (initData: string) => {
    try {
      set({ isLoading: true, error: null })

      // Verify with backend
      const response = await api.post('/auth/verify', { initData })
      const user = response.data.user

      set({
        user,
        isAdmin: user.role === 'admin',
        isLoading: false,
      })
    } catch (error: any) {
      console.error('Auth error:', error)
      set({
        user: null,
        isAdmin: false,
        isLoading: false,
        error: error.response?.data?.detail || 'Ошибка авторизации',
      })
    }
  },

  logout: () => {
    set({ user: null, isAdmin: false })
  },

  refreshUser: async () => {
    try {
      const response = await api.get('/auth/me')
      const user = response.data
      set({
        user,
        isAdmin: user.role === 'admin',
      })
    } catch (error) {
      console.error('Refresh user error:', error)
    }
  },

  setAdmin: (isAdmin: boolean) => {
    set((state) => ({
      isAdmin,
      user: state.user ? { ...state.user, role: isAdmin ? 'admin' : state.user.role } : null,
    }))
  },
}))

