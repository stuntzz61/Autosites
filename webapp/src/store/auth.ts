import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { api } from '../api/client'

export interface User {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  contact?: string
  role: 'guest' | 'manager' | 'admin'
  approval_status: 'pending' | 'approved' | 'rejected'
  created_at: string
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
  isAuthenticated: boolean
  isLoading: boolean
  error: string | null

  // Actions
  init: () => Promise<void>
  logout: () => void
  refreshUser: () => Promise<void>
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      isAuthenticated: false,
      isLoading: true,
      error: null,

      init: async () => {
        set({ isLoading: true, error: null })

        try {
          // Получаем initData из Telegram WebApp
          const tg = window.Telegram?.WebApp

          if (!tg?.initData) {
            // Для разработки без Telegram
            if (import.meta.env.DEV) {
              // Mock user для разработки
              set({
                user: {
                  id: 'dev-user',
                  tg_id: 123456789,
                  username: 'developer',
                  first_name: 'Developer',
                  role: 'admin',
                  approval_status: 'approved',
                  created_at: new Date().toISOString(),
                  stats: {
                    total_requests: 10,
                    completed_requests: 5,
                    pending_requests: 3,
                    this_week: 4,
                    today: 1,
                  }
                },
                isAuthenticated: true,
                isLoading: false,
              })
              return
            }

            throw new Error('Telegram WebApp not available')
          }

          // Авторизация через API
          const response = await api.post<{ user: User }>('/auth/telegram', {
            init_data: tg.initData,
          })

          set({
            user: response.data.user,
            isAuthenticated: true,
            isLoading: false,
          })

        } catch (error: any) {
          console.error('Auth error:', error)
          set({
            user: null,
            isAuthenticated: false,
            isLoading: false,
            error: error.message || 'Authentication failed',
          })
        }
      },

      logout: () => {
        set({
          user: null,
          isAuthenticated: false,
          error: null,
        })
      },

      refreshUser: async () => {
        try {
          const response = await api.get<{ user: User }>('/auth/me')
          set({ user: response.data.user })
        } catch (error) {
          console.error('Refresh user error:', error)
        }
      },
    }),
    {
      name: 'auth-storage',
      partialize: (state) => ({ user: state.user }),
    }
  )
)

