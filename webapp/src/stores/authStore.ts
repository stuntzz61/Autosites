import { create } from 'zustand'
import { api } from '@/api/client'

export type UserRole = 'guest' | 'manager' | 'supervisor' | 'director' | 'owner'

interface User {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  contact?: string
  role: UserRole
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
  isAdmin: boolean // Legacy: true for supervisor, director, owner
  isSupervisor: boolean // supervisor, director, owner
  isDirector: boolean // director, owner
  isOwner: boolean // owner only
  error: string | null

  // Actions
  init: (initData: string) => Promise<void>
  logout: () => void
  refreshUser: () => Promise<void>
  setAdmin: (isAdmin: boolean) => void
}

// Helper functions
export function isSupervisorRole(role: UserRole): boolean {
  return role === 'supervisor' || role === 'director' || role === 'owner'
}

export function isDirectorRole(role: UserRole): boolean {
  return role === 'director' || role === 'owner'
}

export function isOwnerRole(role: UserRole): boolean {
  return role === 'owner'
}

export function getRoleLabel(role: UserRole): string {
  const labels: Record<UserRole, string> = {
    guest: 'Гость',
    manager: 'Менеджер',
    supervisor: 'Супервайзер',
    director: 'Директор',
    owner: 'Владелец',
  }
  return labels[role] || role
}

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  isLoading: true,
  isAdmin: false,
  isSupervisor: false,
  isDirector: false,
  isOwner: false,
  error: null,

  init: async (initData: string) => {
    try {
      set({ isLoading: true, error: null })

      // Verify with backend
      const response = await api.post('/auth/verify', { initData })
      const user = response.data.user
      const role = user.role as UserRole

      set({
        user,
        isAdmin: isSupervisorRole(role), // Legacy compatibility
        isSupervisor: isSupervisorRole(role),
        isDirector: isDirectorRole(role),
        isOwner: isOwnerRole(role),
        isLoading: false,
      })
    } catch (error: any) {
      console.error('Auth error:', error)
      set({
        user: null,
        isAdmin: false,
        isSupervisor: false,
        isDirector: false,
        isOwner: false,
        isLoading: false,
        error: error.response?.data?.detail || 'Ошибка авторизации',
      })
    }
  },

  logout: () => {
    set({
      user: null,
      isAdmin: false,
      isSupervisor: false,
      isDirector: false,
      isOwner: false,
    })
  },

  refreshUser: async () => {
    try {
      const response = await api.get('/auth/me')
      const user = response.data
      const role = user.role as UserRole

      set({
        user,
        isAdmin: isSupervisorRole(role), // Legacy compatibility
        isSupervisor: isSupervisorRole(role),
        isDirector: isDirectorRole(role),
        isOwner: isOwnerRole(role),
      })
    } catch (error) {
      console.error('Refresh user error:', error)
    }
  },

  setAdmin: (isAdmin: boolean) => {
    set((state) => {
      const role = isAdmin ? 'supervisor' : (state.user?.role || 'manager')
      return {
        isAdmin,
        isSupervisor: isSupervisorRole(role),
        isDirector: isDirectorRole(role),
        isOwner: isOwnerRole(role),
        user: state.user ? { ...state.user, role } : null,
      }
    })
  },
}))

