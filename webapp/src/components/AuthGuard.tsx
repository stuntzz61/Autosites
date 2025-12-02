import { ReactNode } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'

interface AuthGuardProps {
  children: ReactNode
  requireAdmin?: boolean
}

export default function AuthGuard({ children, requireAdmin = false }: AuthGuardProps) {
  const { isAuthenticated, user } = useAuthStore()

  if (!isAuthenticated || !user) {
    return <Navigate to="/" replace />
  }

  if (user.approval_status === 'pending') {
    return (
      <div className="min-h-screen flex items-center justify-center p-4">
        <div className="text-center max-w-sm">
          <div className="text-6xl mb-4">⏳</div>
          <h1 className="text-xl font-semibold mb-2">Ожидание одобрения</h1>
          <p className="text-tg-hint">
            Ваша заявка на регистрацию находится на рассмотрении администратора.
            Мы уведомим вас, когда она будет одобрена.
          </p>
        </div>
      </div>
    )
  }

  if (user.approval_status === 'rejected') {
    return (
      <div className="min-h-screen flex items-center justify-center p-4">
        <div className="text-center max-w-sm">
          <div className="text-6xl mb-4">❌</div>
          <h1 className="text-xl font-semibold mb-2">Заявка отклонена</h1>
          <p className="text-tg-hint">
            К сожалению, ваша заявка на регистрацию была отклонена.
            Обратитесь к администратору для получения информации.
          </p>
        </div>
      </div>
    )
  }

  if (requireAdmin && user.role !== 'admin') {
    return <Navigate to="/" replace />
  }

  return <>{children}</>
}

