import { ReactNode } from 'react'
import { Navigate } from 'react-router-dom'
import { Clock, XCircle } from 'lucide-react'
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
          <div className="w-20 h-20 rounded-full bg-amber-500/20 flex items-center justify-center mx-auto mb-4">
            <Clock className="w-10 h-10 text-amber-400" />
          </div>
          <h1 className="text-xl font-semibold mb-2 text-slate-100">Ожидание одобрения</h1>
          <p className="text-slate-400">
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
          <div className="w-20 h-20 rounded-full bg-red-500/20 flex items-center justify-center mx-auto mb-4">
            <XCircle className="w-10 h-10 text-red-400" />
          </div>
          <h1 className="text-xl font-semibold mb-2 text-slate-100">Заявка отклонена</h1>
          <p className="text-slate-400">
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

