import { Routes, Route, Navigate } from 'react-router-dom'
import { AnimatePresence } from 'framer-motion'
import { useLocation } from 'react-router-dom'
import { useTelegram } from './contexts/TelegramContext'
import { useAuthStore } from './stores/authStore'

// Layouts
import MainLayout from './layouts/MainLayout'
import AdminLayout from './layouts/AdminLayout'

// Pages
import HomePage from './pages/HomePage'
import RequestsPage from './pages/RequestsPage'
import RequestDetailPage from './pages/RequestDetailPage'
import NewRequestPage from './pages/NewRequestPage'
import ProfilePage from './pages/ProfilePage'
import ArchivePage from './pages/ArchivePage'

// Admin Pages
import AdminDashboard from './pages/admin/AdminDashboard'
import AdminManagers from './pages/admin/AdminManagers'
import AdminRequests from './pages/admin/AdminRequests'
import AdminStats from './pages/admin/AdminStats'
import AdminBroadcast from './pages/admin/AdminBroadcast'
import AdminFeedback from './pages/admin/AdminFeedback'
import AdminLoginPage from './pages/AdminLoginPage'
import DevLoginPage from './pages/DevLoginPage'

// Manager Pages
import FeedbackPage from './pages/FeedbackPage'
import PaymentPage from './pages/PaymentPage'

// Admin Pages
import AdminSites from './pages/admin/AdminSites'

// Loading
import LoadingScreen from './components/LoadingScreen'

function App() {
  const location = useLocation()
  const { isReady, webApp } = useTelegram()
  const { user, isLoading, isAdmin } = useAuthStore()

  // Check if running in browser (not Telegram)
  const isBrowser = typeof window !== 'undefined' && !window.Telegram?.WebApp

  // Show loading while initializing (only in Telegram)
  if (!isBrowser && (!isReady || isLoading)) {
    return <LoadingScreen />
  }

  // Check if user is blocked
  if (user?.is_blocked) {
    return (
      <div className="flex items-center justify-center min-h-screen p-4">
        <div className="text-center">
          <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
            </svg>
          </div>
          <h1 className="text-xl font-semibold text-tg-text mb-2">
            Аккаунт заблокирован
          </h1>
          <p className="text-tg-hint mb-4">
            Ваш аккаунт был заблокирован администратором.
          </p>
          <p className="text-sm text-tg-hint">
            Для разблокировки свяжитесь с администратором.
          </p>
        </div>
      </div>
    )
  }

  // Check if user is pending approval
  if (user && user.approval_status === 'pending') {
    return (
      <div className="flex items-center justify-center min-h-screen p-4">
        <div className="text-center">
          <div className="w-16 h-16 bg-orange-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-orange-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h1 className="text-xl font-semibold text-tg-text mb-2">
            Ожидание одобрения
          </h1>
          <p className="text-tg-hint">
            Ваша заявка на регистрацию находится на рассмотрении.
            Мы уведомим вас, когда администратор примет решение.
          </p>
        </div>
      </div>
    )
  }

  // Check if user is rejected
  if (user && user.approval_status === 'rejected') {
    return (
      <div className="flex items-center justify-center min-h-screen p-4">
        <div className="text-center">
          <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </div>
          <h1 className="text-xl font-semibold text-tg-text mb-2">
            Заявка отклонена
          </h1>
          <p className="text-tg-hint">
            К сожалению, ваша заявка была отклонена.
            Свяжитесь с администратором для уточнения деталей.
          </p>
        </div>
      </div>
    )
  }

  // Not authenticated
  if (!user) {
    // In browser, show dev login page
    if (isBrowser && location.pathname !== '/dev-login' && location.pathname !== '/admin-login') {
      return <DevLoginPage />
    }

    // In Telegram, show welcome message
    if (!isBrowser) {
      return (
        <div className="flex items-center justify-center min-h-screen p-4">
          <div className="text-center">
            <h1 className="text-xl font-semibold text-tg-text mb-2">
              Добро пожаловать!
            </h1>
            <p className="text-tg-hint">
              Откройте приложение через бота @weblyMN_bot
            </p>
          </div>
        </div>
      )
    }
  }

  return (
    <AnimatePresence mode="wait">
      <Routes location={location} key={location.pathname}>
        {/* Main Routes */}
        <Route element={<MainLayout />}>
          <Route path="/" element={<HomePage />} />
          <Route path="/requests" element={<RequestsPage />} />
          <Route path="/requests/new" element={<NewRequestPage />} />
          <Route path="/requests/:id" element={<RequestDetailPage />} />
          <Route path="/archive" element={<ArchivePage />} />
          <Route path="/profile" element={<ProfilePage />} />
          <Route path="/feedback" element={<FeedbackPage />} />
          <Route path="/sites/:siteId/payment" element={<PaymentPage />} />
        </Route>

        {/* Dev & Admin Login Routes */}
        <Route path="/dev-login" element={<DevLoginPage />} />
        <Route path="/admin-login" element={<AdminLoginPage />} />

        {/* Admin Routes */}
        {isAdmin ? (
          <Route path="/admin" element={<AdminLayout />}>
            <Route index element={<AdminDashboard />} />
            <Route path="managers" element={<AdminManagers />} />
            <Route path="requests" element={<AdminRequests />} />
            <Route path="stats" element={<AdminStats />} />
            <Route path="broadcast" element={<AdminBroadcast />} />
            <Route path="feedback" element={<AdminFeedback />} />
            <Route path="sites" element={<AdminSites />} />
          </Route>
        ) : (
          <Route path="/admin/*" element={<Navigate to="/admin-login" replace />} />
        )}

        {/* Fallback */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AnimatePresence>
  )
}

export default App
