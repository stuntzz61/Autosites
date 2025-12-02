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
import AdminLoginPage from './pages/AdminLoginPage'

// Loading
import LoadingScreen from './components/LoadingScreen'

function App() {
  const location = useLocation()
  const { isReady, webApp } = useTelegram()
  const { user, isLoading, isAdmin } = useAuthStore()

  // Show loading while initializing
  if (!isReady || isLoading) {
    return <LoadingScreen />
  }

  // Not authenticated
  if (!user) {
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
        </Route>

        {/* Admin Login Route */}
        <Route path="/admin-login" element={<AdminLoginPage />} />

        {/* Admin Routes */}
        {isAdmin ? (
          <Route path="/admin" element={<AdminLayout />}>
            <Route index element={<AdminDashboard />} />
            <Route path="managers" element={<AdminManagers />} />
            <Route path="requests" element={<AdminRequests />} />
            <Route path="stats" element={<AdminStats />} />
            <Route path="broadcast" element={<AdminBroadcast />} />
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
