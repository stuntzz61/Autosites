import { createContext, useContext, useEffect, useState, ReactNode } from 'react'
import { useAuthStore } from '../stores/authStore'

// Telegram WebApp types
interface TelegramUser {
  id: number
  first_name: string
  last_name?: string
  username?: string
  language_code?: string
  is_premium?: boolean
  photo_url?: string
}

interface ThemeParams {
  bg_color?: string
  text_color?: string
  hint_color?: string
  link_color?: string
  button_color?: string
  button_text_color?: string
  secondary_bg_color?: string
  header_bg_color?: string
  accent_text_color?: string
  section_bg_color?: string
  section_header_text_color?: string
  subtitle_text_color?: string
  destructive_text_color?: string
}

interface WebApp {
  initData: string
  initDataUnsafe: {
    user?: TelegramUser
    auth_date?: number
    hash?: string
    query_id?: string
    start_param?: string
  }
  version: string
  platform: string
  colorScheme: 'light' | 'dark'
  themeParams: ThemeParams
  isExpanded: boolean
  viewportHeight: number
  viewportStableHeight: number
  headerColor: string
  backgroundColor: string
  isClosingConfirmationEnabled: boolean
  BackButton: {
    isVisible: boolean
    show: () => void
    hide: () => void
    onClick: (callback: () => void) => void
    offClick: (callback: () => void) => void
  }
  MainButton: {
    text: string
    color: string
    textColor: string
    isVisible: boolean
    isActive: boolean
    isProgressVisible: boolean
    setText: (text: string) => void
    show: () => void
    hide: () => void
    enable: () => void
    disable: () => void
    showProgress: (leaveActive?: boolean) => void
    hideProgress: () => void
    onClick: (callback: () => void) => void
    offClick: (callback: () => void) => void
    setParams: (params: { text?: string; color?: string; text_color?: string; is_active?: boolean; is_visible?: boolean }) => void
  }
  HapticFeedback: {
    impactOccurred: (style: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft') => void
    notificationOccurred: (type: 'error' | 'success' | 'warning') => void
    selectionChanged: () => void
  }
  ready: () => void
  expand: () => void
  close: () => void
  enableClosingConfirmation: () => void
  disableClosingConfirmation: () => void
  setHeaderColor: (color: string) => void
  setBackgroundColor: (color: string) => void
  showPopup: (params: { title?: string; message: string; buttons?: Array<{ id?: string; type?: 'default' | 'ok' | 'close' | 'cancel' | 'destructive'; text?: string }> }, callback?: (buttonId: string) => void) => void
  showAlert: (message: string, callback?: () => void) => void
  showConfirm: (message: string, callback?: (confirmed: boolean) => void) => void
  openLink: (url: string, options?: { try_instant_view?: boolean }) => void
  openTelegramLink: (url: string) => void
  openInvoice: (url: string, callback?: (status: string) => void) => void
  showScanQrPopup: (params: { text?: string }, callback?: (text: string) => boolean) => void
  closeScanQrPopup: () => void
  readTextFromClipboard: (callback?: (text: string) => void) => void
  requestWriteAccess: (callback?: (granted: boolean) => void) => void
  requestContact: (callback?: (shared: boolean) => void) => void
  switchInlineQuery: (query: string, choose_chat_types?: string[]) => void
}

declare global {
  interface Window {
    Telegram?: {
      WebApp: WebApp
    }
  }
}

interface TelegramContextValue {
  webApp: WebApp | null
  user: TelegramUser | null
  initData: string
  colorScheme: 'light' | 'dark'
  isReady: boolean
  haptic: WebApp['HapticFeedback'] | null
  mainButton: WebApp['MainButton'] | null
  backButton: WebApp['BackButton'] | null
}

const TelegramContext = createContext<TelegramContextValue | null>(null)

export function TelegramProvider({ children }: { children: ReactNode }) {
  const [webApp, setWebApp] = useState<WebApp | null>(null)
  const [isReady, setIsReady] = useState(false)
  const { init } = useAuthStore()

  useEffect(() => {
    const tg = window.Telegram?.WebApp

    if (tg) {
      // Initialize
      tg.ready()
      tg.expand()

      // Set dark mode class first
      const isDark = tg.colorScheme === 'dark'
      if (isDark) {
        document.documentElement.classList.add('dark')
      } else {
        document.documentElement.classList.remove('dark')
      }

      // Apply Telegram theme colors only if they exist and improve dark theme
      const theme = tg.themeParams
      if (theme.bg_color) {
        // If Telegram sends pure black (#000000), use a nicer dark blue instead
        const bgColor = (theme.bg_color === '#000000' || theme.bg_color === '#000') 
          ? '#0f172a' 
          : theme.bg_color
        document.documentElement.style.setProperty('--tg-theme-bg-color', bgColor)
      }
      if (theme.text_color) {
        document.documentElement.style.setProperty('--tg-theme-text-color', theme.text_color)
      }
      if (theme.hint_color) {
        document.documentElement.style.setProperty('--tg-theme-hint-color', theme.hint_color)
      }
      if (theme.link_color) {
        document.documentElement.style.setProperty('--tg-theme-link-color', theme.link_color)
      }
      if (theme.button_color) {
        document.documentElement.style.setProperty('--tg-theme-button-color', theme.button_color)
      }
      if (theme.button_text_color) {
        document.documentElement.style.setProperty('--tg-theme-button-text-color', theme.button_text_color)
      }
      if (theme.secondary_bg_color) {
        // Improve pure black secondary bg too
        const secBgColor = (theme.secondary_bg_color === '#000000' || theme.secondary_bg_color === '#000')
          ? '#1e293b'
          : theme.secondary_bg_color
        document.documentElement.style.setProperty('--tg-theme-secondary-bg-color', secBgColor)
      }
      if (theme.header_bg_color) {
        document.documentElement.style.setProperty('--tg-theme-header-bg-color', theme.header_bg_color)
      }
      if (theme.section_bg_color) {
        // Improve pure black section bg
        const secColor = (theme.section_bg_color === '#000000' || theme.section_bg_color === '#000')
          ? '#1e293b'
          : theme.section_bg_color
        document.documentElement.style.setProperty('--tg-theme-section-bg-color', secColor)
      }
      if (theme.section_header_text_color) {
        document.documentElement.style.setProperty('--tg-theme-section-header-text-color', theme.section_header_text_color)
      }
      if (theme.accent_text_color) {
        document.documentElement.style.setProperty('--tg-theme-accent-text-color', theme.accent_text_color)
      }
      if (theme.destructive_text_color) {
        document.documentElement.style.setProperty('--tg-theme-destructive-text-color', theme.destructive_text_color)
      }
      if (theme.subtitle_text_color) {
        document.documentElement.style.setProperty('--tg-theme-subtitle-text-color', theme.subtitle_text_color)
      }

      setWebApp(tg)

      // Initialize auth with Telegram init data
      if (tg.initData) {
        init(tg.initData)
      }

      setIsReady(true)
    } else {
      // Development mode without Telegram - use light theme
      console.log('Running outside of Telegram')
      setIsReady(true)
    }
  }, [init])

  const value: TelegramContextValue = {
    webApp,
    user: webApp?.initDataUnsafe?.user || null,
    initData: webApp?.initData || '',
    colorScheme: webApp?.colorScheme || 'light',
    isReady,
    haptic: webApp?.HapticFeedback || null,
    mainButton: webApp?.MainButton || null,
    backButton: webApp?.BackButton || null,
  }

  return (
    <TelegramContext.Provider value={value}>
      {children}
    </TelegramContext.Provider>
  )
}

export function useTelegram() {
  const context = useContext(TelegramContext)
  if (!context) {
    throw new Error('useTelegram must be used within TelegramProvider')
  }
  return context
}

