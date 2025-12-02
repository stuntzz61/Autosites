import { useCallback, useEffect, useState } from 'react'

interface TelegramUser {
  id: number
  first_name: string
  last_name?: string
  username?: string
  language_code?: string
  is_premium?: boolean
}

export function useTelegram() {
  const tg = window.Telegram?.WebApp
  const [isReady, setIsReady] = useState(false)

  useEffect(() => {
    if (tg) {
      setIsReady(true)
    }
  }, [tg])

  // Haptic feedback
  const hapticFeedback = useCallback((type: 'light' | 'medium' | 'heavy' | 'success' | 'warning' | 'error' | 'selection') => {
    if (!tg?.HapticFeedback) return

    switch (type) {
      case 'light':
      case 'medium':
      case 'heavy':
        tg.HapticFeedback.impactOccurred(type)
        break
      case 'success':
      case 'warning':
      case 'error':
        tg.HapticFeedback.notificationOccurred(type)
        break
      case 'selection':
        tg.HapticFeedback.selectionChanged()
        break
    }
  }, [tg])

  // Main Button
  const showMainButton = useCallback((text: string, onClick: () => void) => {
    if (!tg?.MainButton) return

    tg.MainButton.setText(text)
    tg.MainButton.onClick(onClick)
    tg.MainButton.show()
  }, [tg])

  const hideMainButton = useCallback(() => {
    if (!tg?.MainButton) return
    tg.MainButton.hide()
  }, [tg])

  const setMainButtonLoading = useCallback((loading: boolean) => {
    if (!tg?.MainButton) return

    if (loading) {
      tg.MainButton.showProgress()
    } else {
      tg.MainButton.hideProgress()
    }
  }, [tg])

  // Back Button
  const showBackButton = useCallback((onClick: () => void) => {
    if (!tg?.BackButton) return

    tg.BackButton.onClick(onClick)
    tg.BackButton.show()
  }, [tg])

  const hideBackButton = useCallback(() => {
    if (!tg?.BackButton) return
    tg.BackButton.hide()
  }, [tg])

  // Close app
  const close = useCallback(() => {
    tg?.close()
  }, [tg])

  // Expand app
  const expand = useCallback(() => {
    tg?.expand()
  }, [tg])

  return {
    tg,
    isReady,
    user: tg?.initDataUnsafe?.user as TelegramUser | undefined,
    colorScheme: tg?.colorScheme || 'light',
    viewportHeight: tg?.viewportHeight || window.innerHeight,
    viewportStableHeight: tg?.viewportStableHeight || window.innerHeight,
    initData: tg?.initData || '',
    startParam: tg?.initDataUnsafe?.start_param,

    // Methods
    hapticFeedback,
    showMainButton,
    hideMainButton,
    setMainButtonLoading,
    showBackButton,
    hideBackButton,
    close,
    expand,
  }
}

