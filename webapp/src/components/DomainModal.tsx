import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Globe, Search, CheckCircle2, XCircle, Loader2, AlertCircle,
  ChevronRight, RefreshCw, Shield, CreditCard, X
} from 'lucide-react'
import { useMutation } from '@tanstack/react-query'
import { domainApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

type DomainStatus =
  | 'idle'
  | 'checking'
  | 'available'
  | 'taken'
  | 'invalid'
  | 'registering'
  | 'bill_created'
  | 'configuring'
  | 'connected'
  | 'error'

interface Alternative {
  domain: string
  price?: { currency: string; amount: number }
}

interface DomainModalProps {
  isOpen: boolean
  onClose: () => void
  requestId: string
  currentDomain?: string
  onDomainRegistered?: (domain: string) => void
}

export default function DomainModal({
  isOpen,
  onClose,
  requestId,
  currentDomain,
  onDomainRegistered
}: DomainModalProps) {
  const [domainInput, setDomainInput] = useState(currentDomain || '')
  const [status, setStatus] = useState<DomainStatus>('idle')
  const [price, setPrice] = useState<{ currency: string; amount: number } | null>(null)
  const [alternatives, setAlternatives] = useState<Alternative[]>([])
  const [errorMessage, setErrorMessage] = useState('')
  const [billId, setBillId] = useState<string | null>(null)
  const [registeredDomain, setRegisteredDomain] = useState<string | null>(null)

  // Reset state when modal opens
  useEffect(() => {
    if (isOpen) {
      setDomainInput(currentDomain || '')
      setStatus('idle')
      setPrice(null)
      setAlternatives([])
      setErrorMessage('')
      setBillId(null)
      setRegisteredDomain(null)
    }
  }, [isOpen, currentDomain])

  // Check domain mutation
  const checkMutation = useMutation({
    mutationFn: (domain: string) => domainApi.check(requestId, {
      domain,
      max_suggestions: 15
    }),
    onMutate: () => {
      setStatus('checking')
      setPrice(null)
      setAlternatives([])
      setErrorMessage('')
    },
    onSuccess: (res) => {
      const data = res.data
      if (data.status === 'available') {
        setStatus('available')
        setPrice(data.price || null)
      } else if (data.status === 'taken') {
        setStatus('taken')
        setAlternatives(data.alternatives || [])
      } else if (data.status === 'invalid_domain') {
        setStatus('invalid')
        setErrorMessage(data.message || 'Некорректный формат домена')
      }
    },
    onError: (err: any) => {
      setStatus('error')
      setErrorMessage(err.response?.data?.detail || 'Ошибка проверки домена')
      console.error('Domain check error:', err)
    }
  })

  // Register domain mutation
  const registerMutation = useMutation({
    mutationFn: (domain: string) => domainApi.register(requestId, {
      domain,
      configure_dns: true
    }),
    onMutate: () => {
      setStatus('registering')
      setErrorMessage('')
    },
    onSuccess: (res) => {
      const data = res.data
      if (data.status === 'registered') {
        setStatus('configuring')
        setRegisteredDomain(data.domain)

        // Wait a bit then show as connected
        setTimeout(() => {
          setStatus('connected')
          toast.success('Домен успешно оформлен!')
          onDomainRegistered?.(data.domain)
        }, 2000)
      } else if (data.status === 'bill_created') {
        setStatus('bill_created')
        setBillId(data.bill_id)
        setErrorMessage(data.message || 'Недостаточно средств на балансе REG.RU')
      } else if (data.status === 'taken') {
        setStatus('taken')
        setErrorMessage('Домен был занят в процессе регистрации')
        // Re-check to get alternatives
        checkMutation.mutate(domainInput)
      } else {
        setStatus('error')
        setErrorMessage(data.message || 'Ошибка регистрации')
      }
    },
    onError: (err: any) => {
      setStatus('error')
      setErrorMessage(err.response?.data?.detail || 'Ошибка регистрации домена')
      console.error('Domain register error:', err)
    }
  })

  const handleCheck = () => {
    const domain = domainInput.trim().toLowerCase()
    if (!domain) {
      toast.error('Введите домен')
      return
    }
    checkMutation.mutate(domain)
  }

  const handleSelectAlternative = (alt: Alternative) => {
    setDomainInput(alt.domain)
    setPrice(alt.price || null)
    setStatus('available')
    setAlternatives([])
  }

  const handleRegister = () => {
    const domain = domainInput.trim().toLowerCase()
    if (!domain) return
    registerMutation.mutate(domain)
  }

  const handleRefreshStatus = () => {
    // Re-check domain status
    checkMutation.mutate(domainInput)
  }

  if (!isOpen) return null

  return (
    <AnimatePresence>
      <motion.div
        className="fixed inset-0 z-[9998]"
        style={{ background: 'rgba(0, 0, 0, 0.7)' }}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
      />
      <motion.div
        className="fixed left-0 right-0 rounded-t-3xl z-[9999] max-h-[80vh] overflow-y-auto"
        style={{
          bottom: 'calc(80px + env(safe-area-inset-bottom, 0px))',
          background: 'var(--bg-elevated)',
          borderTop: '1px solid var(--border-subtle)',
          boxShadow: '0 -10px 40px rgba(0, 0, 0, 0.4)',
          padding: '1.25rem',
          paddingBottom: '1.5rem'
        }}
        initial={{ y: '100%', opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        exit={{ y: '100%', opacity: 0 }}
        transition={{ type: 'spring', damping: 30, stiffness: 400 }}
      >
        <div className="w-10 h-1 rounded-full mx-auto mb-5" style={{ background: 'var(--bg-tertiary)' }} />

        {/* Header */}
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center gap-3">
            <div
              className="w-10 h-10 rounded-xl flex items-center justify-center"
              style={{
                background: 'linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(139, 92, 246, 0.1) 100%)',
                border: '1px solid rgba(139, 92, 246, 0.3)'
              }}
            >
              <Globe className="w-5 h-5 text-purple-400" />
            </div>
            <div>
              <h2 className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>Подключить домен</h2>
              <p className="text-xs" style={{ color: 'var(--text-muted)' }}>Регистрация и настройка DNS</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2.5 rounded-xl transition-all hover:scale-105"
            style={{
              background: 'var(--bg-surface)',
              border: '1px solid var(--border-default)'
            }}
          >
            <X className="w-5 h-5" style={{ color: 'var(--text-muted)' }} />
          </button>
        </div>

        <div className="space-y-4">
          {/* Domain Input */}
          <div>
            <label className="text-xs font-medium mb-2 block" style={{ color: 'var(--text-muted)' }}>Домен</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={domainInput}
                onChange={(e) => {
                  setDomainInput(e.target.value.toLowerCase().replace(/\s/g, ''))
                  if (status !== 'idle' && status !== 'checking') {
                    setStatus('idle')
                  }
                }}
                onKeyDown={(e) => e.key === 'Enter' && handleCheck()}
                placeholder="example.ru"
                className="input flex-1"
                disabled={status === 'registering' || status === 'configuring' || status === 'connected'}
              />
              <button
                onClick={handleCheck}
                disabled={checkMutation.isPending || status === 'registering' || status === 'configuring'}
                className="btn btn-primary px-4 flex-shrink-0"
              >
                {checkMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Search className="w-5 h-5" />
                )}
              </button>
            </div>
            <p className="text-xs mt-2" style={{ color: 'var(--text-subtle)' }}>
              Укажите домен без http:// и www
            </p>
          </div>

          {/* Status Messages */}
          {status === 'checking' && (
            <div
              className="flex items-center gap-3 p-4 rounded-xl"
              style={{
                background: 'rgba(59, 130, 246, 0.1)',
                border: '1px solid rgba(59, 130, 246, 0.2)'
              }}
            >
              <Loader2 className="w-5 h-5 animate-spin" style={{ color: 'var(--accent-primary-light)' }} />
              <span className="text-sm font-medium" style={{ color: 'var(--accent-primary-light)' }}>
                Проверяем доступность...
              </span>
            </div>
          )}

          {status === 'available' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(16, 185, 129, 0.05) 100%)',
                border: '1px solid rgba(16, 185, 129, 0.3)'
              }}
            >
              <div className="flex items-center gap-3 mb-4">
                <div
                  className="w-10 h-10 rounded-xl flex items-center justify-center"
                  style={{ background: 'rgba(16, 185, 129, 0.2)' }}
                >
                  <CheckCircle2 className="w-5 h-5" style={{ color: 'var(--success-light)' }} />
                </div>
                <div>
                  <p className="font-semibold" style={{ color: 'var(--success-light)' }}>
                    Домен свободен
                  </p>
                  <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                    {domainInput}
                  </p>
                </div>
              </div>
              {price && (
                <div
                  className="flex items-center justify-between p-3 rounded-lg mb-4"
                  style={{ background: 'rgba(0, 0, 0, 0.2)' }}
                >
                  <span className="text-sm" style={{ color: 'var(--text-muted)' }}>Стоимость регистрации:</span>
                  <span className="font-bold text-lg" style={{ color: 'var(--text-primary)' }}>
                    {price.amount} {price.currency === 'RUB' ? '₽' : price.currency}/год
                  </span>
                </div>
              )}
              <button
                onClick={handleRegister}
                disabled={registerMutation.isPending}
                className="w-full btn btn-primary"
              >
                {registerMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <>
                    <Shield className="w-5 h-5 mr-2" />
                    Оформить и подключить
                  </>
                )}
              </button>
            </div>
          )}

          {status === 'taken' && (
            <div className="space-y-4">
              <div
                className="flex items-center gap-3 p-4 rounded-xl"
                style={{
                  background: 'var(--error-bg)',
                  border: '1px solid var(--error-border)'
                }}
              >
                <XCircle className="w-5 h-5" style={{ color: 'var(--error-light)' }} />
                <span className="text-sm font-medium" style={{ color: 'var(--error-light)' }}>
                  Домен <strong>{domainInput}</strong> уже занят
                </span>
              </div>

              {alternatives.length > 0 && (
                <div>
                  <p className="text-sm font-medium mb-3" style={{ color: 'var(--text-primary)' }}>
                    Доступные варианты:
                  </p>
                  <div className="space-y-2 max-h-[200px] overflow-y-auto">
                    {alternatives.map((alt, i) => (
                      <button
                        key={i}
                        onClick={() => handleSelectAlternative(alt)}
                        className="w-full flex items-center justify-between p-3 rounded-xl transition-all hover:scale-[1.01] active:scale-[0.99] text-left"
                        style={{
                          background: 'var(--bg-surface)',
                          border: '1px solid var(--border-default)'
                        }}
                      >
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="w-4 h-4" style={{ color: 'var(--success-light)' }} />
                          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>{alt.domain}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          {alt.price && (
                            <span className="text-sm" style={{ color: 'var(--text-muted)' }}>
                              {alt.price.amount} ₽
                            </span>
                          )}
                          <ChevronRight className="w-4 h-4" style={{ color: 'var(--text-subtle)' }} />
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {status === 'invalid' && (
            <div
              className="flex items-center gap-3 p-4 rounded-xl"
              style={{
                background: 'var(--warning-bg)',
                border: '1px solid var(--warning-border)'
              }}
            >
              <AlertCircle className="w-5 h-5" style={{ color: 'var(--warning-light)' }} />
              <span className="text-sm font-medium" style={{ color: 'var(--warning-light)' }}>
                {errorMessage}
              </span>
            </div>
          )}

          {status === 'registering' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'rgba(59, 130, 246, 0.1)',
                border: '1px solid rgba(59, 130, 246, 0.2)'
              }}
            >
              <div className="flex items-center gap-3 mb-3">
                <Loader2 className="w-5 h-5 animate-spin" style={{ color: 'var(--accent-primary-light)' }} />
                <span className="font-medium" style={{ color: 'var(--accent-primary-light)' }}>
                  Создаём заявку в REG.RU...
                </span>
              </div>
              <div className="w-full h-2 rounded-full" style={{ background: 'var(--bg-tertiary)' }}>
                <div className="h-2 rounded-full w-1/3 animate-pulse" style={{ background: 'var(--accent-primary)' }} />
              </div>
            </div>
          )}

          {status === 'configuring' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'rgba(139, 92, 246, 0.1)',
                border: '1px solid rgba(139, 92, 246, 0.2)'
              }}
            >
              <div className="flex items-center gap-3 mb-3">
                <Loader2 className="w-5 h-5 animate-spin" style={{ color: 'var(--info-light)' }} />
                <span className="font-medium" style={{ color: 'var(--info-light)' }}>
                  Настраиваем DNS...
                </span>
              </div>
              <div className="w-full h-2 rounded-full" style={{ background: 'var(--bg-tertiary)' }}>
                <div className="h-2 rounded-full w-2/3 animate-pulse" style={{ background: 'var(--info)' }} />
              </div>
            </div>
          )}

          {status === 'connected' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'var(--success-bg)',
                border: '1px solid var(--success-border)'
              }}
            >
              <div className="flex items-center gap-3 mb-3">
                <div
                  className="w-10 h-10 rounded-xl flex items-center justify-center"
                  style={{ background: 'rgba(16, 185, 129, 0.2)' }}
                >
                  <CheckCircle2 className="w-5 h-5" style={{ color: 'var(--success-light)' }} />
                </div>
                <div>
                  <p className="font-semibold" style={{ color: 'var(--success-light)' }}>
                    Домен подключён!
                  </p>
                  <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                    {registeredDomain}
                  </p>
                </div>
              </div>
              <p className="text-xs mb-4" style={{ color: 'var(--text-subtle)' }}>
                DNS записи настроены. Полная активация может занять до 24 часов.
              </p>
              <button
                onClick={onClose}
                className="w-full btn btn-primary"
              >
                Готово
              </button>
            </div>
          )}

          {status === 'bill_created' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'var(--warning-bg)',
                border: '1px solid var(--warning-border)'
              }}
            >
              <div className="flex items-center gap-3 mb-3">
                <div
                  className="w-10 h-10 rounded-xl flex items-center justify-center"
                  style={{ background: 'rgba(245, 158, 11, 0.2)' }}
                >
                  <CreditCard className="w-5 h-5" style={{ color: 'var(--warning-light)' }} />
                </div>
                <div>
                  <p className="font-semibold" style={{ color: 'var(--warning-light)' }}>
                    Создан счёт на оплату
                  </p>
                  {billId && (
                    <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                      Счёт №{billId}
                    </p>
                  )}
                </div>
              </div>
              <p className="text-sm mb-4" style={{ color: 'var(--text-muted)' }}>
                {errorMessage || 'Недостаточно средств на балансе REG.RU. Оплатите счёт в личном кабинете REG.RU.'}
              </p>
              <button
                onClick={handleRefreshStatus}
                disabled={checkMutation.isPending}
                className="btn btn-secondary w-full"
              >
                {checkMutation.isPending ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2" />
                    Обновить статус
                  </>
                )}
              </button>
            </div>
          )}

          {status === 'error' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'var(--error-bg)',
                border: '1px solid var(--error-border)'
              }}
            >
              <div className="flex items-center gap-3 mb-3">
                <AlertCircle className="w-5 h-5" style={{ color: 'var(--error-light)' }} />
                <span className="font-semibold" style={{ color: 'var(--error-light)' }}>
                  Произошла ошибка
                </span>
              </div>
              <p className="text-sm mb-4" style={{ color: 'var(--text-muted)' }}>
                {errorMessage || 'Попробуйте ещё раз или обратитесь в поддержку.'}
              </p>
              <button
                onClick={handleCheck}
                className="btn btn-secondary w-full"
              >
                <RefreshCw className="w-4 h-4 mr-2" />
                Повторить
              </button>
            </div>
          )}

          {/* Info Block */}
          {status === 'idle' && (
            <div
              className="p-4 rounded-xl"
              style={{
                background: 'rgba(59, 130, 246, 0.08)',
                border: '1px solid rgba(59, 130, 246, 0.15)'
              }}
            >
              <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                <strong style={{ color: 'var(--accent-primary-light)' }}>Как это работает:</strong>
                <br /><span style={{ color: 'var(--text-subtle)' }}>1.</span> Введите желаемый домен и нажмите «Проверить»
                <br /><span style={{ color: 'var(--text-subtle)' }}>2.</span> Если домен свободен — оформите регистрацию
                <br /><span style={{ color: 'var(--text-subtle)' }}>3.</span> Если занят — выберите из предложенных вариантов
                <br /><span style={{ color: 'var(--text-subtle)' }}>4.</span> Домен будет автоматически подключён к вашему сайту
              </p>
            </div>
          )}
        </div>
      </motion.div>
    </AnimatePresence>
  )
}

