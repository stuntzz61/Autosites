import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Phone, Mail, MapPin, Globe, Briefcase,
  FileText, Image, Edit2, Trash2, Archive, Play, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronDown, Plus, X, ExternalLink,
  Calendar, Palette, Layout
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const statusConfig: Record<string, { icon: React.ReactNode; color: string; bgColor: string; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, color: 'text-gray-600 dark:text-gray-400', bgColor: 'bg-gray-100 dark:bg-gray-800', label: 'Черновик' },
  awaiting_photos: { icon: <Image className="w-4 h-4" />, color: 'text-amber-600 dark:text-amber-400', bgColor: 'bg-amber-100 dark:bg-amber-900/30', label: 'Ожидание фото' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, color: 'text-amber-600 dark:text-amber-400', bgColor: 'bg-amber-100 dark:bg-amber-900/30', label: 'Сбор данных' },
  collecting_photos: { icon: <Image className="w-4 h-4" />, color: 'text-amber-600 dark:text-amber-400', bgColor: 'bg-amber-100 dark:bg-amber-900/30', label: 'Сбор фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-green-600 dark:text-green-400', bgColor: 'bg-green-100 dark:bg-green-900/30', label: 'Готов к генерации' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, color: 'text-blue-600 dark:text-blue-400', bgColor: 'bg-blue-100 dark:bg-blue-900/30', label: 'Генерация...' },
  in_queue: { icon: <Clock className="w-4 h-4" />, color: 'text-blue-600 dark:text-blue-400', bgColor: 'bg-blue-100 dark:bg-blue-900/30', label: 'В очереди' },
  generated_ok: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-emerald-600 dark:text-emerald-400', bgColor: 'bg-emerald-100 dark:bg-emerald-900/30', label: 'Сайт готов!' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-emerald-600 dark:text-emerald-400', bgColor: 'bg-emerald-100 dark:bg-emerald-900/30', label: 'Сайт готов!' },
  generated_error: { icon: <AlertCircle className="w-4 h-4" />, color: 'text-red-600 dark:text-red-400', bgColor: 'bg-red-100 dark:bg-red-900/30', label: 'Ошибка' },
  error: { icon: <AlertCircle className="w-4 h-4" />, color: 'text-red-600 dark:text-red-400', bgColor: 'bg-red-100 dark:bg-red-900/30', label: 'Ошибка' },
  archived: { icon: <Archive className="w-4 h-4" />, color: 'text-purple-600 dark:text-purple-400', bgColor: 'bg-purple-100 dark:bg-purple-900/30', label: 'В архиве' },
}

interface ServiceItem {
  name: string
  summary?: string
  priceFrom?: string
}

interface ImageItem {
  url: string
  alt?: string
  category?: string
}

export default function RequestDetailPage() {
  const { id } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [showStatusMenu, setShowStatusMenu] = useState(false)

  const { data: request, isLoading, error } = useQuery({
    queryKey: ['request', id],
    queryFn: () => requestsApi.get(id!).then(res => res.data),
    enabled: !!id,
  })

  const generateMutation = useMutation({
    mutationFn: () => requestsApi.generate(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Заявка отправлена на генерацию')
      haptic?.notificationOccurred('success')
    },
    onError: () => {
      toast.error('Ошибка генерации')
      haptic?.notificationOccurred('error')
    },
  })

  const archiveMutation = useMutation({
    mutationFn: () => requestsApi.archive(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('Заявка в архиве')
      navigate('/requests')
    },
    onError: () => toast.error('Ошибка'),
  })

  const statusMutation = useMutation({
    mutationFn: (status: string) => requestsApi.updateStatus(id!, status),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Статус обновлён')
      setShowStatusMenu(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        <div className="skeleton h-32 rounded-2xl" />
        <div className="skeleton h-48 rounded-2xl" />
        <div className="skeleton h-32 rounded-2xl" />
      </div>
    )
  }

  if (error || !request) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen p-4">
        <AlertCircle className="w-12 h-12 text-red-500 mb-4" />
        <p className="text-tg-text font-medium mb-2">Заявка не найдена</p>
        <p className="text-tg-hint text-sm mb-4">Возможно, она была удалена</p>
        <button onClick={() => navigate('/requests')} className="btn btn-primary">
          К списку заявок
        </button>
      </div>
    )
  }

  // Parse data from payload - handle both new and old structures
  const payload = request.payload || {}
  const site = payload.site || {}
  const client = payload.client || {}

  const status = site.meta?.status || request.status || 'draft'
  const config = statusConfig[status] || statusConfig.draft

  // Company name - try multiple sources
  const companyName = site.company || request.company_name || 'Без названия'
  const clientName = client.name || request.client_name || ''
  const clientCompany = client.company || ''
  const clientContact = client.contact || ''

  // Contact info - can be at site level or in contacts object
  const phone = site.phone || site.contacts?.phone || ''
  const email = site.email || site.contacts?.email || ''
  const address = site.address || site.contacts?.address || ''

  // Business info
  const businessType = site.business_type || site.sphere || ''
  const summary = site.summary || site.about || ''
  const workHours = site.work_hours || ''
  const colorPalette = site.color_palette || ''

  // Services - can be array of strings or array of objects
  const services: ServiceItem[] = (site.services || []).map((s: string | ServiceItem) =>
    typeof s === 'string' ? { name: s } : s
  )

  // Images from assets
  const images: ImageItem[] = site.assets?.images || []
  const imagesByCategory = images.reduce((acc: Record<string, ImageItem[]>, img) => {
    const cat = img.category || 'other'
    if (!acc[cat]) acc[cat] = []
    acc[cat].push(img)
    return acc
  }, {})

  // Structure/sections
  const structure: string[] = site.structure || []

  // Result URL if generated
  const resultUrl = request.result_url || site.result_url

  const handleGenerate = () => {
    if (webApp?.showConfirm) {
      webApp.showConfirm('Запустить генерацию сайта?', (confirmed) => {
        if (confirmed) generateMutation.mutate()
      })
    } else {
      generateMutation.mutate()
    }
  }

  const handleArchive = () => {
    if (webApp?.showConfirm) {
      webApp.showConfirm('Отправить заявку в архив?', (confirmed) => {
        if (confirmed) archiveMutation.mutate()
      })
    } else {
      archiveMutation.mutate()
    }
  }

  const categoryLabels: Record<string, string> = {
    hero: '🏠 Главный баннер',
    services: '🛠 Услуги',
    portfolio: '📁 Портфолио',
    team: '👥 Команда',
    gallery: '🖼 Галерея',
    other: '📷 Прочее',
  }

  return (
    <div className="min-h-screen pb-32">
      {/* Header Card */}
      <motion.div
        className="m-4 bg-gradient-to-br from-blue-500 to-blue-600 rounded-3xl p-6 text-white shadow-lg"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <div className="flex items-start justify-between mb-4">
          <div className="w-14 h-14 rounded-2xl bg-white/20 backdrop-blur flex items-center justify-center text-2xl font-bold">
            {companyName[0]?.toUpperCase() || '?'}
          </div>
          <button
            onClick={() => setShowStatusMenu(true)}
            className={clsx('flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium', config.bgColor, config.color)}
          >
            {config.icon}
            {config.label}
            <ChevronDown className="w-4 h-4" />
          </button>
        </div>
        <h1 className="text-2xl font-bold mb-1">{companyName}</h1>
        {businessType && <p className="text-white/80 text-sm">{businessType}</p>}

        {resultUrl && (
          <a
            href={resultUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 mt-4 px-4 py-2 bg-white/20 rounded-xl text-sm font-medium hover:bg-white/30 transition-colors"
          >
            <ExternalLink className="w-4 h-4" />
            Открыть сайт
          </a>
        )}
      </motion.div>

      {/* Status Menu */}
      <AnimatePresence>
        {showStatusMenu && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowStatusMenu(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[70vh] overflow-y-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 25 }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <p className="text-lg font-semibold text-tg-text mb-4">Изменить статус</p>
              <div className="space-y-2">
                {Object.entries(statusConfig).map(([key, cfg]) => (
                  <button
                    key={key}
                    onClick={() => {
                      haptic?.selectionChanged()
                      statusMutation.mutate(key)
                    }}
                    className={clsx(
                      'w-full flex items-center gap-3 p-3 rounded-xl transition-colors',
                      status === key ? 'bg-tg-button/10' : 'hover:bg-tg-secondary-bg'
                    )}
                  >
                    <div className={clsx('p-2 rounded-lg', cfg.bgColor, cfg.color)}>
                      {cfg.icon}
                    </div>
                    <span className="font-medium text-tg-text">{cfg.label}</span>
                    {status === key && (
                      <CheckCircle2 className="w-5 h-5 text-tg-button ml-auto" />
                    )}
                  </button>
                ))}
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Content */}
      <div className="px-4 space-y-4">
        {/* Client Info */}
        {(clientName || clientCompany || clientContact) && (
          <Section title="Клиент">
            {clientName && (
              <InfoItem icon={<User className="w-5 h-5 text-blue-500" />} label="Имя" value={clientName} />
            )}
            {clientCompany && (
              <InfoItem icon={<Building2 className="w-5 h-5 text-blue-500" />} label="Компания" value={clientCompany} />
            )}
            {clientContact && (
              <InfoItem icon={<Phone className="w-5 h-5 text-blue-500" />} label="Контакт" value={clientContact} />
            )}
          </Section>
        )}

        {/* Contacts */}
        {(phone || email || address) && (
          <Section title="Контакты сайта">
            {phone && <InfoItem icon={<Phone className="w-5 h-5 text-green-500" />} label="Телефон" value={phone} />}
            {email && <InfoItem icon={<Mail className="w-5 h-5 text-blue-500" />} label="Email" value={email} />}
            {address && <InfoItem icon={<MapPin className="w-5 h-5 text-red-500" />} label="Адрес" value={address} />}
            {workHours && <InfoItem icon={<Clock className="w-5 h-5 text-amber-500" />} label="Часы работы" value={workHours} />}
          </Section>
        )}

        {/* Summary */}
        {summary && (
          <Section title="О компании">
            <div className="p-4">
              <p className="text-tg-text whitespace-pre-wrap leading-relaxed">{summary}</p>
            </div>
          </Section>
        )}

        {/* Services */}
        {services.length > 0 && (
          <Section title="Услуги">
            <div className="divide-y divide-tg-separator">
              {services.map((service, i) => (
                <div key={i} className="p-4">
                  <div className="flex items-start gap-3">
                    <Briefcase className="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" />
                    <div className="flex-1">
                      <p className="font-medium text-tg-text">{service.name}</p>
                      {service.summary && (
                        <p className="text-sm text-tg-hint mt-1">{service.summary}</p>
                      )}
                      {service.priceFrom && (
                        <p className="text-sm text-green-600 dark:text-green-400 mt-1 font-medium">{service.priceFrom}</p>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </Section>
        )}

        {/* Structure */}
        {structure.length > 0 && (
          <Section title="Структура сайта">
            <div className="p-4 flex flex-wrap gap-2">
              {structure.map((section, i) => (
                <span key={i} className="px-3 py-1.5 bg-tg-secondary-bg rounded-full text-sm text-tg-text">
                  {section}
                </span>
              ))}
            </div>
          </Section>
        )}

        {/* Color Palette */}
        {colorPalette && (
          <Section title="Цветовая палитра">
            <InfoItem icon={<Palette className="w-5 h-5 text-purple-500" />} label="Палитра" value={colorPalette} />
          </Section>
        )}

        {/* Photos by Category */}
        {Object.keys(imagesByCategory).length > 0 && (
          <Section title="Фотографии">
            <div className="p-4 space-y-4">
              {Object.entries(imagesByCategory).map(([category, imgs]) => (
                <div key={category}>
                  <p className="text-sm font-medium text-tg-text mb-2">
                    {categoryLabels[category] || category}
                  </p>
                  <div className="flex gap-2 overflow-x-auto pb-2">
                    {imgs.map((img, i) => (
                      <a
                        key={i}
                        href={img.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex-shrink-0"
                      >
                        <img
                          src={img.url}
                          alt={img.alt || category}
                          className="w-20 h-20 rounded-xl object-cover hover:opacity-80 transition-opacity"
                        />
                      </a>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </Section>
        )}
      </div>

      {/* Bottom Actions */}
      <div className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        <div className="flex gap-3">
          {['draft', 'awaiting_photos', 'collecting_info', 'collecting_photos', 'ready_to_generate'].includes(status) && (
            <button
              onClick={handleGenerate}
              disabled={generateMutation.isPending}
              className="btn btn-primary flex-1"
            >
              {generateMutation.isPending ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <>
                  <Play className="w-5 h-5" />
                  Сгенерировать
                </>
              )}
            </button>
          )}

          {resultUrl && (
            <a
              href={resultUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-primary flex-1"
            >
              <ExternalLink className="w-5 h-5" />
              Открыть сайт
            </a>
          )}

          <button onClick={handleArchive} className="btn btn-secondary">
            <Archive className="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
    >
      <p className="section-header">{title}</p>
      <div className="bg-tg-section rounded-2xl overflow-hidden">
        {children}
      </div>
    </motion.div>
  )
}

function InfoItem({
  icon,
  label,
  value,
}: {
  icon: React.ReactNode
  label: string
  value?: string
}) {
  return (
    <div className="list-item">
      <div className="flex-shrink-0">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="text-xs text-tg-hint">{label}</p>
        <p className="text-tg-text">{value || '—'}</p>
      </div>
    </div>
  )
}
