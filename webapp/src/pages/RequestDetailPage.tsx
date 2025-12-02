import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Phone, Mail, MapPin, Globe, Briefcase,
  FileText, Image, Edit2, Trash2, Archive, Play, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronDown, Plus, X
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const statusConfig: Record<string, { icon: React.ReactNode; color: string; bgColor: string; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, color: 'text-gray-600', bgColor: 'bg-gray-100', label: 'Черновик' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, color: 'text-amber-600', bgColor: 'bg-amber-100', label: 'Сбор данных' },
  collecting_photos: { icon: <Image className="w-4 h-4" />, color: 'text-amber-600', bgColor: 'bg-amber-100', label: 'Сбор фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-green-600', bgColor: 'bg-green-100', label: 'Готов к генерации' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, color: 'text-blue-600', bgColor: 'bg-blue-100', label: 'Генерация...' },
  in_queue: { icon: <Clock className="w-4 h-4" />, color: 'text-blue-600', bgColor: 'bg-blue-100', label: 'В очереди' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-emerald-600', bgColor: 'bg-emerald-100', label: 'Сайт готов!' },
  error: { icon: <AlertCircle className="w-4 h-4" />, color: 'text-red-600', bgColor: 'bg-red-100', label: 'Ошибка' },
  archived: { icon: <Archive className="w-4 h-4" />, color: 'text-purple-600', bgColor: 'bg-purple-100', label: 'В архиве' },
}

export default function RequestDetailPage() {
  const { id } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [showStatusMenu, setShowStatusMenu] = useState(false)
  const [editField, setEditField] = useState<string | null>(null)
  const [editValue, setEditValue] = useState('')

  const { data: request, isLoading } = useQuery({
    queryKey: ['request', id],
    queryFn: () => requestsApi.get(id!).then(res => res.data),
    enabled: !!id,
  })

  const updateMutation = useMutation({
    mutationFn: (data: any) => requestsApi.update(id!, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Сохранено')
      setEditField(null)
    },
    onError: () => toast.error('Ошибка сохранения'),
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

  const deleteMutation = useMutation({
    mutationFn: () => requestsApi.delete(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('Заявка удалена')
      navigate('/requests')
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

  if (!request) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <p className="text-tg-hint">Заявка не найдена</p>
      </div>
    )
  }

  const status = request.payload?.site?.meta?.status || request.status || 'draft'
  const config = statusConfig[status] || statusConfig.draft
  const payload = request.payload || {}
  const site = payload.site || {}
  const photos = site.photos || {}

  const handleEdit = (field: string, value: string) => {
    setEditField(field)
    setEditValue(value || '')
  }

  const handleSave = () => {
    if (!editField) return

    const fieldPath = editField.split('.')
    const data: any = {}

    if (fieldPath[0] === 'site') {
      data.payload = { ...payload }
      if (fieldPath[1] === 'contacts') {
        data.payload.site = { ...site, contacts: { ...site.contacts, [fieldPath[2]]: editValue } }
      } else {
        data.payload.site = { ...site, [fieldPath[1]]: editValue }
      }
    } else {
      data[fieldPath[0]] = editValue
    }

    updateMutation.mutate(data)
  }

  const handleGenerate = () => {
    webApp?.showConfirm('Запустить генерацию сайта?', (confirmed) => {
      if (confirmed) {
        generateMutation.mutate()
      }
    })
  }

  const handleArchive = () => {
    webApp?.showConfirm('Отправить заявку в архив?', (confirmed) => {
      if (confirmed) {
        archiveMutation.mutate()
      }
    })
  }

  const handleDelete = () => {
    webApp?.showConfirm('Удалить заявку? Это действие нельзя отменить.', (confirmed) => {
      if (confirmed) {
        deleteMutation.mutate()
      }
    })
  }

  return (
    <div className="min-h-screen pb-32">
      {/* Header Card */}
      <motion.div
        className="m-4 bg-gradient-to-br from-brand-500 to-brand-600 rounded-3xl p-6 text-white shadow-lg"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <div className="flex items-start justify-between mb-4">
          <div className="w-14 h-14 rounded-2xl bg-white/20 backdrop-blur flex items-center justify-center text-2xl font-bold">
            {request.company_name?.[0]?.toUpperCase() || '?'}
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
        <h1 className="text-2xl font-bold mb-1">{request.company_name || 'Без названия'}</h1>
        <p className="text-white/80">{request.client_name || 'Без клиента'}</p>
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
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom"
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
        {/* Info Section */}
        <Section title="Информация">
          <InfoItem
            icon={<Building2 className="w-5 h-5 text-brand-500" />}
            label="Компания"
            value={request.company_name}
            onEdit={() => handleEdit('company_name', request.company_name)}
          />
          <InfoItem
            icon={<User className="w-5 h-5 text-brand-500" />}
            label="Клиент"
            value={request.client_name}
            onEdit={() => handleEdit('client_name', request.client_name)}
          />
          <InfoItem
            icon={<Globe className="w-5 h-5 text-brand-500" />}
            label="Сфера"
            value={site.sphere}
            onEdit={() => handleEdit('site.sphere', site.sphere)}
          />
        </Section>

        {/* Contacts Section */}
        <Section title="Контакты">
          <InfoItem
            icon={<Phone className="w-5 h-5 text-green-500" />}
            label="Телефон"
            value={site.contacts?.phone}
            onEdit={() => handleEdit('site.contacts.phone', site.contacts?.phone)}
          />
          <InfoItem
            icon={<Mail className="w-5 h-5 text-blue-500" />}
            label="Email"
            value={site.contacts?.email}
            onEdit={() => handleEdit('site.contacts.email', site.contacts?.email)}
          />
          <InfoItem
            icon={<MapPin className="w-5 h-5 text-red-500" />}
            label="Адрес"
            value={site.contacts?.address}
            onEdit={() => handleEdit('site.contacts.address', site.contacts?.address)}
          />
        </Section>

        {/* Services Section */}
        <Section title="Услуги">
          <div className="p-4 space-y-2">
            {site.services?.length ? (
              site.services.map((service: string, i: number) => (
                <div key={i} className="flex items-center gap-2 text-tg-text">
                  <Briefcase className="w-4 h-4 text-tg-hint" />
                  {service}
                </div>
              ))
            ) : (
              <p className="text-tg-hint">Услуги не указаны</p>
            )}
          </div>
        </Section>

        {/* Photos Section */}
        <Section title="Фотографии">
          <div className="p-4 space-y-3">
            {Object.entries(photos).map(([category, urls]) => (
              <div key={category}>
                <p className="text-sm text-tg-hint mb-2 capitalize">{category}</p>
                <div className="flex gap-2 overflow-x-auto">
                  {(urls as string[]).map((url, i) => (
                    <img
                      key={i}
                      src={url}
                      alt={category}
                      className="w-20 h-20 rounded-xl object-cover flex-shrink-0"
                    />
                  ))}
                </div>
              </div>
            ))}
            {Object.keys(photos).length === 0 && (
              <p className="text-tg-hint">Фото не загружены</p>
            )}
            <button className="btn btn-secondary w-full mt-2">
              <Plus className="w-5 h-5" />
              Добавить фото
            </button>
          </div>
        </Section>

        {/* Description */}
        {site.about && (
          <Section title="Описание">
            <div className="p-4">
              <p className="text-tg-text whitespace-pre-wrap">{site.about}</p>
            </div>
          </Section>
        )}
      </div>

      {/* Edit Modal */}
      <AnimatePresence>
        {editField && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setEditField(null)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 25 }}
            >
              <div className="flex items-center justify-between mb-4">
                <p className="text-lg font-semibold text-tg-text">Редактирование</p>
                <button onClick={() => setEditField(null)} className="p-2 -mr-2">
                  <X className="w-5 h-5 text-tg-hint" />
                </button>
              </div>
              <textarea
                value={editValue}
                onChange={(e) => setEditValue(e.target.value)}
                className="input min-h-[100px] resize-none"
                autoFocus
              />
              <button
                onClick={handleSave}
                disabled={updateMutation.isPending}
                className="btn btn-primary w-full mt-4"
              >
                {updateMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  'Сохранить'
                )}
              </button>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Bottom Actions */}
      <div className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        <div className="flex gap-3">
          {['draft', 'collecting_info', 'collecting_photos', 'ready_to_generate'].includes(status) && (
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

          <button
            onClick={handleArchive}
            className="btn btn-secondary"
          >
            <Archive className="w-5 h-5" />
          </button>

          <button
            onClick={handleDelete}
            className="btn btn-destructive"
          >
            <Trash2 className="w-5 h-5" />
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
  onEdit,
}: {
  icon: React.ReactNode
  label: string
  value?: string
  onEdit: () => void
}) {
  return (
    <button onClick={onEdit} className="list-item w-full text-left">
      <div className="flex-shrink-0">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="text-xs text-tg-hint">{label}</p>
        <p className="text-tg-text truncate">{value || '—'}</p>
      </div>
      <Edit2 className="w-4 h-4 text-tg-hint" />
    </button>
  )
}
