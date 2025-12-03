import { useState, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Phone, Mail, MapPin, Briefcase,
  Image, Archive, Send, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronDown, X, ExternalLink,
  Palette, Upload, Camera
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const statusConfig: Record<string, { icon: React.ReactNode; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, label: 'Черновик' },
  awaiting_photos: { icon: <Image className="w-4 h-4" />, label: 'Ожидание фото' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, label: 'Сбор данных' },
  collecting_photos: { icon: <Image className="w-4 h-4" />, label: 'Сбор фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готов' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, label: 'В работе' },
  in_queue: { icon: <Clock className="w-4 h-4" />, label: 'В очереди' },
  generated_ok: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готово!' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готово!' },
  generated_error: { icon: <AlertCircle className="w-4 h-4" />, label: 'Ошибка' },
  error: { icon: <AlertCircle className="w-4 h-4" />, label: 'Ошибка' },
  archived: { icon: <Archive className="w-4 h-4" />, label: 'Архив' },
}

const photoCategories = [
  { id: 'hero', label: 'Баннер' },
  { id: 'services', label: 'Услуги' },
  { id: 'portfolio', label: 'Портфолио' },
  { id: 'team', label: 'Команда' },
  { id: 'gallery', label: 'Галерея' },
]

export default function RequestDetailPage() {
  const { id } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [showStatusMenu, setShowStatusMenu] = useState(false)
  const [showPhotoUpload, setShowPhotoUpload] = useState(false)
  const [selectedCategory, setSelectedCategory] = useState('gallery')
  const [uploadingPhoto, setUploadingPhoto] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const { data: request, isLoading, error } = useQuery({
    queryKey: ['request', id],
    queryFn: () => requestsApi.get(id!).then(res => res.data),
    enabled: !!id,
  })

  const generateMutation = useMutation({
    mutationFn: () => requestsApi.generate(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Отправлено в разработку')
      haptic?.notificationOccurred('success')
    },
    onError: () => {
      toast.error('Ошибка')
      haptic?.notificationOccurred('error')
    },
  })

  const archiveMutation = useMutation({
    mutationFn: () => requestsApi.archive(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('В архиве')
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

  const handlePhotoUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !id) return

    setUploadingPhoto(true)
    try {
      const formData = new FormData()
      formData.append('file', file)
      formData.append('category', selectedCategory)
      await requestsApi.uploadPhotos(id, formData)
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Фото загружено')
      haptic?.notificationOccurred('success')
      setShowPhotoUpload(false)
    } catch {
      toast.error('Ошибка загрузки')
      haptic?.notificationOccurred('error')
    } finally {
      setUploadingPhoto(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  const handleDeletePhoto = async (photoUrl: string) => {
    if (!id) return
    try {
      await requestsApi.deletePhoto(id, photoUrl)
      queryClient.invalidateQueries({ queryKey: ['request', id] })
      toast.success('Фото удалено')
    } catch {
      toast.error('Ошибка')
    }
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        <div className="skeleton h-32 rounded-2xl" />
        <div className="skeleton h-48 rounded-2xl" />
      </div>
    )
  }

  if (error || !request) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen p-4">
        <AlertCircle className="w-12 h-12 text-red-500 mb-4" />
        <p className="text-tg-text font-medium mb-4">Заявка не найдена</p>
        <button onClick={() => navigate('/requests')} className="btn btn-primary">
          К списку
        </button>
      </div>
    )
  }

  const payload = request.payload || {}
  const site = payload.site || {}
  const client = payload.client || {}

  const status = site.meta?.status || request.status || 'draft'
  const config = statusConfig[status] || statusConfig.draft

  const companyName = site.company || request.company_name || 'Без названия'
  const clientName = client.name || request.client_name || ''
  const phone = site.phone || ''
  const email = site.email || ''
  const address = site.address || ''
  const businessType = site.business_type || ''
  const summary = site.summary || ''
  const services = (site.services || []).map((s: any) => typeof s === 'string' ? { name: s } : s)
  const images = site.assets?.images || []
  const structure = site.structure || []
  const colorPalette = site.color_palette || ''
  const resultUrl = request.result_url || site.result_url

  const handleGenerate = () => {
    if (webApp?.showConfirm) {
      webApp.showConfirm('Отправить в разработку?', (confirmed) => {
        if (confirmed) generateMutation.mutate()
      })
    } else {
      generateMutation.mutate()
    }
  }

  const handleArchive = () => {
    if (webApp?.showConfirm) {
      webApp.showConfirm('В архив?', (confirmed) => {
        if (confirmed) archiveMutation.mutate()
      })
    } else {
      archiveMutation.mutate()
    }
  }

  return (
    <div className="min-h-screen pb-32 bg-tg-bg">
      {/* Header */}
      <div className="m-4 bg-black dark:bg-white text-white dark:text-black rounded-2xl p-6">
        <div className="flex items-start justify-between mb-4">
          <div className="w-14 h-14 rounded-2xl bg-white/20 dark:bg-black/20 flex items-center justify-center text-2xl font-bold">
            {companyName[0]?.toUpperCase() || '?'}
          </div>
          <button
            onClick={() => setShowStatusMenu(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium bg-white/20 dark:bg-black/20"
          >
            {config.icon}
            {config.label}
            <ChevronDown className="w-4 h-4" />
          </button>
        </div>
        <h1 className="text-2xl font-bold mb-1">{companyName}</h1>
        {businessType && <p className="opacity-70 text-sm">{businessType}</p>}
        {resultUrl && (
          <a
            href={resultUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 mt-4 px-4 py-2 bg-white/20 dark:bg-black/20 rounded-xl text-sm"
          >
            <ExternalLink className="w-4 h-4" /> Открыть сайт
          </a>
        )}
      </div>

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
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <p className="text-lg font-semibold mb-4">Статус</p>
              <div className="space-y-2">
                {Object.entries(statusConfig).map(([key, cfg]) => (
                  <button
                    key={key}
                    onClick={() => statusMutation.mutate(key)}
                    className={clsx(
                      'w-full flex items-center gap-3 p-3 rounded-xl',
                      status === key ? 'bg-tg-secondary-bg' : ''
                    )}
                  >
                    {cfg.icon}
                    <span>{cfg.label}</span>
                    {status === key && <CheckCircle2 className="w-5 h-5 ml-auto" />}
                  </button>
                ))}
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Photo Upload Modal */}
      <AnimatePresence>
        {showPhotoUpload && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => !uploadingPhoto && setShowPhotoUpload(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <p className="text-lg font-semibold mb-4">Загрузить фото</p>
              <div className="space-y-2 mb-4">
                {photoCategories.map((cat) => (
                  <button
                    key={cat.id}
                    onClick={() => setSelectedCategory(cat.id)}
                    className={clsx(
                      'w-full p-3 rounded-xl text-left',
                      selectedCategory === cat.id ? 'bg-tg-secondary-bg border-2 border-black dark:border-white' : 'bg-tg-secondary-bg'
                    )}
                  >
                    {cat.label}
                  </button>
                ))}
              </div>
              <input type="file" ref={fileInputRef} onChange={handlePhotoUpload} accept="image/*" className="hidden" />
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={uploadingPhoto}
                className="btn btn-primary w-full"
              >
                {uploadingPhoto ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Выбрать фото'}
              </button>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Content */}
      <div className="px-4 space-y-4">
        {clientName && (
          <Section title="Клиент">
            <InfoItem icon={<User className="w-5 h-5" />} label="Имя" value={clientName} />
            {client.company && <InfoItem icon={<Building2 className="w-5 h-5" />} label="Компания" value={client.company} />}
            {client.contact && <InfoItem icon={<Phone className="w-5 h-5" />} label="Контакт" value={client.contact} />}
          </Section>
        )}

        {(phone || email || address) && (
          <Section title="Контакты сайта">
            {phone && <InfoItem icon={<Phone className="w-5 h-5" />} label="Телефон" value={phone} />}
            {email && <InfoItem icon={<Mail className="w-5 h-5" />} label="Email" value={email} />}
            {address && <InfoItem icon={<MapPin className="w-5 h-5" />} label="Адрес" value={address} />}
          </Section>
        )}

        {summary && (
          <Section title="О компании">
            <div className="p-4">
              <p className="text-tg-text whitespace-pre-wrap">{summary}</p>
            </div>
          </Section>
        )}

        {services.length > 0 && (
          <Section title="Услуги">
            <div className="divide-y divide-tg-separator">
              {services.map((service: any, i: number) => (
                <div key={i} className="p-4">
                  <p className="font-medium">{service.name}</p>
                  {service.summary && <p className="text-sm text-tg-hint mt-1">{service.summary}</p>}
                  {service.priceFrom && <p className="text-sm text-emerald-600 mt-1">{service.priceFrom}</p>}
                </div>
              ))}
            </div>
          </Section>
        )}

        {structure.length > 0 && (
          <Section title="Структура">
            <div className="p-4 flex flex-wrap gap-2">
              {structure.map((s: string, i: number) => (
                <span key={i} className="px-3 py-1.5 bg-tg-secondary-bg rounded-full text-sm">{s}</span>
              ))}
            </div>
          </Section>
        )}

        {colorPalette && (
          <Section title="Палитра">
            <InfoItem icon={<Palette className="w-5 h-5" />} label="Цвета" value={colorPalette} />
          </Section>
        )}

        <Section title="Фото">
          <div className="p-4 space-y-4">
            <button
              onClick={() => setShowPhotoUpload(true)}
              className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint"
            >
              <Camera className="w-5 h-5 mx-auto mb-2" />
              Добавить фото
            </button>

            {images.length > 0 ? (
              <div className="flex gap-2 overflow-x-auto">
                {images.map((img: any, i: number) => (
                  <div key={i} className="relative flex-shrink-0 group">
                    <img src={img.url} alt="" className="w-20 h-20 rounded-xl object-cover" />
                    <button
                      onClick={() => handleDeletePhoto(img.url)}
                      className="absolute -top-2 -right-2 w-6 h-6 bg-red-500 text-white rounded-full flex items-center justify-center opacity-0 group-hover:opacity-100"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-center text-tg-hint py-4">Нет фото</p>
            )}
          </div>
        </Section>
      </div>

      {/* Bottom Actions */}
      <div className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        <div className="flex gap-3">
          {['draft', 'awaiting_photos', 'collecting_info', 'collecting_photos', 'ready_to_generate'].includes(status) && (
            <button onClick={handleGenerate} disabled={generateMutation.isPending} className="btn btn-primary flex-1">
              {generateMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Send className="w-5 h-5" /> В разработку</>}
            </button>
          )}
          {resultUrl && (
            <a href={resultUrl} target="_blank" rel="noopener noreferrer" className="btn btn-primary flex-1">
              <ExternalLink className="w-5 h-5" /> Открыть
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
    <div>
      <p className="section-header">{title}</p>
      <div className="bg-tg-section rounded-2xl overflow-hidden border border-tg-separator">
        {children}
      </div>
    </div>
  )
}

function InfoItem({ icon, label, value }: { icon: React.ReactNode; label: string; value?: string }) {
  return (
    <div className="list-item">
      <div className="text-tg-hint">{icon}</div>
      <div className="flex-1">
        <p className="text-xs text-tg-hint">{label}</p>
        <p className="text-tg-text">{value || '—'}</p>
      </div>
    </div>
  )
}
