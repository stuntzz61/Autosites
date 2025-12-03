import { useState, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Phone, Mail, MapPin, Briefcase,
  Image, Archive, Send, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronDown, X, ExternalLink,
  Palette, Upload, Camera, Edit3, Trash2
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { useAuthStore } from '@/stores/authStore'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const statusConfig: Record<string, { icon: React.ReactNode; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, label: 'Черновик' },
  awaiting_photos: { icon: <Image className="w-4 h-4" />, label: 'Ожидание фото' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, label: 'Сбор данных' },
  collecting_photos: { icon: <Image className="w-4 h-4" />, label: 'Сбор фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готов к отправке' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, label: 'Генерация...' },
  in_queue: { icon: <Clock className="w-4 h-4" />, label: 'В очереди' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Сайт готов!' },
  error: { icon: <AlertCircle className="w-4 h-4" />, label: 'Ошибка генерации' },
  archived: { icon: <Archive className="w-4 h-4" />, label: 'В архиве' },
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
  const { user } = useAuthStore()
  const [showStatusMenu, setShowStatusMenu] = useState(false)
  const [showPhotoUpload, setShowPhotoUpload] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [selectedCategory, setSelectedCategory] = useState('gallery')
  const [uploadingPhoto, setUploadingPhoto] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Check if user is admin
  const isAdmin = user?.role === 'admin'

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

  const deleteMutation = useMutation({
    mutationFn: () => requestsApi.delete(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('Заявка удалена')
      haptic?.notificationOccurred('success')
      navigate('/requests')
    },
    onError: () => {
      toast.error('Ошибка удаления')
      haptic?.notificationOccurred('error')
    },
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

// Normalize legacy statuses
  const rawStatus = site.meta?.status || request.status || 'draft'
  const statusMap: Record<string, string> = {
    'generated_ok': 'success',
    'generated_error': 'error',
    'ready': 'ready_to_generate',
  }
  const status = statusMap[rawStatus] || rawStatus
  const config = statusConfig[status] || statusConfig.draft

  // Debug log
  console.log('[RequestDetail] rawStatus:', rawStatus, 'normalized:', status, 'images:', site.assets?.images)

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

  const handleDelete = () => {
    if (webApp?.showConfirm) {
      webApp.showConfirm('Удалить заявку? Это действие нельзя отменить.', (confirmed) => {
        if (confirmed) deleteMutation.mutate()
      })
    } else {
      deleteMutation.mutate()
    }
  }

  const handleEdit = () => {
    haptic?.impactOccurred('light')
    setShowEditModal(true)
  }

  return (
    <div className="min-h-screen pb-48 bg-tg-bg">
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

        <Section title={`Фото (${images.length})`}>
          <div className="p-4 space-y-4">
            <button
              onClick={() => setShowPhotoUpload(true)}
              className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint"
            >
              <Camera className="w-5 h-5 mx-auto mb-2" />
              Добавить фото
            </button>

            {images.length > 0 ? (
              <div className="grid grid-cols-4 gap-2">
                {images.map((img: any, i: number) => (
                  <div key={i} className="relative aspect-square">
                    <img
                      src={img.url}
                      alt={img.category || 'Фото'}
                      className="w-full h-full rounded-xl object-cover bg-tg-secondary-bg"
                      onError={(e) => {
                        (e.target as HTMLImageElement).src = 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="%23666"><path d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"/></svg>'
                      }}
                    />
                    <button
                      onClick={() => handleDeletePhoto(img.url)}
                      className="absolute -top-1 -right-1 w-5 h-5 bg-red-500 text-white rounded-full flex items-center justify-center shadow-lg"
                    >
                      <X className="w-3 h-3" />
                    </button>
                    {img.category && (
                      <span className="absolute bottom-1 left-1 text-[10px] bg-black/50 text-white px-1 rounded">
                        {img.category}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-center text-tg-hint py-4">Нет фото</p>
            )}
          </div>
        </Section>
      </div>

      {/* Bottom Actions - positioned above bottom nav */}
      <div className="fixed bottom-16 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 z-20">
        <div className="flex gap-3">
          {/* Edit button - show for editable statuses */}
          {!['in_queue', 'generating', 'success', 'generated_ok', 'archived', 'closed'].includes(status) && (
            <button onClick={handleEdit} className="btn btn-secondary">
              <Edit3 className="w-5 h-5" />
            </button>
          )}

          {/* Generate button - show for statuses that can be sent to generation */}
          {!['in_queue', 'generating', 'success', 'generated_ok', 'archived', 'closed'].includes(status) && (
            <button onClick={handleGenerate} disabled={generateMutation.isPending} className="btn btn-primary flex-1">
              {generateMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Send className="w-5 h-5" /> В разработку</>}
            </button>
          )}

          {/* Open result button */}
          {resultUrl && (
            <a href={resultUrl} target="_blank" rel="noopener noreferrer" className="btn btn-primary flex-1">
              <ExternalLink className="w-5 h-5" /> Открыть
            </a>
          )}

          {/* Archive button */}
          <button onClick={handleArchive} className="btn btn-secondary" disabled={archiveMutation.isPending}>
            {archiveMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <Archive className="w-5 h-5" />}
          </button>

          {/* Delete button (available for all users on drafts/errors, or always for admins) */}
          {(isAdmin || ['draft', 'error', 'generated_error'].includes(status)) && (
            <button onClick={handleDelete} disabled={deleteMutation.isPending} className="btn btn-destructive">
              {deleteMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <Trash2 className="w-5 h-5" />}
            </button>
          )}
        </div>
      </div>

      {/* Edit Modal */}
      <AnimatePresence>
        {showEditModal && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowEditModal(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[80vh] overflow-y-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <p className="text-lg font-semibold mb-4">Редактирование</p>

              <EditRequestForm
                request={request}
                onSave={() => {
                  queryClient.invalidateQueries({ queryKey: ['request', id] })
                  setShowEditModal(false)
                  toast.success('Сохранено')
                }}
                onCancel={() => setShowEditModal(false)}
              />
            </motion.div>
          </>
        )}
      </AnimatePresence>
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

interface ServiceItem {
  name: string
  summary: string
  priceFrom: string
}

function EditRequestForm({
  request,
  onSave,
  onCancel
}: {
  request: any
  onSave: () => void
  onCancel: () => void
}) {
  const payload = request.payload || {}
  const site = payload.site || {}
  const client = payload.client || {}

  // Parse existing services
  const existingServices = (site.services || []).map((s: any) =>
    typeof s === 'string' ? { name: s, summary: '', priceFrom: '' } : s
  )

  const [activeTab, setActiveTab] = useState<'info' | 'services' | 'details'>('info')
  const [formData, setFormData] = useState({
    company: site.company || request.company_name || '',
    business_type: site.business_type || '',
    phone: site.phone || '',
    email: site.email || '',
    address: site.address || '',
    work_hours: site.work_hours || '',
    client_name: client.name || request.client_name || '',
    client_company: client.company || '',
    client_contact: client.contact || '',
    summary: site.summary || '',
    color_palette: site.color_palette || '',
    services: existingServices.length > 0 ? existingServices : [{ name: '', summary: '', priceFrom: '' }],
  })

  const [saving, setSaving] = useState(false)

  const addService = () => {
    setFormData(prev => ({
      ...prev,
      services: [...prev.services, { name: '', summary: '', priceFrom: '' }],
    }))
  }

  const updateService = (index: number, field: keyof ServiceItem, value: string) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.map((s, i) => i === index ? { ...s, [field]: value } : s),
    }))
  }

  const removeService = (index: number) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.filter((_, i) => i !== index),
    }))
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      const updatedPayload = {
        ...payload,
        client: {
          ...client,
          name: formData.client_name,
          company: formData.client_company,
          contact: formData.client_contact,
        },
        site: {
          ...site,
          company: formData.company,
          business_type: formData.business_type,
          phone: formData.phone,
          email: formData.email,
          address: formData.address,
          work_hours: formData.work_hours,
          summary: formData.summary,
          color_palette: formData.color_palette,
          services: formData.services.filter(s => s.name.trim()),
        }
      }

      await requestsApi.update(request.id, {
        company_name: formData.company,
        client_name: formData.client_name,
        payload: updatedPayload,
      })

      onSave()
    } catch (e) {
      toast.error('Ошибка сохранения')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-4">
      {/* Tabs */}
      <div className="flex gap-2 border-b border-tg-separator pb-3">
        <button
          onClick={() => setActiveTab('info')}
          className={`px-3 py-1.5 rounded-lg text-sm font-medium ${
            activeTab === 'info' ? 'bg-black dark:bg-white text-white dark:text-black' : 'text-tg-hint'
          }`}
        >
          Основное
        </button>
        <button
          onClick={() => setActiveTab('services')}
          className={`px-3 py-1.5 rounded-lg text-sm font-medium ${
            activeTab === 'services' ? 'bg-black dark:bg-white text-white dark:text-black' : 'text-tg-hint'
          }`}
        >
          Услуги ({formData.services.filter(s => s.name.trim()).length})
        </button>
        <button
          onClick={() => setActiveTab('details')}
          className={`px-3 py-1.5 rounded-lg text-sm font-medium ${
            activeTab === 'details' ? 'bg-black dark:bg-white text-white dark:text-black' : 'text-tg-hint'
          }`}
        >
          Детали
        </button>
      </div>

      {/* Info Tab */}
      {activeTab === 'info' && (
        <div className="space-y-4">
          <div>
            <label className="text-xs text-tg-hint mb-1 block">Название компании</label>
            <input
              type="text"
              value={formData.company}
              onChange={(e) => setFormData(prev => ({ ...prev, company: e.target.value }))}
              className="input"
              placeholder="Webly"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Сфера деятельности</label>
            <input
              type="text"
              value={formData.business_type}
              onChange={(e) => setFormData(prev => ({ ...prev, business_type: e.target.value }))}
              className="input"
              placeholder="Создание сайтов"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">ФИО клиента</label>
            <input
              type="text"
              value={formData.client_name}
              onChange={(e) => setFormData(prev => ({ ...prev, client_name: e.target.value }))}
              className="input"
              placeholder="Иванов Иван"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Компания клиента</label>
            <input
              type="text"
              value={formData.client_company}
              onChange={(e) => setFormData(prev => ({ ...prev, client_company: e.target.value }))}
              className="input"
              placeholder="ООО «Компания»"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Контакт клиента</label>
            <input
              type="text"
              value={formData.client_contact}
              onChange={(e) => setFormData(prev => ({ ...prev, client_contact: e.target.value }))}
              className="input"
              placeholder="+7... или @telegram"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Телефон для сайта</label>
            <input
              type="tel"
              value={formData.phone}
              onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
              className="input"
              placeholder="+7 (XXX) XXX-XX-XX"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Email</label>
            <input
              type="email"
              value={formData.email}
              onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
              className="input"
              placeholder="info@company.ru"
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Адрес</label>
            <input
              type="text"
              value={formData.address}
              onChange={(e) => setFormData(prev => ({ ...prev, address: e.target.value }))}
              className="input"
              placeholder="г. Москва, ул..."
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Часы работы</label>
            <input
              type="text"
              value={formData.work_hours}
              onChange={(e) => setFormData(prev => ({ ...prev, work_hours: e.target.value }))}
              className="input"
              placeholder="Пн-Пт 9:00-18:00"
            />
          </div>
        </div>
      )}

      {/* Services Tab */}
      {activeTab === 'services' && (
        <div className="space-y-4">
          {formData.services.map((service, i) => (
            <div key={i} className="bg-tg-secondary-bg rounded-xl p-4">
              <div className="flex justify-between items-center mb-3">
                <span className="text-sm font-medium text-tg-text">Услуга {i + 1}</span>
                {formData.services.length > 1 && (
                  <button
                    onClick={() => removeService(i)}
                    className="text-red-500 text-sm"
                  >
                    Удалить
                  </button>
                )}
              </div>
              <div className="space-y-3">
                <input
                  value={service.name}
                  onChange={(e) => updateService(i, 'name', e.target.value)}
                  placeholder="Название услуги *"
                  className="input"
                />
                <input
                  value={service.summary}
                  onChange={(e) => updateService(i, 'summary', e.target.value)}
                  placeholder="Описание"
                  className="input"
                />
                <input
                  value={service.priceFrom}
                  onChange={(e) => updateService(i, 'priceFrom', e.target.value)}
                  placeholder="от 10 000 ₽"
                  className="input"
                />
              </div>
            </div>
          ))}
          <button
            onClick={addService}
            className="w-full py-3 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint"
          >
            + Добавить услугу
          </button>
        </div>
      )}

      {/* Details Tab */}
      {activeTab === 'details' && (
        <div className="space-y-4">
          <div>
            <label className="text-xs text-tg-hint mb-1 block">О компании</label>
            <textarea
              value={formData.summary}
              onChange={(e) => setFormData(prev => ({ ...prev, summary: e.target.value }))}
              className="input min-h-[100px] resize-none"
              placeholder="Подробное описание компании..."
            />
          </div>

          <div>
            <label className="text-xs text-tg-hint mb-1 block">Цветовая палитра</label>
            <input
              type="text"
              value={formData.color_palette}
              onChange={(e) => setFormData(prev => ({ ...prev, color_palette: e.target.value }))}
              className="input"
              placeholder="синий и белый"
            />
          </div>
        </div>
      )}

      {/* Actions */}
      <div className="flex gap-3 pt-4 border-t border-tg-separator">
        <button onClick={onCancel} className="btn btn-secondary flex-1">
          Отмена
        </button>
        <button onClick={handleSave} disabled={saving} className="btn btn-primary flex-1">
          {saving ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Сохранить'}
        </button>
      </div>
    </div>
  )
}
