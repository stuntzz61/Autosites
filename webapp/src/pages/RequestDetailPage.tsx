import { useState, useRef, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Phone, Mail, MapPin, Briefcase,
  Image, Archive, Send, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronDown, X, ExternalLink,
  Palette, Upload, Camera, Edit3, Trash2, Plus, Sparkles,
  ChevronLeft, ChevronRight, ZoomIn, ImageIcon, Globe,
  CreditCard, Power, RotateCcw, Play, Square, Link2, Shield,
  MessageSquare, FileEdit, History, SendHorizonal, RefreshCw
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { useAuthStore } from '@/stores/authStore'
import { requestsApi, servicesApi, sitesApi, revisionsApi } from '@/api/client'
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
  const [showServicesModal, setShowServicesModal] = useState(false)
  const [selectedCategory, setSelectedCategory] = useState('gallery')
  const [uploadingPhoto, setUploadingPhoto] = useState(false)
  const [viewingPhotoIndex, setViewingPhotoIndex] = useState<number | null>(null)
  const [showDomainModal, setShowDomainModal] = useState(false)
  const [domainInput, setDomainInput] = useState('')
  const [enableSsl, setEnableSsl] = useState(true)
  const [showRevisionsModal, setShowRevisionsModal] = useState(false)
  const [showNewRevisionModal, setShowNewRevisionModal] = useState(false)
  const [newRevisionChanges, setNewRevisionChanges] = useState<Array<{
    type: string
    description: string
    area: string
    screenshot?: File
    screenshotPreview?: string
  }>>([{ type: 'text_change', description: '', area: '' }])
  const [isCreatingRevision, setIsCreatingRevision] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const revisionScreenshotRef = useRef<HTMLInputElement>(null)

  // Check if user is admin
  const isAdmin = user?.role === 'admin'

  const { data: request, isLoading, error } = useQuery({
    queryKey: ['request', id],
    queryFn: () => requestsApi.get(id!).then(res => res.data),
    enabled: !!id,
  })

  // Available additional services
  const { data: availableServices = [] } = useQuery({
    queryKey: ['additional-services'],
    queryFn: () => servicesApi.list().then(res => res.data),
  })

  // Services attached to this request
  const { data: requestServices = [] } = useQuery({
    queryKey: ['request-services', id],
    queryFn: () => servicesApi.getForRequest(id!).then(res => res.data),
    enabled: !!id,
  })

  // Get client site for this request
  const { data: clientSite, isLoading: isLoadingSite } = useQuery({
    queryKey: ['client-site', id],
    queryFn: () => sitesApi.getByRequest(id!).then(res => res.data),
    enabled: !!id,
    retry: false, // Don't retry if site doesn't exist yet
    refetchInterval: (query) => {
      // Auto-refetch every 30 seconds if site has deploy_id and status is not final
      const site = query.state.data
      if (site?.deploy_id && site.deploy_status &&
          !['active', 'failed', 'stopped'].includes(site.deploy_status)) {
        return 30000 // 30 seconds
      }
      return false
    },
  })

  // Get revisions for this site
  const { data: revisionsData } = useQuery({
    queryKey: ['revisions', clientSite?.id],
    queryFn: () => revisionsApi.listBySite(clientSite!.id).then(res => res.data),
    enabled: !!clientSite?.id,
  })

  // Auto-sync status when page loads if site has deploy_id
  useEffect(() => {
    if (clientSite?.deploy_id) {
      // Sync once on mount if status is not final
      const nonFinalStatuses = ['pending', 'deploying', 'none']
      if (!clientSite.deploy_status || nonFinalStatuses.includes(clientSite.deploy_status)) {
        // Small delay to avoid race condition
        const timer = setTimeout(() => {
          if (!syncStatusMutation.isPending) {
            syncStatusMutation.mutate()
          }
        }, 2000)
        return () => clearTimeout(timer)
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clientSite?.deploy_id]) // Only sync once when deploy_id is available

  const addServiceMutation = useMutation({
    mutationFn: (serviceId: string) => servicesApi.add(id!, { service_id: serviceId }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request-services', id] })
      toast.success('Услуга добавлена')
      haptic?.notificationOccurred('success')
    },
    onError: () => {
      toast.error('Ошибка добавления')
      haptic?.notificationOccurred('error')
    },
  })

  const removeServiceMutation = useMutation({
    mutationFn: (serviceId: string) => servicesApi.remove(id!, serviceId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['request-services', id] })
      toast.success('Услуга удалена')
    },
    onError: () => toast.error('Ошибка'),
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

  // Deploy site mutation
  const deployMutation = useMutation({
    mutationFn: () => sitesApi.deploy(clientSite!.id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Деплой запущен!')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка запуска деплоя')
      haptic?.notificationOccurred('error')
    },
  })

  // Stop site mutation
  const stopMutation = useMutation({
    mutationFn: () => sitesApi.stop(clientSite!.id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Сайт остановлен')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка остановки')
      haptic?.notificationOccurred('error')
    },
  })

  // Sync status mutation
  const syncStatusMutation = useMutation({
    mutationFn: () => sitesApi.syncStatus(clientSite!.id),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success(`Статус синхронизирован: ${data.data?.status || 'обновлено'}`)
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка синхронизации')
      haptic?.notificationOccurred('error')
    },
  })

  // Create site for request mutation
  const createSiteMutation = useMutation({
    mutationFn: () => sitesApi.createForRequest(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Сайт создан!')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка создания сайта')
      haptic?.notificationOccurred('error')
    },
  })

  // Assign domain mutation
  const assignDomainMutation = useMutation({
    mutationFn: ({ domain, ssl }: { domain: string; ssl: boolean }) =>
      sitesApi.assignDomain(clientSite!.id, domain, ssl),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Домен привязан!')
      haptic?.notificationOccurred('success')
      setShowDomainModal(false)
      setDomainInput('')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка привязки домена')
      haptic?.notificationOccurred('error')
    },
  })

  // Create revision mutation
  const createRevisionMutation = useMutation({
    mutationFn: (changes: Array<{ type: string; client_description: string; location?: { area: string } }>) =>
      revisionsApi.create({
        site_id: clientSite!.id,
        changes,
        source: 'webapp',
        auto_submit: false,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['revisions', clientSite?.id] })
      toast.success('Правки созданы!')
      haptic?.notificationOccurred('success')
      setShowNewRevisionModal(false)
      setNewRevisionChanges([{ type: 'text_change', description: '', area: '' }])
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка создания правок')
      haptic?.notificationOccurred('error')
    },
  })

  // Submit revision mutation
  const submitRevisionMutation = useMutation({
    mutationFn: (revisionId: string) => revisionsApi.submit(revisionId, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['revisions', clientSite?.id] })
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Правки отправлены в обработку!')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка отправки правок')
      haptic?.notificationOccurred('error')
    },
  })

  // Cancel revision mutation
  const cancelRevisionMutation = useMutation({
    mutationFn: (revisionId: string) => revisionsApi.cancel(revisionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['revisions', clientSite?.id] })
      queryClient.invalidateQueries({ queryKey: ['client-site', id] })
      toast.success('Правки отменены')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка отмены правок')
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
  const tariff = request.tariff || 'standard'
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
          <div className="flex items-center gap-3">
            <div className="w-14 h-14 rounded-2xl bg-white/20 dark:bg-black/20 flex items-center justify-center text-2xl font-bold">
              {companyName[0]?.toUpperCase() || '?'}
            </div>
            {/* Tariff Badge */}
            {tariff === 'premium' && (
              <div className="flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-bold bg-purple-500/20 text-purple-300 dark:text-purple-600 border border-purple-400/30">
                <span>⭐</span>
                <span>PREMIUM</span>
              </div>
            )}
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

        {/* Additional Services Section */}
        <Section title={`Доп. услуги (${requestServices.length})`}>
          <div className="p-4 space-y-3">
            {requestServices.length > 0 ? (
              requestServices.map((service: any) => (
                <div key={service.id} className="flex items-center justify-between p-3 bg-tg-secondary-bg rounded-xl">
                  <div className="flex items-center gap-3">
                    <span className="text-xl">{service.icon || '✨'}</span>
                    <div>
                      <p className="font-medium text-tg-text">{service.name}</p>
                      <p className="text-xs text-tg-hint">
                        {service.status === 'pending' && '⏳ Ожидает'}
                        {service.status === 'in_progress' && '🔄 В работе'}
                        {service.status === 'completed' && '✅ Выполнено'}
                        {service.status === 'cancelled' && '❌ Отменено'}
                      </p>
                    </div>
                  </div>
                  <button
                    onClick={() => removeServiceMutation.mutate(service.service_id)}
                    className="p-2 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              ))
            ) : (
              <p className="text-center text-tg-hint py-2">Нет доп. услуг</p>
            )}

            <button
              onClick={() => setShowServicesModal(true)}
              className="w-full p-3 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint flex items-center justify-center gap-2"
            >
              <Plus className="w-4 h-4" />
              Добавить услугу
            </button>
          </div>
        </Section>

        <Section title={`Фото (${images.length})`}>
          <div className="p-4 space-y-4">
            {/* Upload button */}
            <button
              onClick={() => setShowPhotoUpload(true)}
              className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint hover:border-tg-link hover:text-tg-link transition-colors"
            >
              <Camera className="w-6 h-6 mx-auto mb-2" />
              <span className="text-sm font-medium">Добавить фото</span>
            </button>

            {images.length > 0 ? (
              <div className="space-y-3">
                {/* Main grid - 2 columns for larger previews */}
                <div className="grid grid-cols-2 gap-3">
                  {images.map((img: any, i: number) => (
                    <motion.div
                      key={i}
                      className="relative aspect-[4/3] group"
                      whileTap={{ scale: 0.98 }}
                    >
                      <img
                        src={img.url}
                        alt={img.category || 'Фото'}
                        className="w-full h-full rounded-2xl object-cover bg-tg-secondary-bg cursor-pointer"
                        onClick={() => {
                          haptic?.impactOccurred('light')
                          setViewingPhotoIndex(i)
                        }}
                        onError={(e) => {
                          (e.target as HTMLImageElement).src = 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="%23666"><path d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"/></svg>'
                        }}
                      />

                      {/* Overlay on hover/tap */}
                      <div
                        className="absolute inset-0 bg-black/0 group-hover:bg-black/20 transition-colors rounded-2xl flex items-center justify-center cursor-pointer"
                        onClick={() => {
                          haptic?.impactOccurred('light')
                          setViewingPhotoIndex(i)
                        }}
                      >
                        <ZoomIn className="w-8 h-8 text-white opacity-0 group-hover:opacity-100 transition-opacity drop-shadow-lg" />
                      </div>

                      {/* Delete button */}
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          handleDeletePhoto(img.url)
                        }}
                        className="absolute top-2 right-2 w-8 h-8 bg-red-500 text-white rounded-full flex items-center justify-center shadow-lg opacity-90 hover:opacity-100 transition-opacity"
                      >
                        <X className="w-4 h-4" />
                      </button>

                      {/* Category badge */}
                      {img.category && (
                        <div className="absolute bottom-2 left-2 px-2.5 py-1 bg-black/70 backdrop-blur-sm text-white text-xs font-medium rounded-lg">
                          {photoCategories.find(c => c.id === img.category)?.label || img.category}
                        </div>
                      )}
                    </motion.div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="text-center py-8">
                <ImageIcon className="w-12 h-12 text-tg-hint/50 mx-auto mb-3" />
                <p className="text-tg-hint">Нет фото</p>
                <p className="text-xs text-tg-hint/70 mt-1">Добавьте фото для заявки</p>
              </div>
            )}
          </div>
        </Section>

        {/* Create Site Button - if site doesn't exist */}
        {!clientSite && !isLoadingSite && (
          <Section title="Сайт">
            <div className="p-4">
              <div className="text-center py-6">
                <Globe className="w-12 h-12 text-tg-hint/50 mx-auto mb-3" />
                <p className="text-tg-hint mb-4">Сайт ещё не создан для этой заявки</p>
                <button
                  onClick={() => createSiteMutation.mutate()}
                  disabled={createSiteMutation.isPending}
                  className="inline-flex items-center gap-2 px-4 py-2 bg-tg-button text-tg-button-text rounded-xl hover:opacity-90 transition-opacity disabled:opacity-50"
                >
                  {createSiteMutation.isPending ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Plus className="w-4 h-4" />
                  )}
                  Создать сайт
                </button>
                <p className="text-xs text-tg-hint/70 mt-3">
                  После создания вы сможете управлять хостингом, деплоем и правками
                </p>
              </div>
            </div>
          </Section>
        )}

        {/* Client Site Info & Payment Section - only show when there's actual site content or deploy */}
        {clientSite && (clientSite.archive_s3_key || clientSite.deploy_id || clientSite.deploy_status && clientSite.deploy_status !== 'none') && (
          <Section title="Хостинг сайта">
            <div className="p-4 space-y-3">
              {/* Site Status */}
              <div className="flex items-center justify-between p-3 bg-tg-secondary-bg rounded-xl">
                <div className="flex items-center gap-3">
                  <Globe className="w-5 h-5 text-blue-500" />
                  <div>
                    <p className="font-medium text-tg-text">Статус деплоя</p>
                    <p className="text-sm text-tg-hint">
                      {(clientSite.deploy_status === 'active' || clientSite.deploy_status === 'running') && '✅ Активен'}
                      {(clientSite.deploy_status === 'deploying' || clientSite.deploy_status === 'building' || clientSite.deploy_status === 'uploading') && '🔄 Деплоится...'}
                      {clientSite.deploy_status === 'pending' && '⏳ Ожидает деплоя'}
                      {(clientSite.deploy_status === 'failed' || clientSite.deploy_status === 'error') && '❌ Ошибка'}
                      {clientSite.deploy_status === 'stopped' && '⏸ Остановлен'}
                      {(!clientSite.deploy_status || clientSite.deploy_status === 'none') && '⏸ Не задеплоен'}
                    </p>
                    {clientSite.deploy_id && (
                      <p className="text-xs text-tg-hint/70 mt-0.5">
                        Deploy ID: {clientSite.deploy_id.slice(0, 8)}...
                      </p>
                    )}
                  </div>
                </div>

                {/* Deploy/Stop/Sync Controls */}
                <div className="flex gap-2">
                  {/* Sync button - always visible if deploy_id exists */}
                  {clientSite.deploy_id && (
                    <button
                      onClick={() => syncStatusMutation.mutate()}
                      disabled={syncStatusMutation.isPending}
                      className="p-2 bg-blue-500/10 text-blue-500 rounded-lg hover:bg-blue-500/20 transition-colors disabled:opacity-50"
                      title="Синхронизировать статус с deploy-node"
                    >
                      {syncStatusMutation.isPending ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <RefreshCw className="w-4 h-4" />
                      )}
                    </button>
                  )}

                  {(['none', 'stopped', 'failed', 'error'].includes(clientSite.deploy_status || '') || !clientSite.deploy_status) && clientSite.archive_s3_key && (
                    <button
                      onClick={() => deployMutation.mutate()}
                      disabled={deployMutation.isPending}
                      className="p-2 bg-green-500 text-white rounded-lg hover:bg-green-600 transition-colors disabled:opacity-50"
                      title="Запустить деплой"
                    >
                      {deployMutation.isPending ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <Play className="w-4 h-4" />
                      )}
                    </button>
                  )}

                  {(clientSite.deploy_status === 'active' || clientSite.deploy_status === 'running') && (
                    <button
                      onClick={() => stopMutation.mutate()}
                      disabled={stopMutation.isPending}
                      className="p-2 bg-red-500 text-white rounded-lg hover:bg-red-600 transition-colors disabled:opacity-50"
                      title="Остановить сайт"
                    >
                      {stopMutation.isPending ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <Square className="w-4 h-4" />
                      )}
                    </button>
                  )}
                </div>
              </div>

              {/* Error Message */}
              {clientSite.last_error && (
                <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl">
                  <p className="text-sm text-red-600 dark:text-red-400">
                    <AlertCircle className="w-4 h-4 inline mr-1" />
                    {clientSite.last_error}
                  </p>
                </div>
              )}

              {/* Preview URL */}
              {clientSite.preview_url && (
                <a
                  href={clientSite.preview_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-3 p-3 bg-tg-secondary-bg rounded-xl hover:bg-tg-hint/5 transition-colors"
                >
                  <ExternalLink className="w-5 h-5 text-blue-500" />
                  <div className="flex-1">
                    <p className="font-medium text-tg-text">Preview URL</p>
                    <p className="text-sm text-tg-link truncate">{clientSite.preview_url}</p>
                  </div>
                </a>
              )}

              {/* Domain */}
              <div className="flex items-center gap-3 p-3 bg-tg-secondary-bg rounded-xl">
                <Link2 className="w-5 h-5 text-purple-500" />
                <div className="flex-1">
                  <p className="font-medium text-tg-text">Домен</p>
                  {clientSite.domain ? (
                    <div className="flex items-center gap-2">
                      <p className="text-sm text-tg-hint">{clientSite.domain}</p>
                      {clientSite.ssl_enabled && (
                        <Shield className="w-3 h-3 text-green-500" title="SSL включен" />
                      )}
                      <span className={`text-xs px-1.5 py-0.5 rounded ${
                        clientSite.domain_status === 'active' ? 'bg-green-100 text-green-700' :
                        clientSite.domain_status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-gray-100 text-gray-700'
                      }`}>
                        {clientSite.domain_status === 'active' ? 'Активен' :
                         clientSite.domain_status === 'pending' ? 'Настраивается' : 'Неактивен'}
                      </span>
                    </div>
                  ) : (
                    <p className="text-sm text-tg-hint">Не привязан</p>
                  )}
                </div>
                <button
                  onClick={() => {
                    setDomainInput(clientSite.domain || '')
                    setShowDomainModal(true)
                  }}
                  className="p-2 bg-purple-500/10 text-purple-500 rounded-lg hover:bg-purple-500/20 transition-colors"
                  title="Настроить домен"
                >
                  <Edit3 className="w-4 h-4" />
                </button>
              </div>

              {/* Hosting Info */}
              <div className="p-3 bg-tg-secondary-bg rounded-xl space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-tg-hint">Тариф:</span>
                  <span className="font-medium text-tg-text capitalize">
                    {clientSite.hosting_plan === 'trial' ? '🆓 Пробный' :
                     clientSite.hosting_plan === 'basic' ? '📦 Базовый' :
                     clientSite.hosting_plan === 'pro' ? '⭐ Профессиональный' :
                     clientSite.hosting_plan === 'enterprise' ? '🏢 Корпоративный' :
                     clientSite.hosting_plan || 'trial'}
                  </span>
                </div>
                {clientSite.hosting_expires_at && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-tg-hint">Истекает:</span>
                    <span className={`font-medium ${
                      new Date(clientSite.hosting_expires_at) < new Date() ? 'text-red-500' :
                      new Date(clientSite.hosting_expires_at) < new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) ? 'text-orange-500' :
                      'text-tg-text'
                    }`}>
                      {new Date(clientSite.hosting_expires_at).toLocaleDateString('ru-RU')}
                    </span>
                  </div>
                )}
                {clientSite.server_name && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-tg-hint">Сервер:</span>
                    <span className="font-medium text-tg-text">{clientSite.server_name}</span>
                  </div>
                )}
              </div>

              {/* Hosting expiration warning */}
              {clientSite.hosting_expires_at && new Date(clientSite.hosting_expires_at) < new Date(Date.now() + 14 * 24 * 60 * 60 * 1000) && (
                <div className={`p-4 rounded-xl ${
                  new Date(clientSite.hosting_expires_at) < new Date()
                    ? 'bg-red-100 dark:bg-red-900/30 border-2 border-red-300 dark:border-red-700'
                    : 'bg-orange-100 dark:bg-orange-900/30 border-2 border-orange-300 dark:border-orange-700'
                }`}>
                  <div className="flex items-center gap-3 mb-2">
                    <AlertCircle className={`w-6 h-6 ${
                      new Date(clientSite.hosting_expires_at) < new Date() ? 'text-red-500' : 'text-orange-500'
                    }`} />
                    <div>
                      <p className="font-semibold text-tg-text">
                        {new Date(clientSite.hosting_expires_at) < new Date()
                          ? '⚠️ Хостинг истёк!'
                          : '⏰ Хостинг скоро истекает'}
                      </p>
                      <p className="text-sm text-tg-hint">
                        {new Date(clientSite.hosting_expires_at) < new Date()
                          ? 'Продлите хостинг, чтобы сайт продолжал работать'
                          : `Истекает ${new Date(clientSite.hosting_expires_at).toLocaleDateString('ru-RU')}`}
                      </p>
                    </div>
                  </div>
                </div>
              )}

              {/* Payment Button */}
              <button
                onClick={() => navigate(`/sites/${clientSite.id}/payment`)}
                className="w-full p-4 bg-gradient-to-r from-purple-500 to-blue-500 rounded-xl text-white font-medium flex items-center justify-center gap-2 hover:opacity-90 transition-opacity"
              >
                <CreditCard className="w-5 h-5" />
                Оплатить хостинг (QR / СБП)
              </button>
            </div>
          </Section>
        )}

        {/* Revisions Section - show when site has archive (even if not deployed) */}
        {clientSite && clientSite.archive_s3_key && (
          <Section title="Правки сайта">
            <div className="p-4 space-y-3">
              {/* Active revision status */}
              {clientSite.revision_status && clientSite.revision_status !== 'completed' && (
                <div className={`p-3 rounded-xl ${
                  clientSite.revision_status === 'processing' ? 'bg-blue-50 dark:bg-blue-900/20' :
                  clientSite.revision_status === 'pending' ? 'bg-yellow-50 dark:bg-yellow-900/20' :
                  clientSite.revision_status === 'failed' ? 'bg-red-50 dark:bg-red-900/20' :
                  'bg-gray-50 dark:bg-gray-900/20'
                }`}>
                  <div className="flex items-center gap-2">
                    {clientSite.revision_status === 'processing' && <Loader2 className="w-4 h-4 animate-spin text-blue-500" />}
                    {clientSite.revision_status === 'pending' && <Clock className="w-4 h-4 text-yellow-500" />}
                    {clientSite.revision_status === 'in_progress' && <Loader2 className="w-4 h-4 animate-spin text-orange-500" />}
                    {clientSite.revision_status === 'failed' && <AlertCircle className="w-4 h-4 text-red-500" />}
                    <span className="text-sm font-medium">
                      {clientSite.revision_status === 'processing' && 'Правки обрабатываются...'}
                      {clientSite.revision_status === 'pending' && 'Есть неотправленные правки'}
                      {clientSite.revision_status === 'in_progress' && 'Правки в работе'}
                      {clientSite.revision_status === 'failed' && 'Ошибка при обработке правок'}
                    </span>
                  </div>
                </div>
              )}

              {/* Revision History */}
              {revisionsData?.items && revisionsData.items.length > 0 && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium text-tg-hint">История правок</span>
                    <span className="text-xs text-tg-hint">
                      Итераций: {revisionsData.items.length}
                    </span>
                  </div>
                  {revisionsData.items.slice(0, 3).map((revision: any) => (
                    <div
                      key={revision.id}
                      className="flex items-center justify-between p-3 bg-tg-secondary-bg rounded-xl"
                    >
                      <div className="flex items-center gap-3">
                        <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${
                          revision.status === 'completed' ? 'bg-green-100 text-green-600' :
                          revision.status === 'processing' ? 'bg-blue-100 text-blue-600' :
                          revision.status === 'pending' ? 'bg-yellow-100 text-yellow-600' :
                          revision.status === 'failed' ? 'bg-red-100 text-red-600' :
                          'bg-gray-100 text-gray-600'
                        }`}>
                          {revision.status === 'completed' && <CheckCircle2 className="w-4 h-4" />}
                          {revision.status === 'processing' && <Loader2 className="w-4 h-4 animate-spin" />}
                          {revision.status === 'pending' && <Clock className="w-4 h-4" />}
                          {revision.status === 'in_progress' && <Loader2 className="w-4 h-4 animate-spin" />}
                          {revision.status === 'failed' && <AlertCircle className="w-4 h-4" />}
                          {revision.status === 'cancelled' && <X className="w-4 h-4" />}
                        </div>
                        <div>
                          <p className="text-sm font-medium text-tg-text">
                            Итерация #{revision.iteration}
                          </p>
                          <p className="text-xs text-tg-hint">
                            {revision.changes_count || 0} правок • {new Date(revision.created_at).toLocaleDateString('ru-RU')}
                          </p>
                        </div>
                      </div>
                      <div className="flex gap-2">
                        {revision.status === 'pending' && (
                          <>
                            <button
                              onClick={() => submitRevisionMutation.mutate(revision.id)}
                              disabled={submitRevisionMutation.isPending}
                              className="p-2 bg-green-500 text-white rounded-lg hover:bg-green-600 transition-colors disabled:opacity-50"
                              title="Отправить на обработку"
                            >
                              {submitRevisionMutation.isPending ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                              ) : (
                                <SendHorizonal className="w-4 h-4" />
                              )}
                            </button>
                            <button
                              onClick={() => cancelRevisionMutation.mutate(revision.id)}
                              disabled={cancelRevisionMutation.isPending}
                              className="p-2 bg-red-500/10 text-red-500 rounded-lg hover:bg-red-500/20 transition-colors disabled:opacity-50"
                              title="Отменить"
                            >
                              <X className="w-4 h-4" />
                            </button>
                          </>
                        )}
                        {(revision.status === 'processing' || revision.status === 'in_progress') && (
                          <button
                            onClick={() => cancelRevisionMutation.mutate(revision.id)}
                            disabled={cancelRevisionMutation.isPending}
                            className="p-2 bg-red-500/10 text-red-500 rounded-lg hover:bg-red-500/20 transition-colors disabled:opacity-50"
                            title="Отменить"
                          >
                            <X className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* Create New Revision Button */}
              <button
                onClick={() => setShowNewRevisionModal(true)}
                className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint hover:border-blue-500 hover:text-blue-500 transition-colors flex items-center justify-center gap-2"
              >
                <FileEdit className="w-5 h-5" />
                Запросить правки
              </button>
            </div>
          </Section>
        )}

        {/* Domain Assignment Modal */}
        <AnimatePresence>
          {showDomainModal && clientSite && (
            <>
              <motion.div
                className="fixed inset-0 bg-black/50 z-40"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                onClick={() => setShowDomainModal(false)}
              />
              <motion.div
                className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom"
                initial={{ y: '100%' }}
                animate={{ y: 0 }}
                exit={{ y: '100%' }}
              >
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
                <div className="flex items-center gap-2 mb-4">
                  <Link2 className="w-5 h-5 text-purple-500" />
                  <p className="text-lg font-semibold">Настройка домена</p>
                </div>

                <div className="space-y-4">
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Домен</label>
                    <input
                      type="text"
                      value={domainInput}
                      onChange={(e) => setDomainInput(e.target.value.toLowerCase().trim())}
                      placeholder="example.com"
                      className="input"
                    />
                    <p className="text-xs text-tg-hint mt-1">
                      Укажите домен без http:// и www
                    </p>
                  </div>

                  <label className="flex items-center gap-3 p-3 bg-tg-secondary-bg rounded-xl cursor-pointer">
                    <input
                      type="checkbox"
                      checked={enableSsl}
                      onChange={(e) => setEnableSsl(e.target.checked)}
                      className="w-5 h-5 rounded border-tg-separator"
                    />
                    <div className="flex-1">
                      <p className="font-medium text-tg-text flex items-center gap-2">
                        <Shield className="w-4 h-4 text-green-500" />
                        Включить SSL (HTTPS)
                      </p>
                      <p className="text-xs text-tg-hint">Бесплатный сертификат Let's Encrypt</p>
                    </div>
                  </label>

                  <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl p-3">
                    <p className="text-sm text-blue-600 dark:text-blue-400">
                      <strong>Инструкция:</strong>
                      <br />1. Добавьте A-запись для домена, указав IP: <code className="bg-blue-100 dark:bg-blue-800 px-1 rounded">{clientSite.server_host || 'см. сервер'}</code>
                      <br />2. Дождитесь обновления DNS (5-60 минут)
                      <br />3. Нажмите "Привязать домен"
                    </p>
                  </div>

                  <div className="flex gap-3">
                    <button
                      onClick={() => setShowDomainModal(false)}
                      className="btn btn-secondary flex-1"
                    >
                      Отмена
                    </button>
                    <button
                      onClick={() => assignDomainMutation.mutate({ domain: domainInput, ssl: enableSsl })}
                      disabled={!domainInput || assignDomainMutation.isPending}
                      className="btn btn-primary flex-1"
                    >
                      {assignDomainMutation.isPending ? (
                        <Loader2 className="w-5 h-5 animate-spin" />
                      ) : (
                        'Привязать домен'
                      )}
                    </button>
                  </div>
                </div>
              </motion.div>
            </>
          )}
        </AnimatePresence>

        {/* New Revision Modal */}
        <AnimatePresence>
          {showNewRevisionModal && clientSite && (
            <>
              <motion.div
                className="fixed inset-0 bg-black/50 z-40"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                onClick={() => setShowNewRevisionModal(false)}
              />
              <motion.div
                className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[80vh] overflow-y-auto"
                initial={{ y: '100%' }}
                animate={{ y: 0 }}
                exit={{ y: '100%' }}
              >
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
                <div className="flex items-center gap-2 mb-4">
                  <FileEdit className="w-5 h-5 text-blue-500" />
                  <p className="text-lg font-semibold">Запросить правки</p>
                </div>

                <div className="space-y-4">
                  <p className="text-sm text-tg-hint">
                    Опишите, что нужно изменить на сайте. Вы можете добавить несколько правок.
                  </p>

                  {/* Changes List */}
                  {newRevisionChanges.map((change, index) => (
                    <div key={index} className="bg-tg-secondary-bg rounded-xl p-4 space-y-3">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-tg-text">Правка {index + 1}</span>
                        {newRevisionChanges.length > 1 && (
                          <button
                            onClick={() => setNewRevisionChanges(prev => prev.filter((_, i) => i !== index))}
                            className="text-red-500 text-sm"
                          >
                            Удалить
                          </button>
                        )}
                      </div>

                      {/* Change Type */}
                      <div>
                        <label className="text-xs text-tg-hint mb-1 block">Тип изменения</label>
                        <select
                          value={change.type}
                          onChange={(e) => setNewRevisionChanges(prev =>
                            prev.map((c, i) => i === index ? { ...c, type: e.target.value } : c)
                          )}
                          className="input"
                        >
                          <option value="text_change">Изменение текста</option>
                          <option value="visual_change">Визуальное изменение</option>
                          <option value="layout_change">Изменение расположения</option>
                          <option value="content_add">Добавить контент</option>
                          <option value="content_remove">Удалить контент</option>
                          <option value="style_change">Изменить стиль</option>
                        </select>
                      </div>

                      {/* Area */}
                      <div>
                        <label className="text-xs text-tg-hint mb-1 block">Область сайта</label>
                        <select
                          value={change.area}
                          onChange={(e) => setNewRevisionChanges(prev =>
                            prev.map((c, i) => i === index ? { ...c, area: e.target.value } : c)
                          )}
                          className="input"
                        >
                          <option value="">Выберите область</option>
                          <option value="hero">Шапка/Hero</option>
                          <option value="header">Навигация</option>
                          <option value="footer">Подвал</option>
                          <option value="about">О компании</option>
                          <option value="services">Услуги</option>
                          <option value="portfolio">Портфолио</option>
                          <option value="contacts">Контакты</option>
                          <option value="other">Другое</option>
                        </select>
                      </div>

                      {/* Description */}
                      <div>
                        <label className="text-xs text-tg-hint mb-1 block">Описание правки *</label>
                        <textarea
                          value={change.description}
                          onChange={(e) => setNewRevisionChanges(prev =>
                            prev.map((c, i) => i === index ? { ...c, description: e.target.value } : c)
                          )}
                          placeholder="Опишите, что нужно изменить..."
                          className="input min-h-[80px] resize-none"
                        />
                      </div>

                      {/* Screenshot Upload */}
                      <div>
                        <label className="text-xs text-tg-hint mb-1 block">Скриншот (обведите элемент)</label>
                        {change.screenshotPreview ? (
                          <div className="relative">
                            <img
                              src={change.screenshotPreview}
                              alt="Скриншот"
                              className="w-full h-32 object-cover rounded-xl"
                            />
                            <button
                              onClick={() => {
                                setNewRevisionChanges(prev =>
                                  prev.map((c, i) => i === index ? { ...c, screenshot: undefined, screenshotPreview: undefined } : c)
                                )
                              }}
                              className="absolute top-2 right-2 w-6 h-6 bg-red-500 text-white rounded-full flex items-center justify-center"
                            >
                              <X className="w-4 h-4" />
                            </button>
                          </div>
                        ) : (
                          <label className="flex items-center justify-center gap-2 p-4 border-2 border-dashed border-tg-separator rounded-xl cursor-pointer hover:border-blue-500 hover:text-blue-500 transition-colors">
                            <input
                              type="file"
                              accept="image/*"
                              className="hidden"
                              onChange={(e) => {
                                const file = e.target.files?.[0]
                                if (file) {
                                  const reader = new FileReader()
                                  reader.onloadend = () => {
                                    setNewRevisionChanges(prev =>
                                      prev.map((c, i) => i === index ? {
                                        ...c,
                                        screenshot: file,
                                        screenshotPreview: reader.result as string
                                      } : c)
                                    )
                                  }
                                  reader.readAsDataURL(file)
                                }
                              }}
                            />
                            <Camera className="w-5 h-5" />
                            <span className="text-sm">Добавить скриншот</span>
                          </label>
                        )}
                        <p className="text-xs text-tg-hint/70 mt-1">
                          Сделайте скриншот и обведите место, которое нужно изменить
                        </p>
                      </div>
                    </div>
                  ))}

                  {/* Add Another Change */}
                  <button
                    onClick={() => setNewRevisionChanges(prev => [
                      ...prev,
                      { type: 'text_change', description: '', area: '' }
                    ])}
                    className="w-full p-3 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint"
                  >
                    + Добавить ещё правку
                  </button>

                  {/* Info */}
                  <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl p-3">
                    <p className="text-sm text-blue-600 dark:text-blue-400">
                      💡 <strong>Совет:</strong> Приложите скриншот и обведите элемент, который нужно изменить — так правки будут обработаны точнее!
                    </p>
                  </div>

                  {/* Actions */}
                  <div className="flex gap-3">
                    <button
                      onClick={() => setShowNewRevisionModal(false)}
                      className="btn btn-secondary flex-1"
                    >
                      Отмена
                    </button>
                    <button
                      onClick={async () => {
                        const validChanges = newRevisionChanges
                          .filter(c => c.description.trim())

                        if (validChanges.length === 0) {
                          toast.error('Добавьте хотя бы одну правку с описанием')
                          return
                        }

                        setIsCreatingRevision(true)

                        // Prepare changes for API
                        const changesForApi = validChanges.map(c => ({
                          type: c.type,
                          client_description: c.description,
                          location: c.area ? { area: c.area } : undefined,
                        }))

                        try {
                          // First create the revision
                          const result = await revisionsApi.create({
                            site_id: clientSite!.id,
                            changes: changesForApi,
                            source: 'webapp',
                            auto_submit: false,
                          })

                          const revisionId = result.data.id

                          // Then upload screenshots for changes that have them
                          const screenshotChanges = validChanges.filter(c => c.screenshot)
                          for (const change of screenshotChanges) {
                            if (change.screenshot) {
                              const formData = new FormData()
                              formData.append('file', change.screenshot)
                              formData.append('comment', change.description.substring(0, 100))
                              await revisionsApi.uploadScreenshot(revisionId, formData)
                            }
                          }

                          queryClient.invalidateQueries({ queryKey: ['revisions', clientSite?.id] })
                          toast.success('Правки созданы!')
                          haptic?.notificationOccurred('success')
                          setShowNewRevisionModal(false)
                          setNewRevisionChanges([{ type: 'text_change', description: '', area: '' }])
                        } catch (error: any) {
                          toast.error(error.response?.data?.detail || 'Ошибка создания правок')
                          haptic?.notificationOccurred('error')
                        } finally {
                          setIsCreatingRevision(false)
                        }
                      }}
                      disabled={isCreatingRevision}
                      className="btn btn-primary flex-1"
                    >
                      {isCreatingRevision ? (
                        <Loader2 className="w-5 h-5 animate-spin" />
                      ) : (
                        'Создать правки'
                      )}
                    </button>
                  </div>
                </div>
              </motion.div>
            </>
          )}
        </AnimatePresence>

        {/* Photo Viewer Modal */}
        <AnimatePresence>
          {viewingPhotoIndex !== null && images[viewingPhotoIndex] && (
            <>
              <motion.div
                className="fixed inset-0 bg-black z-50"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                onClick={() => setViewingPhotoIndex(null)}
              />
              <motion.div
                className="fixed inset-0 z-50 flex items-center justify-center p-4"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.9 }}
              >
                {/* Close button */}
                <button
                  onClick={() => setViewingPhotoIndex(null)}
                  className="absolute top-4 right-4 z-10 w-10 h-10 bg-white/20 backdrop-blur-sm rounded-full flex items-center justify-center text-white hover:bg-white/30 transition-colors"
                >
                  <X className="w-6 h-6" />
                </button>

                {/* Photo counter */}
                <div className="absolute top-4 left-4 z-10 px-3 py-1.5 bg-white/20 backdrop-blur-sm rounded-full text-white text-sm font-medium">
                  {viewingPhotoIndex + 1} / {images.length}
                </div>

                {/* Previous button */}
                {images.length > 1 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      haptic?.impactOccurred('light')
                      setViewingPhotoIndex(prev =>
                        prev !== null ? (prev === 0 ? images.length - 1 : prev - 1) : 0
                      )
                    }}
                    className="absolute left-2 z-10 w-12 h-12 bg-white/20 backdrop-blur-sm rounded-full flex items-center justify-center text-white hover:bg-white/30 transition-colors"
                  >
                    <ChevronLeft className="w-7 h-7" />
                  </button>
                )}

                {/* Next button */}
                {images.length > 1 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      haptic?.impactOccurred('light')
                      setViewingPhotoIndex(prev =>
                        prev !== null ? (prev === images.length - 1 ? 0 : prev + 1) : 0
                      )
                    }}
                    className="absolute right-2 z-10 w-12 h-12 bg-white/20 backdrop-blur-sm rounded-full flex items-center justify-center text-white hover:bg-white/30 transition-colors"
                  >
                    <ChevronRight className="w-7 h-7" />
                  </button>
                )}

                {/* Image */}
                <img
                  src={images[viewingPhotoIndex].url}
                  alt={images[viewingPhotoIndex].category || 'Фото'}
                  className="max-w-full max-h-full object-contain rounded-lg"
                  onClick={(e) => e.stopPropagation()}
                />

                {/* Category label at bottom */}
                {images[viewingPhotoIndex].category && (
                  <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-10 px-4 py-2 bg-white/20 backdrop-blur-sm rounded-full text-white text-sm font-medium">
                    {photoCategories.find(c => c.id === images[viewingPhotoIndex].category)?.label || images[viewingPhotoIndex].category}
                  </div>
                )}
              </motion.div>
            </>
          )}
        </AnimatePresence>
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
            <button onClick={handleGenerate} disabled={generateMutation.isPending} className="btn btn-primary flex-1 min-w-0">
              {generateMutation.isPending ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <>
                  <Send className="w-5 h-5 flex-shrink-0" />
                  <span className="truncate text-sm">В разработку</span>
                </>
              )}
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

      {/* Additional Services Modal */}
      <AnimatePresence>
        {showServicesModal && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowServicesModal(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[70vh] overflow-y-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <div className="flex items-center gap-2 mb-4">
                <Sparkles className="w-5 h-5 text-amber-500" />
                <p className="text-lg font-semibold">Дополнительные услуги</p>
              </div>

              <div className="space-y-3">
                {availableServices.map((service: any) => {
                  const isAdded = requestServices.some((rs: any) => rs.service_id === service.id)
                  return (
                    <button
                      key={service.id}
                      onClick={() => {
                        if (!isAdded) {
                          addServiceMutation.mutate(service.id)
                        }
                      }}
                      disabled={isAdded || addServiceMutation.isPending}
                      className={clsx(
                        'w-full p-4 rounded-xl text-left transition-all',
                        isAdded
                          ? 'bg-emerald-50 dark:bg-emerald-900/20 border-2 border-emerald-500'
                          : 'bg-tg-secondary-bg hover:bg-tg-hint/10'
                      )}
                    >
                      <div className="flex items-start gap-3">
                        <span className="text-2xl">{service.icon || '✨'}</span>
                        <div className="flex-1">
                          <div className="flex items-center justify-between">
                            <p className="font-medium text-tg-text">{service.name}</p>
                            {isAdded && <CheckCircle2 className="w-5 h-5 text-emerald-500" />}
                          </div>
                          <p className="text-sm text-tg-hint mt-1">{service.description}</p>
                          {service.price_info && (
                            <p className="text-xs text-tg-link mt-2">{service.price_info}</p>
                          )}
                        </div>
                      </div>
                    </button>
                  )
                })}
              </div>

              <button
                onClick={() => setShowServicesModal(false)}
                className="btn btn-primary w-full mt-4"
              >
                Готово
              </button>
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
    tariff: request.tariff || 'standard',
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
        tariff: formData.tariff,
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

          {/* Tariff Selection */}
          <div>
            <label className="text-xs text-tg-hint mb-2 block">Тариф генерации</label>
            <div className="grid grid-cols-2 gap-3">
              <button
                type="button"
                onClick={() => setFormData(prev => ({ ...prev, tariff: 'standard' }))}
                className={clsx(
                  'p-4 rounded-2xl border-2 transition-all text-left',
                  formData.tariff === 'standard'
                    ? 'border-blue-500 bg-blue-500/10'
                    : 'border-zinc-200 dark:border-zinc-700'
                )}
              >
                <div className="flex items-center gap-2 mb-2">
                  <div className={clsx(
                    'w-5 h-5 rounded-full border-2 flex items-center justify-center',
                    formData.tariff === 'standard' ? 'border-blue-500' : 'border-zinc-300 dark:border-zinc-600'
                  )}>
                    {formData.tariff === 'standard' && (
                      <div className="w-3 h-3 rounded-full bg-blue-500" />
                    )}
                  </div>
                  <span className="font-semibold text-tg-text">Стандарт</span>
                </div>
                <p className="text-xs text-tg-hint">Базовая генерация лендинга</p>
                <p className="text-sm font-bold text-green-500 mt-2">Бесплатно</p>
              </button>

              <button
                type="button"
                onClick={() => setFormData(prev => ({ ...prev, tariff: 'premium' }))}
                className={clsx(
                  'p-4 rounded-2xl border-2 transition-all text-left relative overflow-hidden',
                  formData.tariff === 'premium'
                    ? 'border-purple-500 bg-gradient-to-br from-purple-500/20 via-purple-500/10 to-blue-500/10 shadow-lg shadow-purple-500/20'
                    : 'border-zinc-200 dark:border-zinc-700 hover:border-purple-300 dark:hover:border-purple-600'
                )}
              >
                {/* Premium badge */}
                <div className="absolute top-2 right-2">
                  <span className="text-xs bg-gradient-to-r from-purple-500 to-blue-500 text-white px-2.5 py-1 rounded-full font-bold shadow-md">
                    ⭐ PRO
                  </span>
                </div>

                {/* Shine effect when selected */}
                {formData.tariff === 'premium' && (
                  <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent pointer-events-none" style={{
                    backgroundSize: '200% 100%',
                    animation: 'shimmer 2s ease-in-out infinite'
                  }} />
                )}

                <div className="flex items-center gap-2 mb-2 relative z-10">
                  <div className={clsx(
                    'w-5 h-5 rounded-full border-2 flex items-center justify-center transition-all',
                    formData.tariff === 'premium'
                      ? 'border-purple-500 bg-purple-500/20 shadow-md'
                      : 'border-zinc-300 dark:border-zinc-600'
                  )}>
                    {formData.tariff === 'premium' && (
                      <div className="w-3 h-3 rounded-full bg-gradient-to-r from-purple-500 to-blue-500 shadow-sm" />
                    )}
                  </div>
                  <span className={clsx(
                    'font-bold text-base',
                    formData.tariff === 'premium'
                      ? 'bg-gradient-to-r from-purple-600 to-blue-600 bg-clip-text text-transparent'
                      : 'text-tg-text'
                  )}>
                    Премиум лендинг
                  </span>
                </div>
                <p className="text-xs text-tg-hint mb-2">Профессиональный дизайн и качество</p>
                <div className="flex items-center gap-1 mt-2">
                  <span className="text-sm font-bold bg-gradient-to-r from-purple-600 to-blue-600 bg-clip-text text-transparent">
                    Платно
                  </span>
                  <span className="text-xs text-tg-hint">• Премиум качество</span>
                </div>
              </button>
            </div>
            {formData.tariff === 'premium' && request.tariff === 'standard' && (
              <p className="text-xs text-blue-500 mt-2 flex items-center gap-1">
                <AlertCircle className="w-3 h-3" />
                Тариф будет изменён с базового на премиум
              </p>
            )}
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
