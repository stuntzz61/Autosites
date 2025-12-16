import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Briefcase, Phone, Mail, MapPin,
  ArrowRight, ArrowLeft, Check, Loader2, Plus, X,
  Clock, Palette, Camera, Upload, Image, AlertCircle,
  RotateCcw
} from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import Tooltip from '@/components/Tooltip'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const DRAFT_STORAGE_KEY = 'autosites_request_draft'

const steps = [
  { id: 'company', title: 'Компания', icon: Building2 },
  { id: 'client', title: 'Клиент', icon: User },
  { id: 'contacts', title: 'Контакты', icon: Phone },
  { id: 'services', title: 'Услуга/Товар', icon: Briefcase },
  { id: 'photos', title: 'Фото', icon: Camera },
  { id: 'details', title: 'Детали', icon: Palette },
]

const DEFAULT_STRUCTURE = ['Hero', 'О компании', 'Услуги', 'Портфолио', 'Отзывы', 'Контакты']

const photoCategories = [
  { id: 'hero', label: 'Баннер', icon: '' },
  { id: 'services', label: 'Услуги', icon: '' },
  { id: 'portfolio', label: 'Портфолио', icon: '' },
  { id: 'gallery', label: 'Галерея', icon: '' },
]

interface ServiceItem {
  name: string
  summary: string
  priceFrom: string
  subcategory?: string
  photo?: string
  addons?: AddonItem[]
}

interface AddonItem {
  name: string
  price: string
}

const defaultServiceCategories = [
  { id: 'main', label: 'Основная услуга' },
  { id: 'additional', label: 'Дополнительная' },
  { id: 'premium', label: 'Премиум' },
  { id: 'consultation', label: 'Консультация' },
  { id: 'custom', label: 'Другое' },
]

interface PhotoItem {
  file: File
  preview: string
  category: string
  serviceIndex?: number
}

interface DraftFormData {
  company: string
  business_type: string
  summary: string
  client_name: string
  client_company: string
  client_contact: string
  phone: string
  email: string
  address: string
  work_hours: string
  services: ServiceItem[]
  color_palette: string
  tariff: string
  currentStep: number
  savedAt: number
}

interface FormData {
  company: string
  business_type: string
  summary: string
  client_name: string
  client_company: string
  client_contact: string
  phone: string
  email: string
  address: string
  work_hours: string
  services: ServiceItem[]
  photos: PhotoItem[]
  color_palette: string
  tariff: string
}

interface ValidationErrors {
  company?: string
  business_type?: string
  client_name?: string
  phone?: string
  email?: string
  services?: string
}

const getInitialFormData = (): FormData => ({
  company: '',
  business_type: '',
  summary: '',
  client_name: '',
  client_company: '',
  client_contact: '',
  phone: '',
  email: '',
  address: '',
  work_hours: '',
  services: [{ name: '', summary: '', priceFrom: '', addons: [] }],
  photos: [],
  color_palette: 'На усмотрение дизайнера',
  tariff: 'standard',
})

const saveDraft = (formData: FormData, currentStep: number): void => {
  try {
    const draftData: DraftFormData = {
      company: formData.company,
      business_type: formData.business_type,
      summary: formData.summary,
      client_name: formData.client_name,
      client_company: formData.client_company,
      client_contact: formData.client_contact,
      phone: formData.phone,
      email: formData.email,
      address: formData.address,
      work_hours: formData.work_hours,
      services: formData.services,
      color_palette: formData.color_palette,
      tariff: formData.tariff,
      currentStep,
      savedAt: Date.now(),
    }
    localStorage.setItem(DRAFT_STORAGE_KEY, JSON.stringify(draftData))
  } catch (e) {
    console.error('Failed to save draft:', e)
  }
}

const loadDraft = (): { formData: Partial<FormData>; currentStep: number } | null => {
  try {
    const saved = localStorage.getItem(DRAFT_STORAGE_KEY)
    if (!saved) return null

    const draft: DraftFormData = JSON.parse(saved)

    const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000
    if (draft.savedAt < sevenDaysAgo) {
      localStorage.removeItem(DRAFT_STORAGE_KEY)
      return null
    }

    const hasContent = draft.company || draft.client_name ||
      draft.services.some(s => s.name.trim())

    if (!hasContent) {
      localStorage.removeItem(DRAFT_STORAGE_KEY)
      return null
    }

    return {
      formData: {
        company: draft.company,
        business_type: draft.business_type,
        summary: draft.summary,
        client_name: draft.client_name,
        client_company: draft.client_company,
        client_contact: draft.client_contact,
        phone: draft.phone,
        email: draft.email,
        address: draft.address,
        work_hours: draft.work_hours,
        services: draft.services,
        color_palette: draft.color_palette,
        tariff: draft.tariff || 'standard',
      },
      currentStep: draft.currentStep,
    }
  } catch (e) {
    console.error('Failed to load draft:', e)
    return null
  }
}

const clearDraft = (): void => {
  try {
    localStorage.removeItem(DRAFT_STORAGE_KEY)
  } catch (e) {
    console.error('Failed to clear draft:', e)
  }
}

export default function NewRequestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [currentStep, setCurrentStep] = useState(0)
  const [selectedCategory, setSelectedCategory] = useState('hero')
  const [selectedServiceIndexForPhoto, setSelectedServiceIndexForPhoto] = useState<number | null>(null)
  const [uploadingPhotos, setUploadingPhotos] = useState(false)
  const [errors, setErrors] = useState<ValidationErrors>({})
  const [formData, setFormData] = useState<FormData>(getInitialFormData())
  const [showDraftPrompt, setShowDraftPrompt] = useState(false)
  const [draftData, setDraftData] = useState<{ formData: Partial<FormData>; currentStep: number } | null>(null)

  useEffect(() => {
    const draft = loadDraft()
    if (draft) {
      setDraftData(draft)
      setShowDraftPrompt(true)
    }
  }, [])

  const handleRestoreDraft = useCallback(() => {
    if (draftData) {
      setFormData(prev => ({
        ...prev,
        ...draftData.formData,
        photos: [],
      }))
      setCurrentStep(draftData.currentStep)
      setShowDraftPrompt(false)
      setDraftData(null)
      toast.success('Черновик восстановлен')
      haptic?.notificationOccurred('success')
    }
  }, [draftData, haptic])

  const handleDiscardDraft = useCallback(() => {
    clearDraft()
    setShowDraftPrompt(false)
    setDraftData(null)
  }, [])

  useEffect(() => {
    const hasContent = formData.company || formData.client_name ||
      formData.services.some(s => s.name.trim())

    if (hasContent) {
      const timeoutId = setTimeout(() => {
        saveDraft(formData, currentStep)
      }, 1000)
      return () => clearTimeout(timeoutId)
    }
  }, [formData, currentStep])

  const createMutation = useMutation({
    mutationFn: async () => {
      const payload = {
        client: {
          name: formData.client_name,
          company: formData.client_company,
          contact: formData.client_contact,
        },
        site: {
          company: formData.company,
          business_type: formData.business_type,
          summary: formData.summary,
          phone: formData.phone,
          email: formData.email,
          address: formData.address,
          work_hours: formData.work_hours,
          services: formData.services.filter(s => s.name.trim()),
          color_palette: formData.color_palette,
          structure: DEFAULT_STRUCTURE,
          meta: { status: 'draft' }
        }
      }

      const response = await requestsApi.create({
        company_name: formData.company,
        client_name: formData.client_name,
        payload,
        tariff: formData.tariff,
      })

      const requestId = response.data.id

      if (formData.photos.length > 0) {
        setUploadingPhotos(true)
        for (const photo of formData.photos) {
          try {
            const photoFormData = new FormData()
            photoFormData.append('file', photo.file)
            photoFormData.append('category', photo.category)
            if (typeof photo.serviceIndex === 'number') {
              const service = formData.services[photo.serviceIndex]
              if (service?.name) {
                photoFormData.append('service_name', service.name)
                photoFormData.append('service_index', String(photo.serviceIndex))
              }
            }
            await requestsApi.uploadPhotos(requestId, photoFormData)
          } catch (e) {
            console.error('Failed to upload photo:', e)
          }
        }
        setUploadingPhotos(false)
      }

      return response.data
    },
    onSuccess: (data) => {
      clearDraft()
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('Заявка создана!')
      haptic?.notificationOccurred('success')
      navigate(`/requests/${data.id}`)
    },
    onError: () => {
      toast.error('Ошибка создания')
      haptic?.notificationOccurred('error')
      setUploadingPhotos(false)
    },
  })

  const updateField = <K extends keyof FormData>(field: K, value: FormData[K]) => {
    setFormData(prev => ({ ...prev, [field]: value }))
    if (errors[field as keyof ValidationErrors]) {
      setErrors(prev => ({ ...prev, [field]: undefined }))
    }
  }

  const addService = () => {
    setFormData(prev => ({
      ...prev,
      services: [...prev.services, { name: '', summary: '', priceFrom: '', addons: [] }],
    }))
  }

  const updateService = (index: number, field: keyof ServiceItem, value: string) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.map((s, i) => {
        if (i !== index) return s
        return { ...s, [field]: value }
      }),
    }))
    if (errors.services) {
      setErrors(prev => ({ ...prev, services: undefined }))
    }
  }

  const removeService = (index: number) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.filter((_, i) => i !== index),
    }))
  }

  const handlePhotoSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files
    if (!files) return

    addPhotosFromFileList(files)

    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  const addPhotosFromFileList = (files: FileList | File[]) => {
    const newPhotos: PhotoItem[] = []
    for (let i = 0; i < files.length; i++) {
      const file = (files as FileList)[i] ?? (files as File[])[i]
      if (file && file.type.startsWith('image/')) {
        newPhotos.push({
          file,
          preview: URL.createObjectURL(file),
          category: selectedCategory,
          serviceIndex: selectedServiceIndexForPhoto ?? undefined,
        })
      }
    }

    if (newPhotos.length === 0) return

    setFormData(prev => ({
      ...prev,
      photos: [...prev.photos, ...newPhotos],
    }))
  }

  const removePhoto = (index: number) => {
    setFormData(prev => {
      const newPhotos = [...prev.photos]
      URL.revokeObjectURL(newPhotos[index].preview)
      newPhotos.splice(index, 1)
      return { ...prev, photos: newPhotos }
    })
  }

  const updatePhotoCategory = (index: number, category: string) => {
    setFormData(prev => ({
      ...prev,
      photos: prev.photos.map((photo, i) =>
        i === index ? { ...photo, category } : photo
      )
    }))
  }

  const formatPhone = (value: string): string => {
    const digits = value.replace(/\D/g, '')
    const limited = digits.slice(0, 11)

    if (!limited) return ''

    let formatted = ''
    if (limited.length > 0) {
      const startDigit = limited[0] === '8' ? '7' : limited[0]
      formatted = '+' + startDigit
    }
    if (limited.length > 1) formatted += ' (' + limited.slice(1, 4)
    if (limited.length >= 4) formatted += ')'
    if (limited.length > 4) formatted += ' ' + limited.slice(4, 7)
    if (limited.length > 7) formatted += '-' + limited.slice(7, 9)
    if (limited.length > 9) formatted += '-' + limited.slice(9, 11)

    return formatted
  }

  const handlePhoneChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    updateField('phone', formatPhone(e.target.value))
  }

  const validatePhone = (phone: string): { valid: boolean; error?: string } => {
    if (!phone || !phone.trim()) {
      return { valid: false, error: 'Поле обязательно для заполнения' }
    }
    const digits = phone.replace(/\D/g, '')
    if (digits.length < 11) {
      return { valid: false, error: `Введите полный номер (ещё ${11 - digits.length} цифр)` }
    }
    if (digits.length > 11) {
      return { valid: false, error: 'Слишком много цифр в номере' }
    }
    if (!digits.startsWith('7') && !digits.startsWith('8')) {
      return { valid: false, error: 'Номер должен начинаться с +7 или 8' }
    }
    return { valid: true }
  }

  const validateEmail = (email: string): { valid: boolean; error?: string } => {
    if (!email || !email.trim()) {
      return { valid: false, error: 'Поле обязательно для заполнения' }
    }
    if (!email.includes('@')) {
      return { valid: false, error: 'Email должен содержать @' }
    }
    const emailRegex = /^[^\s@]+@[^\s@]+\.[a-zA-Z]{2,}$/
    if (!emailRegex.test(email)) {
      return { valid: false, error: 'Введите корректный email (например: example@mail.ru)' }
    }
    return { valid: true }
  }

  const validateStep = (): boolean => {
    const newErrors: ValidationErrors = {}

    switch (currentStep) {
      case 0:
        if (!formData.company.trim()) newErrors.company = 'Поле обязательно для заполнения'
        if (!formData.business_type.trim()) newErrors.business_type = 'Поле обязательно для заполнения'
        break
      case 1:
        if (!formData.client_name.trim()) newErrors.client_name = 'Поле обязательно для заполнения'
        break
      case 2: {
        const phoneValidation = validatePhone(formData.phone)
        if (!phoneValidation.valid) newErrors.phone = phoneValidation.error

        const emailValidation = validateEmail(formData.email)
        if (!emailValidation.valid) newErrors.email = emailValidation.error
        break
      }
      case 3:
        if (!formData.services.some(s => s.name.trim())) newErrors.services = 'Добавьте хотя бы одну услугу'
        break
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const canGoNext = () => {
    switch (currentStep) {
      case 0: return formData.company.trim() && formData.business_type.trim()
      case 1: return formData.client_name.trim()
      case 2: return validatePhone(formData.phone).valid && validateEmail(formData.email).valid
      case 3: return formData.services.some(s => s.name.trim())
      default: return true
    }
  }

  const goNext = () => {
    if (!validateStep()) {
      haptic?.notificationOccurred('error')
      return
    }

    if (currentStep < steps.length - 1) {
      haptic?.impactOccurred('light')
      setCurrentStep(prev => prev + 1)
    } else {
      createMutation.mutate()
    }
  }

  const goBack = () => {
    if (currentStep > 0) {
      haptic?.impactOccurred('light')
      setCurrentStep(prev => prev - 1)
    }
  }

  const getPhotosByCategory = (category: string) => {
    return formData.photos.filter(p => p.category === category)
  }

  // Paste handler for images (Ctrl+V)
  useEffect(() => {
    const handlePaste = (event: ClipboardEvent) => {
      if (currentStep !== 4) return
      const items = event.clipboardData?.items
      if (!items) return

      const files: File[] = []
      for (let i = 0; i < items.length; i++) {
        const item = items[i]
        if (item.type.startsWith('image/')) {
          const file = item.getAsFile()
          if (file) files.push(file)
        }
      }

      if (files.length > 0) {
        event.preventDefault()
        addPhotosFromFileList(files)
      }
    }

    window.addEventListener('paste', handlePaste)
    return () => window.removeEventListener('paste', handlePaste)
  }, [currentStep, selectedCategory, selectedServiceIndexForPhoto])

  return (
    <div className="min-h-screen flex flex-col bg-tg-bg">
      {/* Draft Restoration Prompt */}
      <AnimatePresence>
        {showDraftPrompt && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-50"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={handleDiscardDraft}
            />
            <motion.div
              className="fixed inset-0 flex items-center justify-center p-4 z-[60]"
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="bg-white dark:bg-zinc-900 rounded-3xl p-6 w-full max-w-sm shadow-2xl border border-zinc-200 dark:border-zinc-800">
                <div className="text-center mb-6">
                  <div className="w-16 h-16 bg-blue-100 dark:bg-blue-900/30 rounded-full flex items-center justify-center mx-auto mb-4">
                    <RotateCcw className="w-8 h-8 text-blue-500" />
                  </div>
                  <h3 className="text-xl font-bold text-tg-text mb-2">Найден черновик</h3>
                  <p className="text-tg-hint text-sm leading-relaxed mb-3">
                    У вас есть незаконченная заявка. Хотите продолжить заполнение?
                  </p>
                  {draftData?.formData.company && (
                    <p className="text-sm text-tg-text font-medium">
                      "{draftData.formData.company}"
                    </p>
                  )}
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={handleDiscardDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-zinc-100 dark:bg-zinc-800 text-tg-text font-semibold hover:bg-zinc-200 dark:hover:bg-zinc-700 transition-colors"
                  >
                    Начать заново
                  </button>
                  <button
                    onClick={handleRestoreDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-blue-500 text-white font-semibold hover:bg-blue-600 transition-colors"
                  >
                    Продолжить
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg/80 backdrop-blur-xl border-b border-tg-separator px-4 py-3">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-lg font-bold text-tg-text">Новая заявка</h1>
            <p className="text-xs text-tg-hint">{steps[currentStep].title}</p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-tg-text">{currentStep + 1}</span>
            <span className="text-sm text-tg-hint">/ {steps.length}</span>
          </div>
        </div>
        <div className="flex gap-1.5">
          {steps.map((step, i) => (
            <div
              key={i}
              className={clsx(
                'flex-1 h-1.5 rounded-full transition-all duration-300',
                i < currentStep
                  ? 'bg-blue-500'
                  : i === currentStep
                    ? 'bg-sky-400'
                    : 'bg-zinc-700'
              )}
            />
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4">
        <AnimatePresence mode="wait">
          <motion.div
            key={currentStep}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            transition={{ duration: 0.2 }}
          >
            {currentStep === 0 && (
              <div className="space-y-4">
                {/* Tariff Selection */}
                <div>
                  <label className="text-sm text-tg-hint mb-2 block">Тариф генерации</label>
                  <div className="space-y-3">
                    {/* Standard tariff */}
                    <button
                      type="button"
                      onClick={() => updateField('tariff', 'standard')}
                      className={clsx(
                        'w-full p-4 rounded-2xl border-2 transition-all text-left',
                        formData.tariff === 'standard'
                          ? 'border-blue-500 bg-blue-500/10'
                          : 'border-zinc-200 dark:border-zinc-700'
                      )}
                    >
                      <div className="flex items-center gap-3">
                        <div className={clsx(
                          'w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0',
                          formData.tariff === 'standard' ? 'border-blue-500' : 'border-zinc-300 dark:border-zinc-600'
                        )}>
                          {formData.tariff === 'standard' && (
                            <div className="w-2.5 h-2.5 rounded-full bg-blue-500" />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2">
                            <span className="font-semibold text-tg-text">Стандарт</span>
                    <span className="text-sm font-bold text-sky-400 flex-shrink-0">Бесплатно</span>
                          </div>
                          <p className="text-xs text-tg-hint mt-0.5">Базовая генерация лендинга</p>
                        </div>
                      </div>
                    </button>

                    {/* Premium tariff */}
                    <button
                      type="button"
                      onClick={() => updateField('tariff', 'premium')}
                      className={clsx(
                        'w-full p-4 rounded-2xl border-2 transition-all text-left relative overflow-hidden',
                        formData.tariff === 'premium'
                          ? 'border-purple-500 bg-gradient-to-br from-purple-500/20 via-purple-500/10 to-blue-500/10 shadow-lg shadow-purple-500/20'
                          : 'border-zinc-200 dark:border-zinc-700 hover:border-purple-300 dark:hover:border-purple-600'
                      )}
                    >
                      {/* Shine effect when selected */}
                      {formData.tariff === 'premium' && (
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent pointer-events-none" style={{
                          backgroundSize: '200% 100%',
                          animation: 'shimmer 2s ease-in-out infinite'
                        }} />
                      )}

                      <div className="flex items-center gap-3 relative z-10">
                        <div className={clsx(
                          'w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 transition-all',
                          formData.tariff === 'premium'
                            ? 'border-purple-500 bg-purple-500/20'
                            : 'border-zinc-300 dark:border-zinc-600'
                        )}>
                          {formData.tariff === 'premium' && (
                            <div className="w-2.5 h-2.5 rounded-full bg-gradient-to-r from-purple-500 to-blue-500" />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2">
                            <span className={clsx(
                              'font-semibold',
                              formData.tariff === 'premium'
                                ? 'bg-gradient-to-r from-purple-600 to-blue-600 bg-clip-text text-transparent'
                                : 'text-tg-text'
                            )}>
                              Премиум
                            </span>
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-gradient-to-r from-purple-500 to-blue-500 text-white flex-shrink-0">
                              PRO
                            </span>
                          </div>
                          <p className="text-xs text-tg-hint mt-0.5">Профессиональный дизайн и качество</p>
                        </div>
                      </div>
                    </button>
                  </div>
                </div>

                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Название компании *</label>
                  <input
                    type="text"
                    value={formData.company}
                    onChange={(e) => updateField('company', e.target.value)}
                    placeholder="Webly"
                    className={clsx('input', errors.company && 'input-error field-error-shake')}
                  />
                  {errors.company && (
                    <p className="error-message">
                      <AlertCircle className="w-3 h-3" />
                      {errors.company}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Сфера деятельности *</label>
                  <input
                    type="text"
                    value={formData.business_type}
                    onChange={(e) => updateField('business_type', e.target.value)}
                    placeholder="Создание сайтов"
                    className={clsx('input', errors.business_type && 'input-error field-error-shake')}
                  />
                  {errors.business_type && (
                    <p className="error-message">
                      <AlertCircle className="w-3 h-3" />
                      {errors.business_type}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Описание</label>
                  <textarea
                    value={formData.summary}
                    onChange={(e) => updateField('summary', e.target.value)}
                    placeholder="О компании..."
                    className="input min-h-[100px] resize-none"
                  />
                </div>
              </div>
            )}

            {currentStep === 1 && (
              <div className="space-y-4">
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">ФИО клиента *</label>
                  <input
                    type="text"
                    value={formData.client_name}
                    onChange={(e) => updateField('client_name', e.target.value)}
                    placeholder="Иванов Иван"
                    className={clsx('input', errors.client_name && 'input-error field-error-shake')}
                    autoFocus
                  />
                  {errors.client_name && (
                    <p className="error-message">
                      <AlertCircle className="w-3 h-3" />
                      {errors.client_name}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Компания</label>
                  <input
                    type="text"
                    value={formData.client_company}
                    onChange={(e) => updateField('client_company', e.target.value)}
                    placeholder="ООО «Компания»"
                    className="input"
                  />
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Контакт</label>
                  <input
                    type="text"
                    value={formData.client_contact}
                    onChange={(e) => updateField('client_contact', e.target.value)}
                    placeholder="+7... или @telegram"
                    className="input"
                  />
                </div>
              </div>
            )}

            {currentStep === 2 && (
              <div className="space-y-4">
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Телефон для сайта *</label>
                  <input
                    type="tel"
                    inputMode="numeric"
                    value={formData.phone}
                    onChange={handlePhoneChange}
                    placeholder="+7 (XXX) XXX-XX-XX"
                    maxLength={18}
                    className={clsx('input', errors.phone && 'input-error field-error-shake')}
                    autoFocus
                  />
                  {errors.phone && (
                    <p className="error-message">
                      <AlertCircle className="w-3 h-3" />
                      {errors.phone}
                    </p>
                  )}
                  <p className="text-xs text-tg-hint/70 mt-1">Формат: +7 (XXX) XXX-XX-XX</p>
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Email *</label>
                  <input
                    type="email"
                    value={formData.email}
                    onChange={(e) => updateField('email', e.target.value)}
                    placeholder="info@company.ru"
                    className={clsx('input', errors.email && 'input-error field-error-shake')}
                  />
                  {errors.email && (
                    <p className="error-message">
                      <AlertCircle className="w-3 h-3" />
                      {errors.email}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Адрес</label>
                  <input
                    type="text"
                    value={formData.address}
                    onChange={(e) => updateField('address', e.target.value)}
                    placeholder="г. Москва, ул..."
                    className="input"
                  />
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Часы работы</label>
                  <input
                    type="text"
                    value={formData.work_hours}
                    onChange={(e) => updateField('work_hours', e.target.value)}
                    placeholder="Пн-Пт 9:00-18:00"
                    className="input"
                  />
                </div>
              </div>
            )}

            {currentStep === 3 && (
              <div className="space-y-4">
                {errors.services && (
                  <p className="text-xs text-red-500 p-3 bg-red-50 dark:bg-red-900/20 rounded-xl flex items-center gap-2">
                    <AlertCircle className="w-4 h-4" />
                    {errors.services}
                  </p>
                )}
                {formData.services.map((service, i) => (
                  <div key={i} className="bg-tg-secondary-bg rounded-xl p-4 space-y-3">
                    <div className="flex justify-between mb-3">
                      <span className="text-sm font-medium">Услуга/Товар {i + 1}</span>
                      {formData.services.length > 1 && (
                        <button onClick={() => removeService(i)} className="text-red-500">
                          <X className="w-4 h-4" />
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

                      {/* Subcategory selector */}
                      <div>
                        <label className="text-xs text-tg-hint mb-1 block">Категория услуги</label>
                        <div className="flex gap-1.5 flex-wrap">
                          {defaultServiceCategories.map(cat => (
                            <button
                              key={cat.id}
                              type="button"
                              onClick={() => updateService(i, 'subcategory', cat.id)}
                              className={clsx(
                                'px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all',
                                service.subcategory === cat.id
                                  ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900'
                                  : 'bg-zinc-100 dark:bg-zinc-700 text-tg-text hover:bg-zinc-200 dark:hover:bg-zinc-600'
                              )}
                            >
                              {cat.label}
                            </button>
                          ))}
                        </div>
                      </div>

                      <textarea
                        value={service.summary}
                        onChange={(e) => updateService(i, 'summary', e.target.value)}
                        placeholder="Описание услуги"
                        className="input min-h-[60px] resize-none"
                      />
                      <input
                        value={service.priceFrom}
                        onChange={(e) => updateService(i, 'priceFrom', e.target.value)}
                        placeholder="Цена (например: 1500 или от 10 000 ₽)"
                        className="input"
                      />
                      {/* Add-ons for this service */}
                      <div className="mt-3 space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-semibold text-tg-hint">Доп. услуги / опции</span>
                        </div>
                        {service.addons?.length ? (
                          <div className="space-y-2">
                            {service.addons.map((addon, addonIndex) => (
                              <div
                                key={addonIndex}
                                className="flex items-center gap-2"
                              >
                                <input
                                  value={addon.name}
                                  onChange={(e) => {
                                    const name = e.target.value
                                    setFormData(prev => ({
                                      ...prev,
                                      services: prev.services.map((s, si) => {
                                        if (si !== i) return s
                                        const addons = s.addons ?? []
                                        const updated = addons.map((a, ai) =>
                                          ai === addonIndex ? { ...a, name } : a
                                        )
                                        return { ...s, addons: updated }
                                      })
                                    }))
                                  }}
                                  placeholder="Название доп. услуги"
                                  className="input flex-1"
                                />
                                <input
                                  value={addon.price}
                                  onChange={(e) => {
                                    const price = e.target.value
                                    setFormData(prev => ({
                                      ...prev,
                                      services: prev.services.map((s, si) => {
                                        if (si !== i) return s
                                        const addons = s.addons ?? []
                                        const updated = addons.map((a, ai) =>
                                          ai === addonIndex ? { ...a, price } : a
                                        )
                                        return { ...s, addons: updated }
                                      })
                                    }))
                                  }}
                                  placeholder="+100"
                                  className="input w-28"
                                />
                                <button
                                  type="button"
                                  onClick={() => {
                                    setFormData(prev => ({
                                      ...prev,
                                      services: prev.services.map((s, si) => {
                                        if (si !== i) return s
                                        const addons = s.addons ?? []
                                        return {
                                          ...s,
                                          addons: addons.filter((_, ai) => ai !== addonIndex)
                                        }
                                      })
                                    }))
                                  }}
                                  className="p-2 text-red-500"
                                >
                                  <X className="w-4 h-4" />
                                </button>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <p className="text-xs text-tg-hint">
                            Добавьте доп. услуги/опции, которые увеличивают стоимость основной услуги.
                          </p>
                        )}
                        <button
                          type="button"
                          onClick={() => {
                            setFormData(prev => ({
                              ...prev,
                              services: prev.services.map((s, si) => {
                                if (si !== i) return s
                                const addons = s.addons ?? []
                                return {
                                  ...s,
                                  addons: [...addons, { name: '', price: '' }]
                                }
                              })
                            }))
                          }}
                          className="text-xs text-tg-link mt-1"
                        >
                          + Добавить доп. услугу
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
                <button onClick={addService} className="btn btn-secondary w-full">
                  <Plus className="w-5 h-5" /> Добавить услугу
                </button>
              </div>
            )}

            {currentStep === 4 && (
              <div className="space-y-4">
                {/* Bind photos to specific service/product */}
                <div>
                  <label className="text-sm text-tg-hint mb-2 block">Привязать фото к услуге/товару (опционально)</label>
                  <div className="flex gap-2 overflow-x-auto scroll-x-container pb-1">
                    <button
                      type="button"
                      onClick={() => setSelectedServiceIndexForPhoto(null)}
                      className={clsx(
                        'px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap',
                        selectedServiceIndexForPhoto === null
                          ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900'
                          : 'bg-tg-secondary-bg text-tg-text'
                      )}
                    >
                      Без привязки
                    </button>
                    {formData.services.map((s, i) => (
                      <button
                        key={i}
                        type="button"
                        onClick={() => setSelectedServiceIndexForPhoto(i)}
                        className={clsx(
                          'px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap',
                          selectedServiceIndexForPhoto === i
                            ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900'
                            : 'bg-tg-secondary-bg text-tg-text'
                        )}
                      >
                        {s.name || `Услуга/Товар ${i + 1}`}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Category selector with improved visibility */}
                <div>
                  <label className="text-sm text-tg-hint mb-2 block">Выберите категорию для новых фото:</label>
                  <div className="flex gap-2 overflow-x-auto pb-2 scroll-x-container">
                    {photoCategories.map(cat => (
                      <button
                        key={cat.id}
                        onClick={() => {
                          setSelectedCategory(cat.id)
                          haptic?.selectionChanged()
                        }}
                        className={clsx(
                          'px-4 py-2.5 rounded-xl text-sm font-medium whitespace-nowrap transition-all',
                          selectedCategory === cat.id
                            ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900 shadow-lg'
                            : 'bg-tg-secondary-bg text-tg-text hover:bg-zinc-200 dark:hover:bg-zinc-700'
                        )}
                      >
                        <span>{cat.label}</span>
                        {getPhotosByCategory(cat.id).length > 0 && (
                          <span className="ml-1.5 px-1.5 py-0.5 bg-white/20 dark:bg-black/20 rounded-md text-xs">
                            {getPhotosByCategory(cat.id).length}
                          </span>
                        )}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Photo upload button with label for better mobile support + drag & drop */}
                <label
                  className="block w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint hover:border-tg-accent hover:text-tg-accent transition-colors cursor-pointer active:bg-tg-secondary-bg"
                  onDragOver={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                  }}
                  onDrop={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
                      addPhotosFromFileList(e.dataTransfer.files)
                      e.dataTransfer.clearData()
                    }
                  }}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    multiple
                    onChange={handlePhotoSelect}
                    className="sr-only"
                  />
                  <Upload className="w-5 h-5 mx-auto mb-2" />
                  <span className="block text-center">Добавить фото в «{photoCategories.find(c => c.id === selectedCategory)?.label}»</span>
                  <span className="block text-center text-[11px] text-tg-hint mt-1">
                    Можно перетащить файлы сюда или вставить из буфера (Ctrl+V)
                  </span>
                </label>

                {/* Photos grid with category labels */}
                {formData.photos.length > 0 && (
                  <div className="space-y-3">
                    <label className="text-sm text-tg-hint block">Загруженные фото (нажмите для смены категории):</label>
                    <div className="grid grid-cols-3 gap-2">
                      {formData.photos.map((photo, i) => (
                        <div key={i} className="relative group">
                          <img
                            src={photo.preview}
                            alt=""
                            className="w-full aspect-square rounded-xl object-cover"
                          />
                          {/* Category badge - clickable to change */}
                          <button
                            onClick={() => {
                              const currentIndex = photoCategories.findIndex(c => c.id === photo.category)
                              const nextIndex = (currentIndex + 1) % photoCategories.length
                              updatePhotoCategory(i, photoCategories[nextIndex].id)
                              haptic?.selectionChanged()
                            }}
                            className="absolute bottom-1 left-1 right-1 px-2 py-1 bg-black/70 backdrop-blur-sm text-white text-xs rounded-lg truncate text-center"
                          >
                            {photoCategories.find(c => c.id === photo.category)?.icon}{' '}
                            {photoCategories.find(c => c.id === photo.category)?.label}
                          </button>
                          {/* Delete button */}
                          <button
                            onClick={() => removePhoto(i)}
                            className="absolute -top-2 -right-2 w-6 h-6 bg-red-500 text-white rounded-full flex items-center justify-center shadow-lg"
                          >
                            <X className="w-3 h-3" />
                          </button>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {formData.photos.length === 0 && (
                  <p className="text-center text-tg-hint py-4">Фото можно добавить позже</p>
                )}
              </div>
            )}

            {currentStep === 5 && (
              <div className="space-y-4">
                <div>
                  <label className="text-sm text-tg-hint mb-2 block">Структура сайта</label>
                  <div className="flex flex-wrap gap-2">
                    {DEFAULT_STRUCTURE.map(s => (
                      <span key={s} className="px-3 py-1.5 bg-tg-secondary-bg rounded-lg text-sm">{s}</span>
                    ))}
                  </div>
                </div>

                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Цветовая палитра</label>
                  <input
                    type="text"
                    value={formData.color_palette}
                    onChange={(e) => updateField('color_palette', e.target.value)}
                    placeholder="синий и белый"
                    className="input"
                  />
                </div>

                <div className="bg-tg-secondary-bg rounded-xl p-4">
                  <p className="font-medium mb-2">Итого</p>
                  <div className="space-y-1 text-sm text-tg-hint">
                    <p>Компания: <span className="text-tg-text">{formData.company || '—'}</span></p>
                    <p>Клиент: <span className="text-tg-text">{formData.client_name || '—'}</span></p>
                    <p>Услуга/Товаров: <span className="text-tg-text">{formData.services.filter(s => s.name.trim()).length}</span></p>
                    <p>Фото: <span className="text-tg-text">{formData.photos.length}</span></p>
                  </div>

                  {/* Price summary with add-ons */}
                  {(() => {
                    const parsePrice = (value: string): number => {
                      if (!value) return 0
                      const cleaned = value.replace(/[^\d]/g, '')
                      const num = parseInt(cleaned || '0', 10)
                      return isNaN(num) ? 0 : num
                    }

                    const servicesTotal = formData.services.reduce((sum, s) => {
                      const base = parsePrice(s.priceFrom)
                      const addonsTotal = (s.addons ?? []).reduce((aSum, a) => aSum + parsePrice(a.price), 0)
                      return sum + base + addonsTotal
                    }, 0)

                    if (!servicesTotal) return null

                    return (
                      <div className="mt-3 pt-3 border-t border-tg-separator space-y-1 text-sm">
                        <p className="flex items-center justify-between">
                          <span className="text-tg-hint">Оценочная стоимость с допами</span>
                          <span className="font-semibold text-tg-text">{servicesTotal.toLocaleString('ru-RU')} ₽</span>
                        </p>
                      </div>
                    )
                  })()}
                </div>
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Bottom Actions */}
      <div className="sticky bottom-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        {/* Show validation hint when button is disabled */}
        {!canGoNext() && Object.keys(errors).length === 0 && (
          <p className="text-xs text-amber-500 text-center mb-2 flex items-center justify-center gap-1">
            <AlertCircle className="w-3 h-3" />
            Заполните обязательные поля для продолжения
          </p>
        )}
        <div className="flex gap-3">
          {currentStep > 0 && (
            <Tooltip content="Вернуться назад" position="top">
              <button onClick={goBack} className="btn btn-secondary">
                <ArrowLeft className="w-5 h-5" />
              </button>
            </Tooltip>
          )}
          <Tooltip
            content={
              createMutation.isPending
                ? 'Подождите...'
                : currentStep === steps.length - 1
                  ? 'Создать заявку'
                  : 'Перейти к следующему шагу'
            }
            position="top"
          >
            <button
              onClick={goNext}
              disabled={createMutation.isPending}
              className={clsx(
                "btn btn-primary flex-1",
                !canGoNext() && !createMutation.isPending && "opacity-50"
              )}
            >
              {createMutation.isPending ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  {uploadingPhotos ? 'Загрузка...' : 'Создание...'}
                </>
              ) : currentStep === steps.length - 1 ? (
                <>
                  <Check className="w-5 h-5" /> Создать
                </>
              ) : (
                <>
                  Далее <ArrowRight className="w-5 h-5" />
                </>
              )}
            </button>
          </Tooltip>
        </div>
      </div>
    </div>
  )
}
