import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Briefcase, Phone, Mail, MapPin,
  ArrowRight, ArrowLeft, Check, Loader2, Plus, X,
  Clock, Palette, Camera, Upload, Image, AlertCircle,
  RotateCcw, Info
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
  { id: 'services', label: 'Услуги/Товары', icon: '' },
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
    <div className="min-h-screen flex flex-col" style={{ background: '#0F1115' }}>
      {/* Draft Restoration Prompt */}
      <AnimatePresence>
        {showDraftPrompt && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/70 z-50"
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
              <div className="rounded-3xl p-6 w-full max-w-sm shadow-2xl border border-blue-500/20" style={{ background: 'linear-gradient(145deg, #0f172a 0%, #0d1424 100%)' }}>
                <div className="text-center mb-6">
                  <div className="w-16 h-16 bg-blue-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                    <RotateCcw className="w-8 h-8 text-blue-400" />
                  </div>
                  <h3 className="text-xl font-bold text-slate-100 mb-2">Найден черновик</h3>
                  <p className="text-slate-400 text-sm leading-relaxed mb-3">
                    У вас есть незаконченная заявка. Хотите продолжить заполнение?
                  </p>
                  {draftData?.formData.company && (
                    <p className="text-sm text-blue-300 font-medium">
                      "{draftData.formData.company}"
                    </p>
                  )}
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={handleDiscardDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl font-semibold transition-colors border border-slate-600/50 text-slate-300 hover:bg-slate-700/50"
                    style={{ background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)' }}
                  >
                    Начать заново
                  </button>
                  <button
                    onClick={handleRestoreDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl text-white font-semibold transition-colors border border-blue-500/30"
                    style={{ background: 'linear-gradient(145deg, #2563eb 0%, #1d4ed8 100%)' }}
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
      <div className="sticky top-0 z-10 backdrop-blur-xl border-b px-4 py-3" style={{
        background: 'rgba(15, 20, 25, 0.95)',
        borderColor: 'rgba(100, 116, 139, 0.15)'
      }}>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-lg font-bold text-slate-100">Новая заявка</h1>
            <p className="text-xs text-slate-400">{steps[currentStep].title}</p>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-full border" style={{
            background: 'rgba(100, 116, 139, 0.1)',
            borderColor: 'rgba(148, 163, 184, 0.2)'
          }}>
            <span className="text-sm font-semibold text-slate-200">{currentStep + 1}</span>
            <span className="text-sm text-slate-400">/ {steps.length}</span>
          </div>
        </div>
        <div className="flex gap-1.5">
          {steps.map((step, i) => (
            <div
              key={i}
              className={clsx(
                'flex-1 h-1.5 rounded-full transition-all duration-300',
                i < currentStep
                  ? 'bg-slate-500'
                  : i === currentStep
                    ? 'bg-slate-400'
                    : 'bg-slate-700'
              )}
            />
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4 pb-6">
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
                        'w-full p-4 rounded-2xl border transition-all text-left',
                        formData.tariff === 'standard'
                          ? 'border-slate-500'
                          : 'border-slate-700/50'
                      )}
                      style={{
                        background: formData.tariff === 'standard'
                          ? 'linear-gradient(145deg, #334155 0%, #1e293b 100%)'
                          : 'linear-gradient(145deg, #1e293b 0%, #1a1f2e 100%)'
                      }}
                    >
                      <div className="flex items-start gap-3">
                        <div className={clsx(
                          'w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 mt-0.5',
                          formData.tariff === 'standard' ? 'border-slate-300' : 'border-slate-600'
                        )}>
                          {formData.tariff === 'standard' && (
                            <div className="w-2.5 h-2.5 rounded-full bg-slate-300" />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="font-semibold text-slate-100">Стандарт</span>
                          </div>
                          <p className="text-xs text-slate-400 mt-1">Базовая генерация лендинга</p>
                        </div>
                      </div>
                    </button>

                    {/* Premium tariff */}
                    <button
                      type="button"
                      onClick={() => updateField('tariff', 'premium')}
                      className={clsx(
                        'w-full p-4 rounded-2xl border transition-all text-left relative overflow-hidden',
                        formData.tariff === 'premium'
                          ? 'border-purple-500/40'
                          : 'border-slate-700/50'
                      )}
                      style={formData.tariff === 'premium' ? {
                        background: 'linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(59, 130, 246, 0.15) 50%, rgba(139, 92, 246, 0.2) 100%)',
                        backgroundSize: '200% 200%',
                        animation: 'gradientShift 3s ease infinite',
                        boxShadow: '0 8px 32px -8px rgba(139, 92, 246, 0.3), inset 0 1px 0 0 rgba(255, 255, 255, 0.1)'
                      } : {
                        background: 'linear-gradient(145deg, #1e293b 0%, #1a1f2e 100%)'
                      }}
                    >
                      {/* Shine effect when selected */}
                      {formData.tariff === 'premium' && (
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent pointer-events-none" style={{
                          backgroundSize: '200% 100%',
                          animation: 'shimmer 2s ease-in-out infinite'
                        }} />
                      )}
                      <div className="flex items-start gap-3 relative z-10">
                        <div className={clsx(
                          'w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 mt-0.5 transition-all',
                          formData.tariff === 'premium'
                            ? 'border-purple-400'
                            : 'border-slate-600'
                        )}>
                          {formData.tariff === 'premium' && (
                            <div className="w-2.5 h-2.5 rounded-full bg-gradient-to-r from-purple-400 to-blue-400" />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className={clsx(
                              'font-semibold',
                              formData.tariff === 'premium'
                                ? 'bg-gradient-to-r from-purple-300 via-blue-300 to-purple-300 bg-clip-text text-transparent'
                                : 'text-slate-100'
                            )}>
                              Премиум лендинг
                            </span>
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold text-white ml-auto whitespace-nowrap" style={{
                              background: 'linear-gradient(135deg, #8b5cf6 0%, #3b82f6 100%)',
                              boxShadow: '0 2px 8px -2px rgba(139, 92, 246, 0.4)'
                            }}>
                              PRO
                            </span>
                          </div>
                          <p className="text-xs text-slate-300 mt-1">Профессиональный дизайн и качество</p>
                        </div>
                      </div>
                    </button>
                  </div>
                </div>

                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">
                    Название компании <span className="text-red-500">*</span>
                  </label>
                  <div className="relative">
                    <Building2 className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="text"
                      value={formData.company}
                      onChange={(e) => updateField('company', e.target.value)}
                      placeholder="Webly"
                      className={clsx('input pl-12', errors.company && 'input-error field-error-shake')}
                    />
                    {formData.company.trim() && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-emerald-500" />
                    )}
                  </div>
                  {errors.company && (
                    <p className="error-message">
                      <AlertCircle className="w-3.5 h-3.5" />
                      {errors.company}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">
                    Сфера деятельности <span className="text-red-500">*</span>
                  </label>
                  <div className="relative">
                    <Briefcase className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="text"
                      value={formData.business_type}
                      onChange={(e) => updateField('business_type', e.target.value)}
                      placeholder="Создание сайтов"
                      className={clsx('input pl-12', errors.business_type && 'input-error field-error-shake')}
                    />
                    {formData.business_type.trim() && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-emerald-500" />
                    )}
                  </div>
                  {errors.business_type && (
                    <p className="error-message">
                      <AlertCircle className="w-3.5 h-3.5" />
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
                  <label className="text-sm text-tg-hint mb-1.5 block">
                    ФИО клиента <span className="text-red-500">*</span>
                  </label>
                  <div className="relative">
                    <User className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="text"
                      value={formData.client_name}
                      onChange={(e) => updateField('client_name', e.target.value)}
                      placeholder="Иванов Иван"
                      className={clsx('input pl-12', errors.client_name && 'input-error field-error-shake')}
                      autoFocus
                    />
                    {formData.client_name.trim() && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-emerald-500" />
                    )}
                  </div>
                  {errors.client_name && (
                    <p className="error-message">
                      <AlertCircle className="w-3.5 h-3.5" />
                      {errors.client_name}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">Компания клиента</label>
                  <div className="relative">
                    <Building2 className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="text"
                      value={formData.client_company}
                      onChange={(e) => updateField('client_company', e.target.value)}
                      placeholder="ООО «Компания»"
                      className="input pl-12"
                    />
                  </div>
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">Контакт для связи</label>
                  <div className="relative">
                    <Phone className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="text"
                      value={formData.client_contact}
                      onChange={(e) => updateField('client_contact', e.target.value)}
                      placeholder="+7... или @telegram"
                      className="input pl-12"
                    />
                  </div>
                  <p className="text-xs text-tg-hint/70 mt-1.5">Телефон или ник в Telegram</p>
                </div>
              </div>
            )}

            {currentStep === 2 && (
              <div className="space-y-4">
                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">
                    Телефон для сайта <span className="text-red-500">*</span>
                  </label>
                  <div className="relative">
                    <Phone className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="tel"
                      inputMode="numeric"
                      value={formData.phone}
                      onChange={handlePhoneChange}
                      placeholder="+7 (XXX) XXX-XX-XX"
                      maxLength={18}
                      className={clsx('input pl-12', errors.phone && 'input-error field-error-shake')}
                      autoFocus
                    />
                    {formData.phone && validatePhone(formData.phone).valid && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-emerald-500" />
                    )}
                  </div>
                  {errors.phone ? (
                    <p className="error-message">
                      <AlertCircle className="w-3.5 h-3.5" />
                      {errors.phone}
                    </p>
                  ) : (
                    <p className="text-xs text-tg-hint/70 mt-1.5">Формат: +7 (XXX) XXX-XX-XX</p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1.5 block">
                    Email <span className="text-red-500">*</span>
                  </label>
                  <div className="relative">
                    <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint pointer-events-none" />
                    <input
                      type="email"
                      value={formData.email}
                      onChange={(e) => updateField('email', e.target.value)}
                      placeholder="info@company.ru"
                      className={clsx('input pl-12', errors.email && 'input-error field-error-shake')}
                    />
                    {formData.email && validateEmail(formData.email).valid && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-emerald-500" />
                    )}
                  </div>
                  {errors.email && (
                    <p className="error-message">
                      <AlertCircle className="w-3.5 h-3.5" />
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
                  <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800/50 rounded-xl flex items-center gap-3 animate-fade-in-up">
                    <div className="w-10 h-10 rounded-full bg-red-100 dark:bg-red-900/30 flex items-center justify-center flex-shrink-0">
                      <AlertCircle className="w-5 h-5 text-red-500" />
                    </div>
                    <div>
                      <p className="text-sm font-medium text-red-600 dark:text-red-400">{errors.services}</p>
                      <p className="text-xs text-red-500/70 mt-0.5">Укажите название хотя бы одной услуги или товара</p>
                    </div>
                  </div>
                )}
                {formData.services.map((service, i) => (
                  <div key={i} className="rounded-xl p-4 space-y-3 border border-blue-500/10" style={{ background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)' }}>
                    <div className="flex justify-between mb-3">
                      <span className="text-sm font-medium text-slate-200">Услуга/Товар {i + 1}</span>
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
                  className="flex flex-col items-center justify-center w-full min-h-[120px] p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint hover:border-blue-400 hover:text-blue-400 hover:bg-blue-500/5 transition-all cursor-pointer active:bg-tg-secondary-bg"
                  onDragOver={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                  }}
                  onDragEnter={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    e.currentTarget.classList.add('border-blue-400', 'bg-blue-500/10')
                  }}
                  onDragLeave={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    e.currentTarget.classList.remove('border-blue-400', 'bg-blue-500/10')
                  }}
                  onDrop={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    e.currentTarget.classList.remove('border-blue-400', 'bg-blue-500/10')
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
                    aria-label="Загрузить фотографии"
                  />
                  <div className="flex flex-col items-center pointer-events-none">
                    <div className="w-12 h-12 rounded-full bg-tg-secondary-bg flex items-center justify-center mb-3">
                      <Upload className="w-6 h-6" />
                    </div>
                    <span className="text-center font-medium">Добавить фото в «{photoCategories.find(c => c.id === selectedCategory)?.label}»</span>
                    <span className="text-center text-[11px] text-tg-hint mt-1">
                      Нажмите, перетащите файлы или вставьте из буфера (Ctrl+V)
                    </span>
                  </div>
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

                <div className="rounded-xl p-4 border border-blue-500/20" style={{ background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)' }}>
                  <p className="font-medium mb-3 text-slate-100">Итого</p>
                  <div className="space-y-2 text-sm">
                    <p className="flex justify-between"><span className="text-slate-400">Компания:</span> <span className="text-slate-200 font-medium">{formData.company || '—'}</span></p>
                    <p className="flex justify-between"><span className="text-slate-400">Клиент:</span> <span className="text-slate-200 font-medium">{formData.client_name || '—'}</span></p>
                    <p className="flex justify-between"><span className="text-slate-400">Услуг/Товаров:</span> <span className="text-slate-200 font-medium">{formData.services.filter(s => s.name.trim()).length}</span></p>
                    <p className="flex justify-between"><span className="text-slate-400">Фото:</span> <span className="text-slate-200 font-medium">{formData.photos.length}</span></p>
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

      {/* Bottom Actions - Full width centered layout */}
      <div className="sticky bottom-0 backdrop-blur-lg border-t safe-bottom z-20" style={{ background: 'rgba(15, 23, 42, 0.95)', borderColor: 'rgba(148, 163, 184, 0.1)' }}>
        {/* Validation status message */}
        {Object.keys(errors).length > 0 && (
          <div className="px-4 pt-3 pb-2">
            <p className="text-xs text-red-400 text-center flex items-center justify-center gap-1.5 animate-fade-in-up">
              <AlertCircle className="w-3.5 h-3.5" />
              Исправьте ошибки в форме выше
            </p>
          </div>
        )}
        {!canGoNext() && Object.keys(errors).length === 0 && (
          <div className="px-4 pt-3 pb-2">
            <p className="text-xs text-slate-400 text-center flex items-center justify-center gap-1.5">
              <AlertCircle className="w-3.5 h-3.5" />
              Заполните обязательные поля для продолжения
            </p>
          </div>
        )}

        <div className="px-4 pb-4 pt-2">
          <div className="flex gap-3 items-center">
            {currentStep > 0 && (
              <Tooltip content="Вернуться назад" position="top">
                <button
                  onClick={goBack}
                  className="flex items-center justify-center w-12 h-12 rounded-xl transition-all active:scale-95 border"
                  style={{
                    background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)',
                    borderColor: 'rgba(148, 163, 184, 0.2)',
                    color: '#cbd5e1'
                  }}
                >
                  <ArrowLeft className="w-5 h-5" />
                </button>
              </Tooltip>
            )}
            <Tooltip
              content={
                createMutation.isPending
                  ? 'Подождите...'
                  : !canGoNext()
                    ? 'Заполните обязательные поля'
                    : currentStep === steps.length - 1
                      ? 'Создать заявку'
                      : 'Перейти к следующему шагу'
              }
              position="top"
            >
              <button
                onClick={goNext}
                disabled={createMutation.isPending || !canGoNext()}
                className={clsx(
                  "flex-1 flex items-center justify-center gap-2.5 h-12 rounded-xl font-semibold text-base transition-all active:scale-[0.98] min-w-0",
                  (canGoNext() && !createMutation.isPending)
                    ? "text-white border"
                    : "text-slate-500 cursor-not-allowed border"
                )}
                style={canGoNext() && !createMutation.isPending ? {
                  background: 'linear-gradient(145deg, #334155 0%, #1e293b 100%)',
                  borderColor: 'rgba(148, 163, 184, 0.3)'
                } : {
                  background: 'linear-gradient(145deg, #0f172a 0%, #0a0f1e 100%)',
                  borderColor: 'rgba(100, 116, 139, 0.2)'
                }}
              >
                {createMutation.isPending ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" />
                    <span className="truncate">{uploadingPhotos ? 'Загрузка фото...' : 'Создание...'}</span>
                  </>
                ) : currentStep === steps.length - 1 ? (
                  <>
                    <Check className="w-5 h-5 flex-shrink-0" />
                    <span className="truncate">Создать заявку</span>
                  </>
                ) : (
                  <>
                    <span className="truncate">Далее</span>
                    <ArrowRight className="w-5 h-5 flex-shrink-0" />
                  </>
                )}
              </button>
            </Tooltip>
          </div>
        </div>
      </div>
    </div>
  )
}
