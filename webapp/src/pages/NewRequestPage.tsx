import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Briefcase, Phone, Mail,
  ArrowRight, ArrowLeft, Check, Loader2, Plus, X,
  Palette, Camera, Upload, AlertCircle,
  RotateCcw, Crown
} from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import ScrollContainer from '@/components/ScrollContainer'
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
  { id: 'hero', label: 'Баннер', icon: '🖼️' },
  { id: 'services', label: 'Услуги/Товары', icon: '🛠️' },
  { id: 'portfolio', label: 'Портфолио', icon: '📂' },
  { id: 'gallery', label: 'Галерея', icon: '📷' },
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

// DropZone Component for drag-and-drop file uploads with category selection
function DropZone({
  fileInputRef,
  onFileSelect,
  onDragDrop
}: {
  fileInputRef: React.RefObject<HTMLInputElement>
  onFileSelect: (e: React.ChangeEvent<HTMLInputElement>) => void
  onDragDrop: (files: File[]) => void
}) {
  const [isDragging, setIsDragging] = useState(false)
  const dragCounter = useRef(0)

  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    dragCounter.current++
    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      setIsDragging(true)
    }
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    dragCounter.current--
    if (dragCounter.current === 0) {
      setIsDragging(false)
    }
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)
    dragCounter.current = 0

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      // Convert FileList to array and trigger category selection modal
      const filesArray = Array.from(e.dataTransfer.files).filter(f => f.type.startsWith('image/'))
      if (filesArray.length > 0) {
        onDragDrop(filesArray)
      }
      e.dataTransfer.clearData()
    }
  }

  return (
    <label
      className={clsx(
        "relative flex flex-col items-center justify-center w-full min-h-[160px] p-6 border-2 border-dashed rounded-2xl transition-all duration-300 cursor-pointer overflow-hidden",
        isDragging
          ? "scale-[1.01]"
          : "hover:border-blue-400/50"
      )}
      style={{
        borderColor: isDragging ? 'var(--accent-primary)' : 'var(--border-default)',
        background: isDragging
          ? 'linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(59, 130, 246, 0.05) 100%)'
          : 'var(--bg-surface)',
        boxShadow: isDragging
          ? '0 0 30px -5px rgba(59, 130, 246, 0.3), inset 0 0 20px -10px rgba(59, 130, 246, 0.2)'
          : 'none'
      }}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* Animated background effect when dragging */}
      {isDragging && (
        <div
          className="absolute inset-0 pointer-events-none"
          style={{
            background: 'linear-gradient(45deg, transparent 30%, rgba(59, 130, 246, 0.1) 50%, transparent 70%)',
            backgroundSize: '200% 200%',
            animation: 'shimmer 1.5s ease-in-out infinite'
          }}
        />
      )}

      <input
        ref={fileInputRef}
        type="file"
        accept="image/*"
        multiple
        onChange={onFileSelect}
        className="sr-only"
        aria-label="Загрузить фотографии"
      />

      <div className="flex flex-col items-center pointer-events-none relative z-10">
        <div
          className={clsx(
            "w-14 h-14 rounded-2xl flex items-center justify-center mb-4 transition-all duration-300",
            isDragging && "scale-110"
          )}
          style={{
            background: isDragging
              ? 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)'
              : 'rgba(59, 130, 246, 0.12)',
            border: `1px solid ${isDragging ? 'var(--accent-primary)' : 'var(--border-accent)'}`,
            boxShadow: isDragging ? '0 4px 20px -4px rgba(59, 130, 246, 0.5)' : 'none'
          }}
        >
          <Upload
            className={clsx(
              "w-6 h-6 transition-all duration-300",
              isDragging && "animate-bounce"
            )}
            style={{
              color: isDragging ? 'white' : 'var(--accent-primary-light)'
            }}
          />
        </div>

        <span
          className="text-center font-semibold mb-1 transition-colors duration-300"
          style={{
            color: isDragging ? 'var(--accent-primary-light)' : 'var(--text-primary)'
          }}
        >
          {isDragging ? 'Отпустите для загрузки' : 'Перетащите фото сюда'}
        </span>

        <span
          className="text-center text-xs transition-colors duration-300"
          style={{ color: 'var(--text-subtle)' }}
        >
          {isDragging
            ? 'Выберите категорию после загрузки'
            : 'Или нажмите для выбора файлов'
          }
        </span>
      </div>
    </label>
  )
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

  // State for drag-and-drop category selection
  const [pendingDragFiles, setPendingDragFiles] = useState<File[]>([])
  const [showCategorySelectModal, setShowCategorySelectModal] = useState(false)

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

  // Handle drag-drop - show category selection modal
  const handleDragDropFiles = (files: File[]) => {
    setPendingDragFiles(files)
    setShowCategorySelectModal(true)
    haptic?.impactOccurred('light')
  }

  // Add photos from drag-drop with selected category
  const addPhotosWithCategory = (category: string) => {
    const newPhotos: PhotoItem[] = pendingDragFiles.map(file => ({
      file,
      preview: URL.createObjectURL(file),
      category,
      serviceIndex: selectedServiceIndexForPhoto ?? undefined,
    }))

    setFormData(prev => ({
      ...prev,
      photos: [...prev.photos, ...newPhotos],
    }))

    setPendingDragFiles([])
    setShowCategorySelectModal(false)
    setSelectedCategory(category)
    toast.success(`Добавлено ${newPhotos.length} фото в категорию "${photoCategories.find(c => c.id === category)?.label}"`)
    haptic?.notificationOccurred('success')
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
    <div className="min-h-screen flex flex-col" style={{ background: 'var(--tg-theme-bg-color)' }}>
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
              <div className="rounded-3xl p-6 w-full max-w-sm shadow-2xl border" style={{
                background: 'var(--surface-primary)',
                borderColor: 'var(--border-accent)',
                boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5), 0 0 0 1px var(--border-accent)'
              }}>
                <div className="text-center mb-6">
                  <div className="w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4" style={{
                    background: 'rgba(59, 130, 246, 0.15)',
                    border: '1px solid rgba(59, 130, 246, 0.3)'
                  }}>
                    <RotateCcw className="w-8 h-8" style={{ color: 'var(--accent-blue-light)' }} />
                  </div>
                  <h3 className="text-xl font-bold mb-2" style={{ color: 'var(--tg-theme-text-color)' }}>Найден черновик</h3>
                  <p className="text-sm leading-relaxed mb-3" style={{ color: 'var(--tg-theme-hint-color)' }}>
                    У вас есть незаконченная заявка. Хотите продолжить заполнение?
                  </p>
                  {draftData?.formData.company && (
                    <p className="text-sm font-medium" style={{ color: 'var(--accent-blue-light)' }}>
                      "{draftData.formData.company}"
                    </p>
                  )}
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={handleDiscardDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl font-semibold transition-all hover:bg-slate-700/30"
                    style={{
                      background: 'var(--surface-secondary)',
                      border: '1px solid var(--border-subtle)',
                      color: 'var(--tg-theme-subtitle-text-color)'
                    }}
                  >
                    Начать заново
                  </button>
                  <button
                    onClick={handleRestoreDraft}
                    className="flex-1 px-4 py-3.5 rounded-xl text-white font-semibold transition-all hover:shadow-lg"
                    style={{
                      background: 'linear-gradient(135deg, var(--accent-blue) 0%, var(--accent-blue-dark) 100%)',
                      boxShadow: '0 4px 16px -4px rgba(59, 130, 246, 0.5)'
                    }}
                  >
                    Продолжить
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Category Selection Modal for Drag & Drop */}
      <AnimatePresence>
        {showCategorySelectModal && (
          <>
            <motion.div
              className="fixed inset-0 z-50"
              style={{ background: 'rgba(0, 0, 0, 0.7)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => {
                setPendingDragFiles([])
                setShowCategorySelectModal(false)
              }}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 rounded-t-3xl z-[60] safe-bottom"
              style={{
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-subtle)',
                borderBottom: 'none'
              }}
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 30, stiffness: 400 }}
            >
              <div className="p-5">
                <div
                  className="w-10 h-1 rounded-full mx-auto mb-5"
                  style={{ background: 'var(--bg-tertiary)' }}
                />

                <div className="text-center mb-5">
                  <div
                    className="w-14 h-14 rounded-2xl flex items-center justify-center mx-auto mb-3"
                    style={{
                      background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)',
                      boxShadow: '0 4px 16px -4px rgba(59, 130, 246, 0.5)'
                    }}
                  >
                    <Camera className="w-7 h-7 text-white" />
                  </div>
                  <h3 className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>
                    Выберите категорию
                  </h3>
                  <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
                    Добавление {pendingDragFiles.length} {pendingDragFiles.length === 1 ? 'фото' : 'фото'}
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-3 mb-4">
                  {photoCategories.map(cat => (
                    <button
                      key={cat.id}
                      onClick={() => addPhotosWithCategory(cat.id)}
                      className="p-4 rounded-xl text-left transition-all active:scale-[0.98] hover:scale-[1.01]"
                      style={{
                        background: 'var(--bg-surface)',
                        border: '1px solid var(--border-default)'
                      }}
                    >
                      <span className="text-2xl mb-2 block">{cat.icon}</span>
                      <span className="font-semibold text-sm" style={{ color: 'var(--text-primary)' }}>
                        {cat.label}
                      </span>
                    </button>
                  ))}
                </div>

                <button
                  onClick={() => {
                    setPendingDragFiles([])
                    setShowCategorySelectModal(false)
                  }}
                  className="btn btn-secondary w-full"
                >
                  Отмена
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Header */}
      <div className="sticky top-0 z-10 backdrop-blur-xl border-b px-4 py-3" style={{
        background: 'rgba(10, 14, 23, 0.95)',
        borderColor: 'var(--border-subtle)'
      }}>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-lg font-bold" style={{ color: 'var(--tg-theme-text-color)' }}>Новая заявка</h1>
            <p className="text-xs" style={{ color: 'var(--tg-theme-hint-color)' }}>{steps[currentStep].title}</p>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-full border" style={{
            background: 'rgba(59, 130, 246, 0.1)',
            borderColor: 'var(--border-accent)'
          }}>
            <span className="text-sm font-semibold" style={{ color: 'var(--accent-blue-light)' }}>{currentStep + 1}</span>
            <span className="text-sm" style={{ color: 'var(--tg-theme-hint-color)' }}>/ {steps.length}</span>
          </div>
        </div>
        <div className="flex gap-1.5">
          {steps.map((_, i) => (
            <div
              key={i}
              className="flex-1 h-1.5 rounded-full transition-all duration-300"
              style={{
                background: i < currentStep
                  ? 'var(--accent-primary)'
                  : i === currentStep
                    ? 'var(--accent-primary-light)'
                    : 'var(--bg-tertiary)'
              }}
            />
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4 pb-24">
        <AnimatePresence mode="wait">
          <motion.div
            key={currentStep}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            transition={{ duration: 0.2 }}
          >
            {currentStep === 0 && (
              <div className="space-y-5">
                {/* Tariff Selection - Premium Design with Base Highlighting */}
                <div>
                  <label className="text-sm font-medium mb-3 block" style={{ color: 'var(--text-muted)' }}>
                    Тариф генерации
                  </label>
                  <div className="space-y-3">
                    {/* Standard tariff - always has visual distinction */}
                    <button
                      type="button"
                      onClick={() => updateField('tariff', 'standard')}
                      className="w-full p-4 rounded-2xl transition-all text-left active:scale-[0.99]"
                      style={{
                        background: formData.tariff === 'standard'
                          ? 'linear-gradient(135deg, rgba(59, 130, 246, 0.18) 0%, rgba(59, 130, 246, 0.08) 100%)'
                          : 'linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, var(--bg-surface) 100%)',
                        border: `1px solid ${formData.tariff === 'standard' ? 'rgba(59, 130, 246, 0.5)' : 'rgba(59, 130, 246, 0.2)'}`,
                        boxShadow: formData.tariff === 'standard'
                          ? '0 4px 20px -4px rgba(59, 130, 246, 0.3), inset 0 1px 0 0 rgba(255, 255, 255, 0.05)'
                          : '0 2px 12px rgba(0, 0, 0, 0.1)'
                      }}
                    >
                      <div className="flex items-center gap-3">
                        <div
                          className="w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 transition-all"
                          style={{
                            borderColor: formData.tariff === 'standard' ? 'var(--accent-primary)' : 'rgba(59, 130, 246, 0.5)',
                            boxShadow: formData.tariff === 'standard' ? '0 0 10px var(--accent-primary-glow)' : 'none'
                          }}
                        >
                          {formData.tariff === 'standard' && (
                            <div
                              className="w-2.5 h-2.5 rounded-full"
                              style={{ background: 'var(--accent-primary)' }}
                            />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <span
                            className="font-semibold"
                            style={{ color: formData.tariff === 'standard' ? 'var(--accent-primary-light)' : 'var(--accent-primary)' }}
                          >
                            Стандарт
                          </span>
                          <p
                            className="text-xs mt-0.5"
                            style={{ color: 'var(--text-muted)' }}
                          >
                            Базовая генерация лендинга
                          </p>
                        </div>
                      </div>
                    </button>

                    {/* Premium tariff - Gold Gradient with base highlighting */}
                    <button
                      type="button"
                      onClick={() => updateField('tariff', 'premium')}
                      className="w-full p-4 rounded-2xl transition-all text-left relative overflow-hidden active:scale-[0.99]"
                      style={{
                        background: formData.tariff === 'premium'
                          ? 'linear-gradient(135deg, rgba(245, 158, 11, 0.25) 0%, rgba(245, 158, 11, 0.15) 50%, rgba(245, 158, 11, 0.25) 100%)'
                          : 'linear-gradient(135deg, rgba(245, 158, 11, 0.12) 0%, rgba(245, 158, 11, 0.06) 50%, rgba(245, 158, 11, 0.12) 100%)',
                        border: `1px solid ${formData.tariff === 'premium' ? 'rgba(245, 158, 11, 0.6)' : 'rgba(245, 158, 11, 0.3)'}`,
                        boxShadow: formData.tariff === 'premium'
                          ? '0 6px 32px -4px rgba(245, 158, 11, 0.4), 0 0 0 1px rgba(245, 158, 11, 0.2), inset 0 1px 0 0 rgba(255, 255, 255, 0.12)'
                          : '0 2px 12px rgba(0, 0, 0, 0.1), 0 0 0 1px rgba(245, 158, 11, 0.15)'
                      }}
                    >
                      {/* Shine effect - always visible but stronger when selected */}
                      <div
                        className="absolute inset-0 pointer-events-none rounded-2xl"
                        style={{
                          opacity: formData.tariff === 'premium' ? 0.5 : 0.2,
                          background: 'linear-gradient(90deg, transparent 0%, rgba(245, 158, 11, 0.3) 50%, transparent 100%)',
                          backgroundSize: '200% 100%',
                          animation: 'shimmer 3s ease-in-out infinite'
                        }}
                      />

                      {/* Additional glow effect when selected */}
                      {formData.tariff === 'premium' && (
                        <div
                          className="absolute inset-0 pointer-events-none rounded-2xl"
                          style={{
                            background: 'radial-gradient(ellipse at center, rgba(245, 158, 11, 0.15) 0%, transparent 70%)',
                            animation: 'pulse 2s ease-in-out infinite'
                          }}
                        />
                      )}

                      <div className="flex items-center gap-3 relative z-10">
                        <div
                          className="w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 transition-all"
                          style={{
                            borderColor: formData.tariff === 'premium' ? 'var(--gold-primary)' : 'rgba(245, 158, 11, 0.5)',
                            boxShadow: formData.tariff === 'premium' ? '0 0 12px var(--gold-glow)' : 'none'
                          }}
                        >
                          {formData.tariff === 'premium' && (
                            <div
                              className="w-2.5 h-2.5 rounded-full"
                              style={{ background: 'linear-gradient(135deg, var(--gold-light) 0%, var(--gold-primary) 100%)' }}
                            />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span
                              className="font-semibold"
                              style={{
                                color: formData.tariff === 'premium'
                                  ? '#FCD34D'
                                  : 'var(--gold-primary)',
                                textShadow: formData.tariff === 'premium'
                                  ? '0 0 8px rgba(252, 211, 77, 0.3)'
                                  : 'none'
                              }}
                            >
                              Премиум лендинг
                            </span>
                            {/* Premium badge with gold gradient - always visible */}
                            <span
                              className={`inline-flex items-center gap-1.5 ml-auto ${formData.tariff !== 'premium' ? 'opacity-70' : ''}`}
                              style={{
                                position: 'relative',
                                overflow: 'hidden',
                                padding: '6px 12px',
                                borderRadius: '9999px',
                                fontSize: '10px',
                                fontWeight: 800,
                                letterSpacing: '0.1em',
                                textTransform: 'uppercase',
                                color: '#1A1A1A',
                                background: 'linear-gradient(135deg, var(--gold-light) 0%, var(--gold-primary) 25%, var(--gold-light) 50%, var(--gold-primary) 75%, var(--gold-light) 100%)',
                                backgroundSize: '200% 100%',
                                animation: 'shimmerGold 3s ease-in-out infinite',
                                boxShadow: '0 2px 12px -2px var(--gold-glow), 0 0 20px -4px rgba(245, 158, 11, 0.4), inset 0 1px 0 0 rgba(255, 255, 255, 0.4), inset 0 -1px 0 0 rgba(0, 0, 0, 0.1)',
                                textShadow: '0 1px 0 rgba(255, 255, 255, 0.3)'
                              }}
                            >
                              <Crown className="w-3.5 h-3.5 flex-shrink-0" style={{ color: '#1A1A1A' }} />
                              <span style={{ fontFamily: 'Manrope, sans-serif', fontWeight: 800 }}>PREMIUM</span>
                              {/* Shine effect */}
                              <div
                                className="absolute inset-0 pointer-events-none"
                                style={{
                                  background: 'linear-gradient(90deg, transparent 0%, rgba(255, 255, 255, 0.3) 50%, transparent 100%)',
                                  backgroundSize: '200% 100%',
                                  animation: 'badgeShine 3s ease-in-out infinite'
                                }}
                              />
                            </span>
                          </div>
                          <p
                            className="text-xs mt-0.5"
                            style={{
                              color: formData.tariff === 'premium'
                                ? 'var(--text-secondary)'
                                : 'var(--text-muted)'
                            }}
                          >
                            Профессиональный дизайн и качество
                          </p>
                        </div>
                      </div>
                    </button>
                  </div>
                </div>

                <div>
                  <label className="text-sm font-medium mb-2 block" style={{ color: 'var(--text-muted)' }}>
                    Название компании <span style={{ color: 'var(--error-light)' }}>*</span>
                  </label>
                  <div className="relative">
                    <Building2
                      className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 pointer-events-none"
                      style={{ color: 'var(--text-subtle)' }}
                    />
                    <input
                      type="text"
                      value={formData.company}
                      onChange={(e) => updateField('company', e.target.value)}
                      placeholder="Webly"
                      className={clsx('input pl-12', errors.company && 'input-error field-error-shake')}
                    />
                    {formData.company.trim() && (
                      <Check className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5" style={{ color: 'var(--success)' }} />
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
                  <div key={i} className="rounded-2xl p-4 space-y-3 border" style={{
                    background: 'var(--surface-secondary)',
                    borderColor: 'var(--border-subtle)'
                  }}>
                    <div className="flex justify-between mb-3">
                      <span className="text-sm font-medium" style={{ color: 'var(--tg-theme-text-color)' }}>Услуга/Товар {i + 1}</span>
                      {formData.services.length > 1 && (
                        <button onClick={() => removeService(i)} style={{ color: 'var(--tg-theme-destructive-text-color)' }}>
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
                        <label className="text-xs mb-1 block" style={{ color: 'var(--tg-theme-hint-color)' }}>Категория услуги</label>
                        <div className="flex gap-1.5 flex-wrap">
                          {defaultServiceCategories.map(cat => (
                            <button
                              key={cat.id}
                              type="button"
                              onClick={() => updateService(i, 'subcategory', cat.id)}
                              className="px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all"
                              style={{
                                background: service.subcategory === cat.id
                                  ? 'var(--accent-blue)'
                                  : 'var(--surface-tertiary)',
                                color: service.subcategory === cat.id
                                  ? 'white'
                                  : 'var(--tg-theme-text-color)'
                              }}
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
                  <label className="text-sm mb-2 block" style={{ color: 'var(--tg-theme-hint-color)' }}>Привязать фото к услуге/товару (опционально)</label>
                  <ScrollContainer>
                    <button
                      type="button"
                      onClick={() => setSelectedServiceIndexForPhoto(null)}
                      className="px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all"
                      style={{
                        background: selectedServiceIndexForPhoto === null
                          ? 'var(--accent-blue)'
                          : 'var(--surface-secondary)',
                        color: selectedServiceIndexForPhoto === null
                          ? 'white'
                          : 'var(--tg-theme-text-color)',
                        border: `1px solid ${selectedServiceIndexForPhoto === null ? 'var(--accent-blue)' : 'var(--border-subtle)'}`
                      }}
                    >
                      Без привязки
                    </button>
                    {formData.services.map((s, i) => (
                      <button
                        key={i}
                        type="button"
                        onClick={() => setSelectedServiceIndexForPhoto(i)}
                        className="px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all"
                        style={{
                          background: selectedServiceIndexForPhoto === i
                            ? 'var(--accent-blue)'
                            : 'var(--surface-secondary)',
                          color: selectedServiceIndexForPhoto === i
                            ? 'white'
                            : 'var(--tg-theme-text-color)',
                          border: `1px solid ${selectedServiceIndexForPhoto === i ? 'var(--accent-blue)' : 'var(--border-subtle)'}`
                        }}
                      >
                        {s.name || `Услуга/Товар ${i + 1}`}
                      </button>
                    ))}
                  </ScrollContainer>
                </div>

                {/* Category selector with improved visibility */}
                <div>
                  <label className="text-sm mb-2 block" style={{ color: 'var(--tg-theme-hint-color)' }}>Выберите категорию для новых фото:</label>
                  <ScrollContainer>
                    {photoCategories.map(cat => (
                      <button
                        key={cat.id}
                        onClick={() => {
                          setSelectedCategory(cat.id)
                          haptic?.selectionChanged()
                        }}
                        className="px-4 py-2.5 rounded-xl text-sm font-medium whitespace-nowrap transition-all"
                        style={{
                          background: selectedCategory === cat.id
                            ? 'var(--accent-blue)'
                            : 'var(--surface-secondary)',
                          color: selectedCategory === cat.id
                            ? 'white'
                            : 'var(--tg-theme-text-color)',
                          border: `1px solid ${selectedCategory === cat.id ? 'var(--accent-blue)' : 'var(--border-subtle)'}`,
                          boxShadow: selectedCategory === cat.id ? '0 4px 12px -4px rgba(59, 130, 246, 0.4)' : 'none'
                        }}
                      >
                        <span>{cat.label}</span>
                        {getPhotosByCategory(cat.id).length > 0 && (
                          <span className="ml-1.5 px-1.5 py-0.5 rounded-md text-xs" style={{
                            background: selectedCategory === cat.id ? 'rgba(255,255,255,0.2)' : 'var(--surface-tertiary)'
                          }}>
                            {getPhotosByCategory(cat.id).length}
                          </span>
                        )}
                      </button>
                    ))}
                  </ScrollContainer>
                </div>

                {/* Photo upload area with drag & drop */}
                <DropZone
                  fileInputRef={fileInputRef}
                  onFileSelect={handlePhotoSelect}
                  onDragDrop={handleDragDropFiles}
                />

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

                <div className="rounded-2xl p-4 border" style={{
                  background: 'var(--surface-secondary)',
                  borderColor: 'var(--border-accent)'
                }}>
                  <p className="font-medium mb-3" style={{ color: 'var(--tg-theme-text-color)' }}>Итого</p>
                  <div className="space-y-2 text-sm">
                    <p className="flex justify-between"><span style={{ color: 'var(--tg-theme-hint-color)' }}>Компания:</span> <span className="font-medium" style={{ color: 'var(--tg-theme-text-color)' }}>{formData.company || '—'}</span></p>
                    <p className="flex justify-between"><span style={{ color: 'var(--tg-theme-hint-color)' }}>Клиент:</span> <span className="font-medium" style={{ color: 'var(--tg-theme-text-color)' }}>{formData.client_name || '—'}</span></p>
                    <p className="flex justify-between"><span style={{ color: 'var(--tg-theme-hint-color)' }}>Услуг/Товаров:</span> <span className="font-medium" style={{ color: 'var(--tg-theme-text-color)' }}>{formData.services.filter(s => s.name.trim()).length}</span></p>
                    <p className="flex justify-between"><span style={{ color: 'var(--tg-theme-hint-color)' }}>Фото:</span> <span className="font-medium" style={{ color: 'var(--tg-theme-text-color)' }}>{formData.photos.length}</span></p>
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
      <div className="sticky bottom-0 backdrop-blur-xl border-t safe-bottom z-20" style={{
        background: 'rgba(10, 14, 23, 0.98)',
        borderColor: 'var(--border-subtle)',
        boxShadow: '0 -4px 30px -4px rgba(0, 0, 0, 0.5)'
      }}>
        <div className="p-4">
          {/* Validation status message - placed directly above button, centered */}
          {Object.keys(errors).length > 0 && (
            <div className="mb-4">
              <p className="text-sm text-center flex items-center justify-center gap-2 animate-fade-in-up font-medium" style={{ color: 'var(--tg-theme-destructive-text-color)' }}>
                <AlertCircle className="w-4 h-4 flex-shrink-0" />
                <span>Заполните поля</span>
              </p>
            </div>
          )}
          {!canGoNext() && Object.keys(errors).length === 0 && (
            <div className="mb-4">
              <p className="text-sm text-center flex items-center justify-center gap-2 font-medium" style={{ color: 'var(--tg-theme-hint-color)' }}>
                <AlertCircle className="w-4 h-4 flex-shrink-0" />
                <span>Заполните обязательные поля для продолжения</span>
              </p>
            </div>
          )}

          {/* Action buttons row */}
          <div className="flex items-center gap-3 w-full">
            {/* Back button - fixed width */}
            {currentStep > 0 && (
              <button
                onClick={goBack}
                className="flex items-center justify-center w-14 h-14 rounded-xl transition-all active:scale-95 border flex-shrink-0"
                style={{
                  background: 'var(--surface-secondary)',
                  borderColor: 'var(--border-subtle)',
                  color: 'var(--tg-theme-subtitle-text-color)'
                }}
              >
                <ArrowLeft className="w-5 h-5" />
              </button>
            )}

            {/* Main CTA Button - Takes remaining space */}
            <button
              onClick={goNext}
              disabled={createMutation.isPending || !canGoNext()}
              className={clsx(
                "flex-1 flex items-center justify-center gap-2.5 h-14 rounded-xl font-bold text-lg transition-all active:scale-[0.98] text-white",
                (!canGoNext() || createMutation.isPending) && "opacity-50 cursor-not-allowed"
              )}
              style={{
                background: (!canGoNext() || createMutation.isPending)
                  ? 'var(--surface-tertiary)'
                  : 'linear-gradient(135deg, var(--accent-blue-light) 0%, var(--accent-blue) 50%, var(--accent-blue-dark) 100%)',
                boxShadow: (!canGoNext() || createMutation.isPending)
                  ? 'none'
                  : '0 8px 32px -4px rgba(59, 130, 246, 0.6), inset 0 1px 0 0 rgba(255, 255, 255, 0.15)'
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
          </div>
        </div>
      </div>
    </div>
  )
}
