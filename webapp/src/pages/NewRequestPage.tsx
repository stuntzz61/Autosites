import { useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Briefcase, Phone, Mail, MapPin,
  ArrowRight, ArrowLeft, Check, Loader2, Plus, X,
  Clock, Palette, Camera, Upload, Image, AlertCircle
} from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const steps = [
  { id: 'company', title: 'Компания', icon: Building2 },
  { id: 'client', title: 'Клиент', icon: User },
  { id: 'contacts', title: 'Контакты', icon: Phone },
  { id: 'services', title: 'Услуги', icon: Briefcase },
  { id: 'photos', title: 'Фото', icon: Camera },
  { id: 'details', title: 'Детали', icon: Palette },
]

const DEFAULT_STRUCTURE = ['Hero', 'О компании', 'Услуги', 'Портфолио', 'Отзывы', 'Контакты']

const photoCategories = [
  { id: 'hero', label: 'Баннер', icon: '🏠' },
  { id: 'services', label: 'Услуги', icon: '🛠' },
  { id: 'portfolio', label: 'Портфолио', icon: '📁' },
  { id: 'gallery', label: 'Галерея', icon: '🖼' },
]

interface ServiceItem {
  name: string
  summary: string
  priceFrom: string
}

interface PhotoItem {
  file: File
  preview: string
  category: string
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
}

interface ValidationErrors {
  company?: string
  business_type?: string
  client_name?: string
  phone?: string
  email?: string
  services?: string
}

export default function NewRequestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [currentStep, setCurrentStep] = useState(0)
  const [selectedCategory, setSelectedCategory] = useState('hero')
  const [uploadingPhotos, setUploadingPhotos] = useState(false)
  const [errors, setErrors] = useState<ValidationErrors>({})
  const [formData, setFormData] = useState<FormData>({
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
    services: [{ name: '', summary: '', priceFrom: '' }],
    photos: [],
    color_palette: 'На усмотрение дизайнера',
  })

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
      })

      const requestId = response.data.id

      if (formData.photos.length > 0) {
        setUploadingPhotos(true)
        for (const photo of formData.photos) {
          try {
            const photoFormData = new FormData()
            photoFormData.append('file', photo.file)
            photoFormData.append('category', photo.category)
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
      services: [...prev.services, { name: '', summary: '', priceFrom: '' }],
    }))
  }

  const updateService = (index: number, field: keyof ServiceItem, value: string) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.map((s, i) => i === index ? { ...s, [field]: value } : s),
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

    const newPhotos: PhotoItem[] = []
    for (let i = 0; i < files.length; i++) {
      const file = files[i]
      if (file.type.startsWith('image/')) {
        newPhotos.push({
          file,
          preview: URL.createObjectURL(file),
          category: selectedCategory,
        })
      }
    }

    setFormData(prev => ({
      ...prev,
      photos: [...prev.photos, ...newPhotos],
    }))

    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  const removePhoto = (index: number) => {
    setFormData(prev => {
      const newPhotos = [...prev.photos]
      URL.revokeObjectURL(newPhotos[index].preview)
      newPhotos.splice(index, 1)
      return { ...prev, photos: newPhotos }
    })
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

  const validatePhone = (phone: string) => {
    if (!phone) return true
    return phone.replace(/\D/g, '').length === 11
  }

  const validateEmail = (email: string) => {
    if (!email) return true
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  }

  const validateStep = (): boolean => {
    const newErrors: ValidationErrors = {}

    switch (currentStep) {
      case 0:
        if (!formData.company.trim()) newErrors.company = 'Введите название'
        if (!formData.business_type.trim()) newErrors.business_type = 'Укажите сферу'
        break
      case 1:
        if (!formData.client_name.trim()) newErrors.client_name = 'Введите имя'
        break
      case 2:
        if (!formData.phone.trim()) newErrors.phone = 'Введите телефон'
        else if (!validatePhone(formData.phone)) newErrors.phone = 'Неверный формат'
        if (formData.email && !validateEmail(formData.email)) newErrors.email = 'Неверный email'
        break
      case 3:
        if (!formData.services.some(s => s.name.trim())) newErrors.services = 'Добавьте услугу'
        break
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const canGoNext = () => {
    switch (currentStep) {
      case 0: return formData.company.trim() && formData.business_type.trim()
      case 1: return formData.client_name.trim()
      case 2: return validatePhone(formData.phone) && validateEmail(formData.email) && formData.phone.trim()
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

  return (
    <div className="min-h-screen flex flex-col bg-tg-bg">
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
                i < currentStep ? 'bg-emerald-500' :
                i === currentStep ? 'bg-zinc-900 dark:bg-white' : 'bg-zinc-200 dark:bg-zinc-700'
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
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Название компании *</label>
                  <input
                    type="text"
                    value={formData.company}
                    onChange={(e) => updateField('company', e.target.value)}
                    placeholder="Webly"
                    className={clsx('input', errors.company && 'border-red-500')}
                    autoFocus
                  />
                  {errors.company && <p className="text-xs text-red-500 mt-1">{errors.company}</p>}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Сфера деятельности *</label>
                  <input
                    type="text"
                    value={formData.business_type}
                    onChange={(e) => updateField('business_type', e.target.value)}
                    placeholder="Создание сайтов"
                    className={clsx('input', errors.business_type && 'border-red-500')}
                  />
                  {errors.business_type && <p className="text-xs text-red-500 mt-1">{errors.business_type}</p>}
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
                    className={clsx('input', errors.client_name && 'border-red-500')}
                    autoFocus
                  />
                  {errors.client_name && <p className="text-xs text-red-500 mt-1">{errors.client_name}</p>}
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
                    className={clsx('input', errors.phone && 'border-red-500')}
                    autoFocus
                  />
                  {errors.phone && <p className="text-xs text-red-500 mt-1">{errors.phone}</p>}
                </div>
                <div>
                  <label className="text-sm text-tg-hint mb-1 block">Email</label>
                  <input
                    type="email"
                    value={formData.email}
                    onChange={(e) => updateField('email', e.target.value)}
                    placeholder="info@company.ru"
                    className={clsx('input', errors.email && 'border-red-500')}
                  />
                  {errors.email && <p className="text-xs text-red-500 mt-1">{errors.email}</p>}
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
                  <div key={i} className="bg-tg-secondary-bg rounded-xl p-4">
                    <div className="flex justify-between mb-3">
                      <span className="text-sm font-medium">Услуга {i + 1}</span>
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
                        placeholder="Название *"
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
                <button onClick={addService} className="btn btn-secondary w-full">
                  <Plus className="w-5 h-5" /> Добавить
                </button>
              </div>
            )}

            {currentStep === 4 && (
              <div className="space-y-4">
                <div className="flex gap-2 overflow-x-auto pb-2">
                  {photoCategories.map(cat => (
                    <button
                      key={cat.id}
                      onClick={() => setSelectedCategory(cat.id)}
                      className={clsx(
                        'px-3 py-2 rounded-xl text-sm font-medium whitespace-nowrap',
                        selectedCategory === cat.id
                          ? 'bg-black dark:bg-white text-white dark:text-black'
                          : 'bg-tg-secondary-bg'
                      )}
                    >
                      {cat.icon} {cat.label}
                      {getPhotosByCategory(cat.id).length > 0 && (
                        <span className="ml-1 opacity-60">({getPhotosByCategory(cat.id).length})</span>
                      )}
                    </button>
                  ))}
                </div>

                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*"
                  multiple
                  onChange={handlePhotoSelect}
                  className="hidden"
                />
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint"
                >
                  <Upload className="w-5 h-5 mx-auto mb-2" />
                  Добавить фото
                </button>

                {formData.photos.length > 0 && (
                  <div className="flex gap-2 overflow-x-auto pb-2">
                    {formData.photos.map((photo, i) => (
                      <div key={i} className="relative flex-shrink-0">
                        <img src={photo.preview} alt="" className="w-20 h-20 rounded-xl object-cover" />
                        <button
                          onClick={() => removePhoto(i)}
                          className="absolute -top-2 -right-2 w-6 h-6 bg-red-500 text-white rounded-full flex items-center justify-center"
                        >
                          <X className="w-3 h-3" />
                        </button>
                      </div>
                    ))}
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
                    <p>Услуг: <span className="text-tg-text">{formData.services.filter(s => s.name.trim()).length}</span></p>
                    <p>Фото: <span className="text-tg-text">{formData.photos.length}</span></p>
                  </div>
                </div>
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Bottom Actions */}
      <div className="sticky bottom-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        <div className="flex gap-3">
          {currentStep > 0 && (
            <button onClick={goBack} className="btn btn-secondary">
              <ArrowLeft className="w-5 h-5" />
            </button>
          )}
          <button
            onClick={goNext}
            disabled={!canGoNext() || createMutation.isPending}
            className="btn btn-primary flex-1"
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
        </div>
      </div>
    </div>
  )
}
