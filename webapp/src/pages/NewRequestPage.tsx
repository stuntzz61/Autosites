import { useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Briefcase, Phone, Mail, MapPin,
  ArrowRight, ArrowLeft, Check, Loader2, Plus, X,
  Clock, Palette, Camera, Upload, Image
} from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const steps = [
  { id: 'company', title: 'О компании', icon: Building2 },
  { id: 'client', title: 'Клиент', icon: User },
  { id: 'contacts', title: 'Контакты', icon: Phone },
  { id: 'services', title: 'Услуги', icon: Briefcase },
  { id: 'photos', title: 'Фото', icon: Camera },
  { id: 'details', title: 'Детали', icon: Palette },
]

// Default site structure - fixed, not editable by manager
const DEFAULT_STRUCTURE = ['Hero', 'О компании', 'Услуги', 'Портфолио', 'Отзывы', 'Контакты']

// Photo categories
const photoCategories = [
  { id: 'hero', label: 'Главный баннер', icon: '🏠', description: 'Главное фото для шапки' },
  { id: 'services', label: 'Услуги', icon: '🛠', description: 'Фото для раздела услуг' },
  { id: 'portfolio', label: 'Портфолио', icon: '📁', description: 'Примеры работ' },
  { id: 'gallery', label: 'Галерея', icon: '🖼', description: 'Дополнительные фото' },
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
  // Company info
  company: string
  business_type: string
  summary: string

  // Client info
  client_name: string
  client_company: string
  client_contact: string

  // Contacts for site
  phone: string
  email: string
  address: string
  work_hours: string

  // Services
  services: ServiceItem[]

  // Photos
  photos: PhotoItem[]

  // Additional
  color_palette: string
}

export default function NewRequestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [currentStep, setCurrentStep] = useState(0)
  const [selectedCategory, setSelectedCategory] = useState('hero')
  const [uploadingPhotos, setUploadingPhotos] = useState(false)
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
          meta: {
            status: 'draft'
          }
        }
      }

      // Create request first
      const response = await requestsApi.create({
        company_name: formData.company,
        client_name: formData.client_name,
        payload,
      })

      const requestId = response.data.id

      // Upload photos if any
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
      toast.error('Ошибка создания заявки')
      haptic?.notificationOccurred('error')
      setUploadingPhotos(false)
    },
  })

  const updateField = <K extends keyof FormData>(field: K, value: FormData[K]) => {
    setFormData(prev => ({ ...prev, [field]: value }))
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

    // Reset input
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
    if (limited.length > 1) {
      formatted += ' (' + limited.slice(1, 4)
    }
    if (limited.length >= 4) {
      formatted += ')'
    }
    if (limited.length > 4) {
      formatted += ' ' + limited.slice(4, 7)
    }
    if (limited.length > 7) {
      formatted += '-' + limited.slice(7, 9)
    }
    if (limited.length > 9) {
      formatted += '-' + limited.slice(9, 11)
    }

    return formatted
  }

  const handlePhoneChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const formatted = formatPhone(e.target.value)
    updateField('phone', formatted)
  }

  const validatePhone = (phone: string) => {
    if (!phone) return true
    const cleaned = phone.replace(/\D/g, '')
    return cleaned.length === 11
  }

  const validateEmail = (email: string) => {
    if (!email) return true
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  }

  const canGoNext = () => {
    switch (currentStep) {
      case 0: return formData.company.trim().length >= 2
      case 1: return formData.client_name.trim().length >= 2
      case 2: return validatePhone(formData.phone) && validateEmail(formData.email) && formData.phone.trim().length > 0
      case 3: return formData.services.some(s => s.name.trim().length > 0)
      default: return true
    }
  }

  const goNext = () => {
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
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator px-4 py-3">
        <div className="flex items-center justify-between mb-3">
          <h1 className="text-lg font-semibold text-tg-text">Новая заявка</h1>
          <span className="text-sm text-tg-hint">{currentStep + 1} / {steps.length}</span>
        </div>

        {/* Progress */}
        <div className="flex gap-1">
          {steps.map((step, i) => (
            <div
              key={step.id}
              className={clsx(
                'flex-1 h-1 rounded-full transition-colors',
                i <= currentStep ? 'bg-blue-600 dark:bg-white' : 'bg-tg-secondary-bg'
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
            {/* Step 0: Company */}
            {currentStep === 0 && (
              <StepContent
                icon={<Building2 className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="О компании"
                subtitle="Расскажите о бизнесе клиента"
              >
                <div className="space-y-4">
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Название компании *</label>
                    <input
                      type="text"
                      value={formData.company}
                      onChange={(e) => updateField('company', e.target.value)}
                      placeholder="Например: Webly"
                      className="input text-lg"
                      autoFocus
                    />
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Сфера деятельности *</label>
                    <input
                      type="text"
                      value={formData.business_type}
                      onChange={(e) => updateField('business_type', e.target.value)}
                      placeholder="Например: Создание сайтов под ключ"
                      className="input"
                    />
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Описание компании</label>
                    <textarea
                      value={formData.summary}
                      onChange={(e) => updateField('summary', e.target.value)}
                      placeholder="Краткое описание для раздела 'О компании'..."
                      className="input min-h-[120px] resize-none"
                      rows={4}
                    />
                  </div>
                </div>
              </StepContent>
            )}

            {/* Step 1: Client */}
            {currentStep === 1 && (
              <StepContent
                icon={<User className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="Клиент"
                subtitle="Кто заказывает сайт?"
              >
                <div className="space-y-4">
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">ФИО клиента *</label>
                    <input
                      type="text"
                      value={formData.client_name}
                      onChange={(e) => updateField('client_name', e.target.value)}
                      placeholder="Иванов Иван Иванович"
                      className="input text-lg"
                      autoFocus
                    />
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Компания клиента</label>
                    <input
                      type="text"
                      value={formData.client_company}
                      onChange={(e) => updateField('client_company', e.target.value)}
                      placeholder="ООО «Рога и Копыта»"
                      className="input"
                    />
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Контакт клиента</label>
                    <input
                      type="text"
                      value={formData.client_contact}
                      onChange={(e) => updateField('client_contact', e.target.value)}
                      placeholder="+7..., email, @telegram"
                      className="input"
                    />
                  </div>
                </div>
              </StepContent>
            )}

            {/* Step 2: Contacts */}
            {currentStep === 2 && (
              <StepContent
                icon={<Phone className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="Контакты для сайта"
                subtitle="Эти данные будут на сайте"
              >
                <div className="space-y-4">
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Телефон *</label>
                    <div className="relative">
                      <Phone className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="tel"
                        inputMode="numeric"
                        value={formData.phone}
                        onChange={handlePhoneChange}
                        placeholder="+7 (XXX) XXX-XX-XX"
                        maxLength={18}
                        className={clsx('input pl-10', !validatePhone(formData.phone) && formData.phone && 'ring-2 ring-red-500/30')}
                        autoFocus
                      />
                    </div>
                    {formData.phone && !validatePhone(formData.phone) && (
                      <p className="text-xs text-red-500 mt-1">Введите номер полностью (11 цифр)</p>
                    )}
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Email</label>
                    <div className="relative">
                      <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="email"
                        value={formData.email}
                        onChange={(e) => updateField('email', e.target.value)}
                        placeholder="info@company.ru"
                        className={clsx('input pl-10', !validateEmail(formData.email) && formData.email && 'ring-2 ring-red-500/30')}
                      />
                    </div>
                    {formData.email && !validateEmail(formData.email) && (
                      <p className="text-xs text-red-500 mt-1">Введите корректный email</p>
                    )}
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Адрес</label>
                    <div className="relative">
                      <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="text"
                        value={formData.address}
                        onChange={(e) => updateField('address', e.target.value)}
                        placeholder="г. Москва, ул. Примерная, 123"
                        className="input pl-10"
                      />
                    </div>
                  </div>
                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Часы работы</label>
                    <div className="relative">
                      <Clock className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="text"
                        value={formData.work_hours}
                        onChange={(e) => updateField('work_hours', e.target.value)}
                        placeholder="Пн-Пт 9:00-18:00"
                        className="input pl-10"
                      />
                    </div>
                  </div>
                </div>
              </StepContent>
            )}

            {/* Step 3: Services */}
            {currentStep === 3 && (
              <StepContent
                icon={<Briefcase className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="Услуги"
                subtitle="Что предлагает компания?"
              >
                <div className="space-y-4">
                  {formData.services.map((service, i) => (
                    <div key={i} className="bg-tg-secondary-bg rounded-xl p-4">
                      <div className="flex items-center justify-between mb-3">
                        <span className="text-sm font-medium text-tg-text">Услуга {i + 1}</span>
                        {formData.services.length > 1 && (
                          <button
                            onClick={() => removeService(i)}
                            className="p-1.5 rounded-lg bg-red-500/10 text-red-500"
                          >
                            <X className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                      <div className="space-y-3">
                        <input
                          type="text"
                          value={service.name}
                          onChange={(e) => updateService(i, 'name', e.target.value)}
                          placeholder="Название услуги *"
                          className="input"
                          autoFocus={i === 0}
                        />
                        <input
                          type="text"
                          value={service.summary}
                          onChange={(e) => updateService(i, 'summary', e.target.value)}
                          placeholder="Краткое описание"
                          className="input"
                        />
                        <input
                          type="text"
                          value={service.priceFrom}
                          onChange={(e) => updateService(i, 'priceFrom', e.target.value)}
                          placeholder="Цена (например: от 10 000 ₽)"
                          className="input"
                        />
                      </div>
                    </div>
                  ))}
                  <button onClick={addService} className="btn btn-secondary w-full">
                    <Plus className="w-5 h-5" />
                    Добавить услугу
                  </button>
                </div>
              </StepContent>
            )}

            {/* Step 4: Photos */}
            {currentStep === 4 && (
              <StepContent
                icon={<Camera className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="Фотографии"
                subtitle="Загрузите фото для сайта"
              >
                <div className="space-y-4">
                  {/* Category selector */}
                  <div className="flex gap-2 overflow-x-auto pb-2 -mx-4 px-4">
                    {photoCategories.map(cat => (
                      <button
                        key={cat.id}
                        onClick={() => setSelectedCategory(cat.id)}
                        className={clsx(
                          'flex items-center gap-2 px-3 py-2 rounded-xl whitespace-nowrap transition-colors',
                          selectedCategory === cat.id
                            ? 'bg-blue-600 dark:bg-white text-white dark:text-black'
                            : 'bg-tg-secondary-bg text-tg-text'
                        )}
                      >
                        <span>{cat.icon}</span>
                        <span className="text-sm font-medium">{cat.label}</span>
                        {getPhotosByCategory(cat.id).length > 0 && (
                          <span className={clsx(
                            'px-1.5 py-0.5 rounded-full text-xs',
                            selectedCategory === cat.id
                              ? 'bg-white/20 dark:bg-black/20'
                              : 'bg-blue-600/10 dark:bg-white/10 text-blue-600 dark:text-white'
                          )}>
                            {getPhotosByCategory(cat.id).length}
                          </span>
                        )}
                      </button>
                    ))}
                  </div>

                  {/* Upload button */}
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
                    className="w-full flex items-center justify-center gap-2 p-4 border-2 border-dashed border-blue-500/30 dark:border-white/20 rounded-xl text-blue-600 dark:text-white hover:bg-blue-500/5 dark:hover:bg-white/5 transition-colors"
                  >
                    <Upload className="w-5 h-5" />
                    <span className="font-medium">Добавить фото в "{photoCategories.find(c => c.id === selectedCategory)?.label}"</span>
                  </button>

                  {/* Photos grid */}
                  {formData.photos.length > 0 && (
                    <div className="space-y-4">
                      {photoCategories.map(cat => {
                        const photos = getPhotosByCategory(cat.id)
                        if (photos.length === 0) return null

                        return (
                          <div key={cat.id}>
                            <p className="text-sm font-medium text-tg-text mb-2">
                              {cat.icon} {cat.label}
                            </p>
                            <div className="flex gap-2 overflow-x-auto pb-2">
                              {photos.map((photo, index) => {
                                const globalIndex = formData.photos.indexOf(photo)
                                return (
                                  <div key={index} className="relative flex-shrink-0 group">
                                    <img
                                      src={photo.preview}
                                      alt={cat.label}
                                      className="w-20 h-20 rounded-xl object-cover"
                                    />
                                    <button
                                      onClick={() => removePhoto(globalIndex)}
                                      className="absolute -top-2 -right-2 w-6 h-6 bg-red-500 text-white rounded-full flex items-center justify-center shadow-lg"
                                    >
                                      <X className="w-3 h-3" />
                                    </button>
                                  </div>
                                )
                              })}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  )}

                  {formData.photos.length === 0 && (
                    <div className="text-center py-8 text-tg-hint">
                      <Image className="w-12 h-12 mx-auto mb-2 opacity-50" />
                      <p>Фото можно добавить позже</p>
                    </div>
                  )}
                </div>
              </StepContent>
            )}

            {/* Step 5: Details */}
            {currentStep === 5 && (
              <StepContent
                icon={<Palette className="w-8 h-8 text-blue-600 dark:text-white" />}
                title="Детали сайта"
                subtitle="Дополнительные настройки"
              >
                <div className="space-y-6">
                  {/* Fixed Structure Info */}
                  <div>
                    <label className="text-sm text-tg-hint mb-2 block">Структура сайта</label>
                    <div className="bg-tg-secondary-bg rounded-xl p-4">
                      <div className="flex flex-wrap gap-2">
                        {DEFAULT_STRUCTURE.map(section => (
                          <span
                            key={section}
                            className="px-3 py-1.5 rounded-lg text-sm font-medium bg-tg-section text-tg-text border border-gray-200 dark:border-zinc-700"
                          >
                            {section}
                          </span>
                        ))}
                      </div>
                      <p className="text-xs text-tg-hint mt-3">
                        Стандартная структура сайта
                      </p>
                    </div>
                  </div>

                  <div>
                    <label className="text-sm text-tg-hint mb-1 block">Цветовая палитра</label>
                    <div className="relative">
                      <Palette className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="text"
                        value={formData.color_palette}
                        onChange={(e) => updateField('color_palette', e.target.value)}
                        placeholder="Например: синий и белый"
                        className="input pl-10"
                      />
                    </div>
                  </div>

                  {/* Summary */}
                  <div className="bg-blue-50 dark:bg-zinc-800 rounded-xl p-4 border border-blue-200 dark:border-zinc-700">
                    <p className="text-sm font-semibold text-tg-text mb-2">📋 Итого</p>
                    <div className="space-y-1 text-sm text-tg-hint">
                      <p>Компания: <span className="text-tg-text">{formData.company || '—'}</span></p>
                      <p>Клиент: <span className="text-tg-text">{formData.client_name || '—'}</span></p>
                      <p>Услуг: <span className="text-tg-text">{formData.services.filter(s => s.name.trim()).length}</span></p>
                      <p>Фото: <span className="text-tg-text">{formData.photos.length}</span></p>
                    </div>
                  </div>
                </div>
              </StepContent>
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
                {uploadingPhotos ? 'Загрузка фото...' : 'Создание...'}
              </>
            ) : currentStep === steps.length - 1 ? (
              <>
                <Check className="w-5 h-5" />
                Создать заявку
              </>
            ) : (
              <>
                Далее
                <ArrowRight className="w-5 h-5" />
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  )
}

function StepContent({
  icon,
  title,
  subtitle,
  children,
}: {
  icon: React.ReactNode
  title: string
  subtitle: string
  children: React.ReactNode
}) {
  return (
    <div>
      <div className="flex items-center gap-4 mb-6">
        <div className="p-3 rounded-xl bg-blue-50 dark:bg-zinc-800">
          {icon}
        </div>
        <div>
          <h2 className="text-xl font-semibold text-tg-text">{title}</h2>
          <p className="text-sm text-tg-hint">{subtitle}</p>
        </div>
      </div>
      {children}
    </div>
  )
}
