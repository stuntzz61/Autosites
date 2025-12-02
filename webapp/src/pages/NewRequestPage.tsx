import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Building2, User, Globe, Briefcase, Phone, Mail, MapPin,
  ArrowRight, ArrowLeft, Check, Loader2, Image, Plus, X
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
  { id: 'photos', title: 'Фото', icon: Image },
]

const photoCategories = [
  { id: 'main', label: 'Главное фото', desc: 'Основное изображение для шапки сайта' },
  { id: 'services', label: 'Фото услуг', desc: 'Изображения для раздела услуг' },
  { id: 'gallery', label: 'Галерея', desc: 'Дополнительные фотографии' },
  { id: 'team', label: 'Команда', desc: 'Фото сотрудников (опционально)' },
]

interface FormData {
  company_name: string
  client_name: string
  sphere: string
  phone: string
  email: string
  address: string
  services: string[]
  about: string
  photos: Record<string, File[]>
}

export default function NewRequestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, mainButton } = useTelegram()

  const [currentStep, setCurrentStep] = useState(0)
  const [formData, setFormData] = useState<FormData>({
    company_name: '',
    client_name: '',
    sphere: '',
    phone: '',
    email: '',
    address: '',
    services: [''],
    about: '',
    photos: {},
  })

  const createMutation = useMutation({
    mutationFn: async () => {
      // Create request
      const response = await requestsApi.create({
        company_name: formData.company_name,
        client_name: formData.client_name,
        payload: {
          site: {
            sphere: formData.sphere,
            contacts: {
              phone: formData.phone,
              email: formData.email,
              address: formData.address,
            },
            services: formData.services.filter(s => s.trim()),
            about: formData.about,
          },
        },
      })

      // Upload photos if any
      const requestId = response.data.id
      for (const [category, files] of Object.entries(formData.photos)) {
        if (files.length > 0) {
          const fd = new FormData()
          fd.append('category', category)
          files.forEach(file => fd.append('files', file))
          await requestsApi.uploadPhotos(requestId, fd)
        }
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
    },
  })

  const updateField = (field: keyof FormData, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const addService = () => {
    setFormData(prev => ({ ...prev, services: [...prev.services, ''] }))
  }

  const updateService = (index: number, value: string) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.map((s, i) => i === index ? value : s),
    }))
  }

  const removeService = (index: number) => {
    setFormData(prev => ({
      ...prev,
      services: prev.services.filter((_, i) => i !== index),
    }))
  }

  const handlePhotoSelect = (category: string, files: FileList | null) => {
    if (!files) return
    setFormData(prev => ({
      ...prev,
      photos: {
        ...prev.photos,
        [category]: [...(prev.photos[category] || []), ...Array.from(files)],
      },
    }))
  }

  const removePhoto = (category: string, index: number) => {
    setFormData(prev => ({
      ...prev,
      photos: {
        ...prev.photos,
        [category]: prev.photos[category]?.filter((_, i) => i !== index) || [],
      },
    }))
  }

  const validatePhone = (phone: string) => {
    const cleaned = phone.replace(/\D/g, '')
    return cleaned.length >= 10 && cleaned.length <= 12
  }

  const validateEmail = (email: string) => {
    if (!email) return true // email optional
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  }

  const canGoNext = () => {
    switch (currentStep) {
      case 0: return formData.company_name.trim().length >= 2
      case 1: return formData.client_name.trim().length >= 2
      case 2: return validatePhone(formData.phone) && validateEmail(formData.email)
      case 3: return formData.services.some(s => s.trim().length > 0)
      default: return true
    }
  }

  const getStepError = () => {
    switch (currentStep) {
      case 0: 
        if (formData.company_name && formData.company_name.trim().length < 2) 
          return 'Минимум 2 символа'
        return null
      case 1:
        if (formData.client_name && formData.client_name.trim().length < 2)
          return 'Минимум 2 символа'
        return null
      case 2:
        if (formData.phone && !validatePhone(formData.phone))
          return 'Введите корректный номер телефона'
        if (formData.email && !validateEmail(formData.email))
          return 'Введите корректный email'
        return null
      default: return null
    }
  }

  const stepError = getStepError()

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
                i <= currentStep ? 'bg-tg-button' : 'bg-tg-secondary-bg'
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
              <StepContent
                icon={<Building2 className="w-8 h-8 text-brand-500" />}
                title="Название компании"
                subtitle="Как называется бизнес клиента?"
              >
                <input
                  type="text"
                  value={formData.company_name}
                  onChange={(e) => updateField('company_name', e.target.value)}
                  placeholder="Например: ООО Ромашка"
                  className="input text-lg"
                  autoFocus
                />
                <input
                  type="text"
                  value={formData.sphere}
                  onChange={(e) => updateField('sphere', e.target.value)}
                  placeholder="Сфера деятельности (опционально)"
                  className="input mt-3"
                />
              </StepContent>
            )}

            {currentStep === 1 && (
              <StepContent
                icon={<User className="w-8 h-8 text-brand-500" />}
                title="Имя клиента"
                subtitle="Кто заказывает сайт?"
              >
                <input
                  type="text"
                  value={formData.client_name}
                  onChange={(e) => updateField('client_name', e.target.value)}
                  placeholder="Имя заказчика"
                  className="input text-lg"
                  autoFocus
                />
              </StepContent>
            )}

            {currentStep === 2 && (
              <StepContent
                icon={<Phone className="w-8 h-8 text-green-500" />}
                title="Контакты"
                subtitle="Как связаться с компанией?"
              >
                <div className="space-y-3">
                  <div>
                    <div className="relative">
                      <Phone className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="tel"
                        value={formData.phone}
                        onChange={(e) => updateField('phone', e.target.value)}
                        placeholder="+7 (XXX) XXX-XX-XX *"
                        className={clsx('input pl-10', !validatePhone(formData.phone) && formData.phone && 'ring-2 ring-red-500/30')}
                        autoFocus
                      />
                    </div>
                    {formData.phone && !validatePhone(formData.phone) && (
                      <p className="text-xs text-red-500 mt-1 ml-1">Введите корректный номер (10-12 цифр)</p>
                    )}
                  </div>
                  <div>
                    <div className="relative">
                      <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                      <input
                        type="email"
                        value={formData.email}
                        onChange={(e) => updateField('email', e.target.value)}
                        placeholder="Email (опционально)"
                        className={clsx('input pl-10', !validateEmail(formData.email) && formData.email && 'ring-2 ring-red-500/30')}
                      />
                    </div>
                    {formData.email && !validateEmail(formData.email) && (
                      <p className="text-xs text-red-500 mt-1 ml-1">Введите корректный email</p>
                    )}
                  </div>
                  <div className="relative">
                    <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                    <input
                      type="text"
                      value={formData.address}
                      onChange={(e) => updateField('address', e.target.value)}
                      placeholder="Адрес (опционально)"
                      className="input pl-10"
                    />
                  </div>
                </div>
              </StepContent>
            )}

            {currentStep === 3 && (
              <StepContent
                icon={<Briefcase className="w-8 h-8 text-amber-500" />}
                title="Услуги"
                subtitle="Чем занимается компания?"
              >
                <div className="space-y-3">
                  {formData.services.map((service, i) => (
                    <div key={i} className="flex gap-2">
                      <input
                        type="text"
                        value={service}
                        onChange={(e) => updateService(i, e.target.value)}
                        placeholder={`Услуга ${i + 1}`}
                        className="input flex-1"
                        autoFocus={i === 0}
                      />
                      {formData.services.length > 1 && (
                        <button
                          onClick={() => removeService(i)}
                          className="p-3 rounded-xl bg-red-500/10 text-red-500"
                        >
                          <X className="w-5 h-5" />
                        </button>
                      )}
                    </div>
                  ))}
                  <button
                    onClick={addService}
                    className="btn btn-secondary w-full"
                  >
                    <Plus className="w-5 h-5" />
                    Добавить услугу
                  </button>
                </div>
              </StepContent>
            )}

            {currentStep === 4 && (
              <StepContent
                icon={<Image className="w-8 h-8 text-purple-500" />}
                title="Фотографии"
                subtitle="Загрузите изображения для сайта"
              >
                <div className="space-y-4">
                  {photoCategories.map(cat => (
                    <div key={cat.id} className="bg-tg-secondary-bg rounded-2xl p-4">
                      <p className="font-medium text-tg-text mb-1">{cat.label}</p>
                      <p className="text-xs text-tg-hint mb-3">{cat.desc}</p>

                      {/* Preview */}
                      {formData.photos[cat.id]?.length > 0 && (
                        <div className="flex gap-2 overflow-x-auto mb-3">
                          {formData.photos[cat.id].map((file, i) => (
                            <div key={i} className="relative flex-shrink-0">
                              <img
                                src={URL.createObjectURL(file)}
                                alt=""
                                className="w-16 h-16 rounded-xl object-cover"
                              />
                              <button
                                onClick={() => removePhoto(cat.id, i)}
                                className="absolute -top-1 -right-1 w-5 h-5 bg-red-500 text-white rounded-full flex items-center justify-center"
                              >
                                <X className="w-3 h-3" />
                              </button>
                            </div>
                          ))}
                        </div>
                      )}

                      <label className="btn btn-secondary w-full cursor-pointer">
                        <Plus className="w-5 h-5" />
                        Выбрать фото
                        <input
                          type="file"
                          accept="image/*"
                          multiple
                          className="hidden"
                          onChange={(e) => handlePhotoSelect(cat.id, e.target.files)}
                        />
                      </label>
                    </div>
                  ))}
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
            className={clsx(
              'btn flex-1',
              currentStep === steps.length - 1 ? 'btn-primary' : 'btn-primary'
            )}
          >
            {createMutation.isPending ? (
              <Loader2 className="w-5 h-5 animate-spin" />
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
        <div className="p-3 rounded-2xl bg-tg-secondary-bg">
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
