import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Radio, Send, Users, User, Image, X, Check, Loader2
} from 'lucide-react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'
import toast from 'react-hot-toast'
import clsx from 'clsx'

export default function AdminBroadcast() {
  const { haptic, webApp } = useTelegram()
  const [message, setMessage] = useState('')
  const [photo, setPhoto] = useState<File | null>(null)
  const [photoPreview, setPhotoPreview] = useState<string | null>(null)
  const [sendToAll, setSendToAll] = useState(true)
  const [selectedManagers, setSelectedManagers] = useState<string[]>([])

  const { data: managers } = useQuery({
    queryKey: ['admin-managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  const broadcastMutation = useMutation({
    mutationFn: async () => {
      let photoBase64 = undefined
      if (photo) {
        const reader = new FileReader()
        photoBase64 = await new Promise<string>((resolve) => {
          reader.onload = () => resolve(reader.result as string)
          reader.readAsDataURL(photo)
        })
      }

      return adminApi.broadcast({
        message,
        photo: photoBase64,
        recipient_ids: sendToAll ? undefined : selectedManagers,
      })
    },
    onSuccess: () => {
      toast.success('Рассылка отправлена!')
      haptic?.notificationOccurred('success')
      setMessage('')
      setPhoto(null)
      setPhotoPreview(null)
      setSelectedManagers([])
    },
    onError: () => {
      toast.error('Ошибка отправки')
      haptic?.notificationOccurred('error')
    },
  })

  const handlePhotoSelect = (files: FileList | null) => {
    if (!files || !files[0]) return
    const file = files[0]
    setPhoto(file)
    setPhotoPreview(URL.createObjectURL(file))
  }

  const toggleManager = (id: string) => {
    setSelectedManagers(prev =>
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    )
  }

  const handleSend = () => {
    if (!message.trim()) {
      toast.error('Введите сообщение')
      return
    }
    if (!sendToAll && selectedManagers.length === 0) {
      toast.error('Выберите получателей')
      return
    }

    webApp?.showConfirm(
      `Отправить сообщение ${sendToAll ? 'всем менеджерам' : `${selectedManagers.length} менеджерам`}?`,
      (confirmed) => {
        if (confirmed) {
          broadcastMutation.mutate()
        }
      }
    )
  }

  const activeManagers = managers?.filter((m: any) => !m.is_blocked) || []

  return (
    <div className="min-h-screen pb-32">
      {/* Header */}
      <div className="p-4">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 rounded-2xl bg-brand-500/10">
            <Radio className="w-6 h-6 text-brand-500" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-tg-text">Рассылка</h2>
            <p className="text-sm text-tg-hint">
              Отправьте сообщение менеджерам
            </p>
          </div>
        </div>

        {/* Recipients */}
        <div className="mb-6">
          <p className="section-header">Получатели</p>
          <div className="bg-tg-section rounded-2xl overflow-hidden">
            <button
              onClick={() => {
                haptic?.selectionChanged()
                setSendToAll(true)
                setSelectedManagers([])
              }}
              className="list-item w-full text-left"
            >
              <div className={clsx(
                'w-6 h-6 rounded-full border-2 flex items-center justify-center',
                sendToAll ? 'bg-tg-button border-tg-button' : 'border-tg-hint'
              )}>
                {sendToAll && <Check className="w-4 h-4 text-tg-button-text" />}
              </div>
              <Users className="w-5 h-5 text-tg-hint" />
              <div className="flex-1">
                <p className="font-medium text-tg-text">Все менеджеры</p>
                <p className="text-xs text-tg-hint">{activeManagers.length} получателей</p>
              </div>
            </button>
            <div className="divider" />
            <button
              onClick={() => {
                haptic?.selectionChanged()
                setSendToAll(false)
              }}
              className="list-item w-full text-left"
            >
              <div className={clsx(
                'w-6 h-6 rounded-full border-2 flex items-center justify-center',
                !sendToAll ? 'bg-tg-button border-tg-button' : 'border-tg-hint'
              )}>
                {!sendToAll && <Check className="w-4 h-4 text-tg-button-text" />}
              </div>
              <User className="w-5 h-5 text-tg-hint" />
              <div className="flex-1">
                <p className="font-medium text-tg-text">Выбрать</p>
                <p className="text-xs text-tg-hint">
                  {selectedManagers.length > 0
                    ? `${selectedManagers.length} выбрано`
                    : 'Выберите получателей'}
                </p>
              </div>
            </button>
          </div>
        </div>

        {/* Manager Selection */}
        <AnimatePresence>
          {!sendToAll && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="mb-6 overflow-hidden"
            >
              <p className="section-header">Выберите менеджеров</p>
              <div className="bg-tg-section rounded-2xl overflow-hidden max-h-60 overflow-y-auto">
                {activeManagers.map((manager: any, i: number) => (
                  <div key={manager.id}>
                    {i > 0 && <div className="divider" />}
                    <button
                      onClick={() => {
                        haptic?.selectionChanged()
                        toggleManager(manager.id)
                      }}
                      className="list-item w-full text-left"
                    >
                      <div className={clsx(
                        'w-6 h-6 rounded-lg border-2 flex items-center justify-center',
                        selectedManagers.includes(manager.id)
                          ? 'bg-tg-button border-tg-button'
                          : 'border-tg-hint'
                      )}>
                        {selectedManagers.includes(manager.id) && (
                          <Check className="w-4 h-4 text-tg-button-text" />
                        )}
                      </div>
                      <div className="w-8 h-8 rounded-full bg-gradient-to-br from-brand-500 to-brand-600 flex items-center justify-center text-white font-bold text-sm">
                        {manager.first_name?.[0]?.toUpperCase()}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-medium text-tg-text truncate">
                          {manager.first_name} {manager.last_name}
                        </p>
                        {manager.username && (
                          <p className="text-xs text-tg-hint">@{manager.username}</p>
                        )}
                      </div>
                    </button>
                  </div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Message */}
        <div className="mb-6">
          <p className="section-header">Сообщение</p>
          <textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="Введите текст сообщения..."
            className="input min-h-[120px] resize-none"
          />
        </div>

        {/* Photo */}
        <div>
          <p className="section-header">Изображение (опционально)</p>
          {photoPreview ? (
            <div className="relative inline-block">
              <img
                src={photoPreview}
                alt="Preview"
                className="w-32 h-32 rounded-2xl object-cover"
              />
              <button
                onClick={() => {
                  setPhoto(null)
                  setPhotoPreview(null)
                }}
                className="absolute -top-2 -right-2 w-7 h-7 bg-red-500 text-white rounded-full flex items-center justify-center shadow-lg"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          ) : (
            <label className="btn btn-secondary cursor-pointer inline-flex">
              <Image className="w-5 h-5" />
              Добавить фото
              <input
                type="file"
                accept="image/*"
                className="hidden"
                onChange={(e) => handlePhotoSelect(e.target.files)}
              />
            </label>
          )}
        </div>
      </div>

      {/* Send Button */}
      <div className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom">
        <button
          onClick={handleSend}
          disabled={broadcastMutation.isPending || !message.trim()}
          className="btn btn-primary w-full"
        >
          {broadcastMutation.isPending ? (
            <Loader2 className="w-5 h-5 animate-spin" />
          ) : (
            <>
              <Send className="w-5 h-5" />
              Отправить
              {!sendToAll && selectedManagers.length > 0 && ` (${selectedManagers.length})`}
            </>
          )}
        </button>
      </div>
    </div>
  )
}

