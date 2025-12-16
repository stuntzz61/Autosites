import { useState, useRef } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { Send, Users, Image, X, Check, Loader2, Upload, User } from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

interface Manager {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  is_blocked: boolean
}

export default function AdminBroadcast() {
  const [message, setMessage] = useState('')
  const [photoPreview, setPhotoPreview] = useState<string | null>(null)
  const [photoFile, setPhotoFile] = useState<File | null>(null)
  const [showManagerSelect, setShowManagerSelect] = useState(false)
  const [selectedManagerIds, setSelectedManagerIds] = useState<string[]>([])
  const [sendToAll, setSendToAll] = useState(true)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const { data: managers } = useQuery({
    queryKey: ['admin', 'managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  // Filter only active (non-blocked) managers
  const activeManagers = (managers || []).filter((m: Manager) => !m.is_blocked)

  const broadcastMutation = useMutation({
    mutationFn: async () => {
      // If photo file exists, first upload it to get URL
      let photoUrl = undefined

      if (photoFile) {
        // For now, we'll use base64 encoding for the photo
        // In production, you'd want to upload to S3 first
        const reader = new FileReader()
        photoUrl = await new Promise<string>((resolve) => {
          reader.onload = () => resolve(reader.result as string)
          reader.readAsDataURL(photoFile)
        })
      }

      const recipientIds = sendToAll ? undefined : selectedManagerIds

      return adminApi.broadcast({
        message,
        photo: photoUrl,
        recipient_ids: recipientIds
      })
    },
    onSuccess: (res) => {
      const data = res.data
      const sent = data?.sent_count || 0
      const failed = data?.failed_count || 0
      const total = data?.total_recipients || sent + failed

      if (sent > 0) {
        toast.success(`Отправлено ${sent} из ${total} получателей`)
      } else {
        toast.error('Не удалось отправить сообщения')
      }

      if (failed > 0) {
        toast.error(`${failed} сообщений не доставлено`)
      }

      // Reset form
      setMessage('')
      setPhotoPreview(null)
      setPhotoFile(null)
      setSelectedManagerIds([])
      setSendToAll(true)
    },
    onError: () => {
      toast.error('Ошибка отправки')
    },
  })

  const handlePhotoSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      if (file.size > 5 * 1024 * 1024) {
        toast.error('Файл слишком большой (макс. 5МБ)')
        return
      }

      setPhotoFile(file)
      const preview = URL.createObjectURL(file)
      setPhotoPreview(preview)
    }
    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  const removePhoto = () => {
    if (photoPreview) {
      URL.revokeObjectURL(photoPreview)
    }
    setPhotoPreview(null)
    setPhotoFile(null)
  }

  const toggleManager = (id: string) => {
    setSelectedManagerIds(prev =>
      prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
    )
  }

  const selectAllManagers = () => {
    setSelectedManagerIds(activeManagers.map((m: Manager) => m.id))
  }

  const deselectAllManagers = () => {
    setSelectedManagerIds([])
  }

  const handleSend = () => {
    if (!message.trim()) {
      toast.error('Введите сообщение')
      return
    }

    if (!sendToAll && selectedManagerIds.length === 0) {
      toast.error('Выберите получателей')
      return
    }

    broadcastMutation.mutate()
  }

  const recipientCount = sendToAll ? activeManagers.length : selectedManagerIds.length

  return (
    <div className="p-4 space-y-4 pb-24">
      <h1 className="text-2xl font-bold text-tg-text">Рассылка</h1>

      <div className="bg-tg-secondary-bg rounded-2xl p-4 space-y-4">
        {/* Recipients selector */}
        <div>
          <label className="block text-sm font-medium text-tg-text mb-2">
            Получатели
          </label>

          <div className="flex gap-2 mb-3">
            <button
              onClick={() => {
                setSendToAll(true)
                setSelectedManagerIds([])
              }}
              className={`flex-1 py-2 px-3 rounded-xl text-sm font-medium transition-colors ${
                sendToAll
                  ? 'bg-tg-accent text-white'
                  : 'bg-tg-bg text-tg-text'
              }`}
            >
              <Users className="w-4 h-4 inline mr-1" />
              Все ({activeManagers.length})
            </button>
            <button
              onClick={() => {
                setSendToAll(false)
                setShowManagerSelect(true)
              }}
              className={`flex-1 py-2 px-3 rounded-xl text-sm font-medium transition-colors ${
                !sendToAll
                  ? 'bg-tg-accent text-white'
                  : 'bg-tg-bg text-tg-text'
              }`}
            >
              <User className="w-4 h-4 inline mr-1" />
              Выбрать ({selectedManagerIds.length})
            </button>
          </div>

          {!sendToAll && selectedManagerIds.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {selectedManagerIds.map(id => {
                const manager = activeManagers.find((m: Manager) => m.id === id)
                if (!manager) return null
                return (
                  <span
                    key={id}
                    className="inline-flex items-center gap-1 px-2 py-1 bg-tg-bg rounded-lg text-sm"
                  >
                    {manager.first_name}
                    <button onClick={() => toggleManager(id)} className="text-tg-hint hover:text-tg-text">
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                )
              })}
            </div>
          )}
        </div>

        {/* Message */}
        <div>
          <label className="block text-sm font-medium text-tg-text mb-2">
            Текст сообщения *
          </label>
          <textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="Введите текст сообщения..."
            rows={5}
            className="w-full p-3 bg-tg-bg rounded-xl text-tg-text placeholder:text-tg-hint resize-none"
          />
        </div>

        {/* Photo upload */}
        <div>
          <label className="block text-sm font-medium text-tg-text mb-2">
            <Image className="w-4 h-4 inline mr-1" />
            Фото (необязательно)
          </label>

          <input
            ref={fileInputRef}
            type="file"
            accept="image/*"
            onChange={handlePhotoSelect}
            className="hidden"
          />

          {photoPreview ? (
            <div className="relative">
              <img
                src={photoPreview}
                alt="Preview"
                className="w-full max-h-48 object-cover rounded-xl"
              />
              <button
                onClick={removePhoto}
                className="absolute top-2 right-2 w-8 h-8 bg-black/50 text-white rounded-full flex items-center justify-center"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          ) : (
            <button
              onClick={() => fileInputRef.current?.click()}
              className="w-full p-4 border-2 border-dashed border-tg-separator rounded-xl text-tg-hint hover:text-tg-text hover:border-tg-hint transition-colors"
            >
              <Upload className="w-5 h-5 mx-auto mb-2" />
              Выбрать фото
            </button>
          )}
        </div>

        {/* Preview */}
        {message && (
          <div className="border border-tg-separator rounded-xl p-3">
            <p className="text-xs text-tg-hint mb-2">Превью сообщения:</p>
            {photoPreview && (
              <img
                src={photoPreview}
                alt="Preview"
                className="w-full max-h-32 object-cover rounded-lg mb-2"
              />
            )}
            <p className="text-tg-text whitespace-pre-wrap text-sm">{message}</p>
          </div>
        )}

        {/* Summary */}
        <div className="bg-tg-bg rounded-xl p-3">
          <p className="text-sm text-tg-hint">
            Сообщение будет отправлено <strong className="text-tg-text">{recipientCount}</strong> менеджерам
            {photoFile && <span> с фото</span>}
          </p>
        </div>

        {/* Send button */}
        <button
          onClick={handleSend}
          disabled={broadcastMutation.isPending || !message.trim() || recipientCount === 0}
          className="w-full py-3 bg-tg-accent text-white rounded-xl font-medium flex items-center justify-center gap-2 disabled:opacity-50"
        >
          {broadcastMutation.isPending ? (
            <>
              <Loader2 className="w-5 h-5 animate-spin" />
              Отправка...
            </>
          ) : (
            <>
              <Send className="w-5 h-5" />
              Отправить ({recipientCount})
            </>
          )}
        </button>
      </div>

      {/* Manager selection modal */}
      <AnimatePresence>
        {showManagerSelect && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowManagerSelect(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom max-h-[80vh] flex flex-col"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="p-4 border-b border-tg-separator">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
                <div className="flex items-center justify-between">
                  <p className="text-lg font-semibold text-tg-text">
                    Выбор получателей ({selectedManagerIds.length})
                  </p>
                  <div className="flex gap-2">
                    <button
                      onClick={selectAllManagers}
                      className="text-sm text-tg-accent"
                    >
                      Все
                    </button>
                    <span className="text-tg-hint">|</span>
                    <button
                      onClick={deselectAllManagers}
                      className="text-sm text-tg-hint"
                    >
                      Сбросить
                    </button>
                  </div>
                </div>
              </div>

              <div className="flex-1 overflow-y-auto p-4 space-y-2">
                {activeManagers.map((manager: Manager) => (
                  <button
                    key={manager.id}
                    onClick={() => toggleManager(manager.id)}
                    className={`w-full p-3 rounded-xl flex items-center gap-3 transition-colors ${
                      selectedManagerIds.includes(manager.id)
                        ? 'bg-tg-accent/20'
                        : 'bg-tg-secondary-bg'
                    }`}
                  >
                    <div className={`w-6 h-6 rounded-full border-2 flex items-center justify-center ${
                      selectedManagerIds.includes(manager.id)
                        ? 'border-tg-accent bg-tg-accent'
                        : 'border-tg-hint'
                    }`}>
                      {selectedManagerIds.includes(manager.id) && (
                        <Check className="w-4 h-4 text-white" />
                      )}
                    </div>
                    <div className="text-left flex-1">
                      <p className="font-medium text-tg-text">
                        {manager.first_name} {manager.last_name}
                      </p>
                      {manager.username && (
                        <p className="text-sm text-tg-hint">@{manager.username}</p>
                      )}
                    </div>
                  </button>
                ))}
              </div>

              <div className="p-4 border-t border-tg-separator">
                <button
                  onClick={() => setShowManagerSelect(false)}
                  className="w-full py-3 bg-tg-accent text-white rounded-xl font-medium"
                >
                  Готово ({selectedManagerIds.length} выбрано)
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}
