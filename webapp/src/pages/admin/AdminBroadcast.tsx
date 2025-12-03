import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { Send, Users, Image } from 'lucide-react'
import toast from 'react-hot-toast'

export default function AdminBroadcast() {
  const [message, setMessage] = useState('')
  const [photo, setPhoto] = useState('')

  const broadcastMutation = useMutation({
    mutationFn: () => adminApi.broadcast({ message, photo: photo || undefined }),
    onSuccess: (res) => {
      toast.success(`Отправлено ${res.data?.sent || 0} пользователям`)
      setMessage('')
      setPhoto('')
    },
    onError: () => {
      toast.error('Ошибка отправки')
    },
  })

  const handleSend = () => {
    if (!message.trim()) {
      toast.error('Введите сообщение')
      return
    }
    broadcastMutation.mutate()
  }

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Рассылка</h1>

      <div className="bg-tg-secondary-bg rounded-2xl p-4 space-y-4">
        <div className="flex items-center gap-2 text-tg-hint">
          <Users className="w-5 h-5" />
          <span>Сообщение будет отправлено всем менеджерам</span>
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

        {/* Photo URL */}
        <div>
          <label className="block text-sm font-medium text-tg-text mb-2">
            <Image className="w-4 h-4 inline mr-1" />
            URL изображения (необязательно)
          </label>
          <input
            type="url"
            value={photo}
            onChange={(e) => setPhoto(e.target.value)}
            placeholder="https://example.com/image.jpg"
            className="w-full p-3 bg-tg-bg rounded-xl text-tg-text placeholder:text-tg-hint"
          />
        </div>

        {/* Preview */}
        {(message || photo) && (
          <div className="border border-tg-separator rounded-xl p-3">
            <p className="text-xs text-tg-hint mb-2">Превью:</p>
            {photo && (
              <img
                src={photo}
                alt="Preview"
                className="w-full max-h-48 object-cover rounded-lg mb-2"
                onError={(e) => (e.currentTarget.style.display = 'none')}
              />
            )}
            <p className="text-tg-text whitespace-pre-wrap">{message}</p>
          </div>
        )}

        {/* Send button */}
        <button
          onClick={handleSend}
          disabled={broadcastMutation.isPending || !message.trim()}
          className="w-full py-3 bg-tg-accent text-white rounded-xl font-medium flex items-center justify-center gap-2 disabled:opacity-50"
        >
          {broadcastMutation.isPending ? (
            'Отправка...'
          ) : (
            <>
              <Send className="w-5 h-5" />
              Отправить рассылку
            </>
          )}
        </button>
      </div>
    </div>
  )
}

