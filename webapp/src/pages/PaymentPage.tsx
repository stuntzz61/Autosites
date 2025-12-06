import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { paymentApi, sitesApi } from '@/api/client'
import { ArrowLeft, Loader2, CheckCircle2, AlertCircle, Clock, CreditCard, QrCode, RefreshCw } from 'lucide-react'
import toast from 'react-hot-toast'
import { motion, AnimatePresence } from 'framer-motion'
import { useTelegram } from '@/contexts/TelegramContext'

// Simple QR Code component using Canvas (fallback if no library)
function QRCodeCanvas({ data, size = 256 }: { data: string; size?: number }) {
  const canvasRef = useState<HTMLCanvasElement | null>(null)[0]

  useEffect(() => {
    if (!canvasRef || !data) return

    // Generate simple QR-like pattern (placeholder)
    const canvas = canvasRef
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    canvas.width = size
    canvas.height = size

    // White background
    ctx.fillStyle = '#FFFFFF'
    ctx.fillRect(0, 0, size, size)

    // Generate pattern based on data hash
    ctx.fillStyle = '#000000'
    const gridSize = 25
    const cellSize = size / gridSize

    for (let i = 0; i < gridSize; i++) {
      for (let j = 0; j < gridSize; j++) {
        const hash = (data.charCodeAt((i * gridSize + j) % data.length) + i + j) % 3
        if (hash === 0 || hash === 1) {
          ctx.fillRect(i * cellSize, j * cellSize, cellSize, cellSize)
        }
      }
    }

    // Add corner markers (QR code style)
    const markerSize = cellSize * 3
    const drawMarker = (x: number, y: number) => {
      // Outer square
      ctx.fillRect(x, y, markerSize, markerSize)
      // Inner square
      ctx.fillStyle = '#FFFFFF'
      ctx.fillRect(x + cellSize, y + cellSize, cellSize, cellSize)
      ctx.fillStyle = '#000000'
    }

    drawMarker(0, 0)
    drawMarker(size - markerSize, 0)
    drawMarker(0, size - markerSize)
  }, [canvasRef, data, size])

  return (
    <div className="bg-white p-4 rounded-2xl">
      <canvas
        ref={(el) => {
          if (el) {
            // @ts-ignore
            canvasRef = el
          }
        }}
        className="w-full h-auto"
      />
      <p className="text-xs text-center mt-2 text-gray-500">QR Code Placeholder</p>
    </div>
  )
}

export default function PaymentPage() {
  const { siteId } = useParams<{ siteId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()

  const [selectedPlan, setSelectedPlan] = useState<string>('basic')
  const [months, setMonths] = useState(1)

  // Get site info
  const { data: site } = useQuery({
    queryKey: ['site', siteId],
    queryFn: () => sitesApi.get(siteId!).then(res => res.data),
    enabled: !!siteId,
  })

  // Get hosting plans
  const { data: plansData } = useQuery({
    queryKey: ['hosting-plans'],
    queryFn: () => sitesApi.getPlans().then(res => res.data),
  })

  const plans = plansData?.items || []

  // Create payment
  const createPaymentMutation = useMutation({
    mutationFn: () => paymentApi.create({
      site_id: siteId!,
      plan: selectedPlan,
      months,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['payments', siteId] })
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка создания платежа')
      haptic?.notificationOccurred('error')
    },
  })

  // Get active payment
  const { data: paymentsData } = useQuery({
    queryKey: ['payments', siteId],
    queryFn: () => paymentApi.listBySite(siteId!).then(res => res.data),
    enabled: !!siteId,
  })

  const activePayment = paymentsData?.items?.find((p: any) =>
    p.status === 'pending' || p.status === 'processing'
  )

  // Get payment QR
  const { data: qrData } = useQuery({
    queryKey: ['payment-qr', activePayment?.id],
    queryFn: () => paymentApi.getQR(activePayment!.id).then(res => res.data),
    enabled: !!activePayment?.id,
    refetchInterval: 10000, // Check every 10 seconds
  })

  // Verify payment
  const verifyMutation = useMutation({
    mutationFn: (paymentId: string) => paymentApi.verify(paymentId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['payments', siteId] })
      queryClient.invalidateQueries({ queryKey: ['site', siteId] })
      toast.success('Платеж подтвержден!')
      haptic?.notificationOccurred('success')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Платеж еще не подтвержден')
    },
  })

  const selectedPlanData = plans.find((p: any) => p.id === selectedPlan)

  const calculatePrice = () => {
    if (!selectedPlanData) return 0
    const monthlyPrice = selectedPlanData.price_monthly || 0
    return monthlyPrice * months
  }

  const handleCreatePayment = () => {
    if (!selectedPlanData) return
    createPaymentMutation.mutate()
  }

  if (!site) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-tg-hint" />
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-tg-bg pb-safe">
      {/* Header */}
      <header className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator px-4 py-3 safe-top">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate(-1)}
            className="p-2 -ml-2 rounded-xl hover:bg-tg-secondary-bg"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>
          <h1 className="text-lg font-semibold">Оплата хостинга</h1>
        </div>
      </header>

      <div className="p-4 space-y-4">
        {/* Site Info */}
        <div className="bg-tg-secondary-bg rounded-2xl p-4">
          <h2 className="font-semibold text-tg-text mb-1">{site.company_name}</h2>
          {site.domain && (
            <p className="text-sm text-tg-hint">{site.domain}</p>
          )}
          {site.preview_url && (
            <p className="text-sm text-tg-hint">{site.preview_url}</p>
          )}
        </div>

        {/* Active Payment */}
        {activePayment && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-tg-secondary-bg rounded-2xl p-4 space-y-4"
          >
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-blue-500/20 rounded-xl flex items-center justify-center">
                <QrCode className="w-6 h-6 text-blue-500" />
              </div>
              <div className="flex-1">
                <h3 className="font-semibold text-tg-text">Ожидание оплаты</h3>
                <p className="text-sm text-tg-hint">
                  Сумма: {activePayment.amount} {activePayment.currency || 'RUB'}
                </p>
              </div>
              <Clock className="w-5 h-5 text-orange-500 animate-pulse" />
            </div>

            {/* QR Code */}
            {qrData?.qr_url ? (
              <div className="flex justify-center">
                <img
                  src={qrData.qr_url}
                  alt="QR Code"
                  className="w-64 h-64 bg-white p-4 rounded-2xl"
                />
              </div>
            ) : (
              <div className="flex justify-center">
                <QRCodeCanvas
                  data={activePayment.id + activePayment.amount}
                  size={256}
                />
              </div>
            )}

            {/* Payment Details */}
            <div className="bg-tg-bg rounded-xl p-3 space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-tg-hint">План:</span>
                <span className="font-medium text-tg-text capitalize">
                  {activePayment.plan_id || 'basic'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-tg-hint">Период:</span>
                <span className="font-medium text-tg-text">
                  {activePayment.period_months} {activePayment.period_months === 1 ? 'месяц' : 'месяца'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-tg-hint">Сумма:</span>
                <span className="font-semibold text-tg-text text-lg">
                  {activePayment.amount} {activePayment.currency || 'RUB'}
                </span>
              </div>
            </div>

            {/* Instructions */}
            <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl p-3">
              <p className="text-sm text-blue-500">
                1. Отсканируйте QR код камерой
                <br />
                2. Подтвердите оплату в приложении банка
                <br />
                3. Нажмите "Проверить оплату" после перевода
              </p>
            </div>

            {/* Actions */}
            <div className="flex gap-2">
              <button
                onClick={() => verifyMutation.mutate(activePayment.id)}
                disabled={verifyMutation.isPending}
                className="flex-1 px-4 py-3 bg-green-500 rounded-xl text-white font-medium disabled:opacity-50 flex items-center justify-center gap-2"
              >
                {verifyMutation.isPending ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Проверка...
                  </>
                ) : (
                  <>
                    <RefreshCw className="w-4 h-4" />
                    Проверить оплату
                  </>
                )}
              </button>
            </div>

            {qrData?.payment_url && (
              <a
                href={qrData.payment_url}
                target="_blank"
                rel="noopener noreferrer"
                className="block w-full px-4 py-3 bg-blue-500 rounded-xl text-white font-medium text-center"
              >
                Открыть ссылку для оплаты
              </a>
            )}
          </motion.div>
        )}

        {/* Create Payment Form */}
        {!activePayment && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-tg-secondary-bg rounded-2xl p-4 space-y-4"
          >
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-purple-500/20 rounded-xl flex items-center justify-center">
                <CreditCard className="w-6 h-6 text-purple-500" />
              </div>
              <h3 className="font-semibold text-tg-text">Выберите план</h3>
            </div>

            {/* Plans */}
            <div className="space-y-2">
              {plans.filter((p: any) => p.id !== 'trial' && p.is_active).map((plan: any) => (
                <button
                  key={plan.id}
                  onClick={() => setSelectedPlan(plan.id)}
                  className={`w-full p-4 rounded-xl border-2 transition-all ${
                    selectedPlan === plan.id
                      ? 'border-purple-500 bg-purple-500/10'
                      : 'border-tg-separator bg-tg-bg'
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-semibold text-tg-text">{plan.name}</span>
                    <span className="text-lg font-bold text-tg-text">
                      {plan.price_monthly}₽
                      <span className="text-sm text-tg-hint font-normal">/мес</span>
                    </span>
                  </div>
                  <p className="text-sm text-tg-hint text-left">{plan.description}</p>
                </button>
              ))}
            </div>

            {/* Period */}
            <div className="bg-tg-bg rounded-xl p-3">
              <label className="block text-sm font-medium text-tg-text mb-2">
                Период оплаты
              </label>
              <div className="flex gap-2">
                {[1, 3, 6, 12].map((m) => (
                  <button
                    key={m}
                    onClick={() => setMonths(m)}
                    className={`flex-1 px-3 py-2 rounded-lg font-medium transition-all ${
                      months === m
                        ? 'bg-purple-500 text-white'
                        : 'bg-tg-secondary-bg text-tg-text'
                    }`}
                  >
                    {m} {m === 1 ? 'месяц' : m < 5 ? 'месяца' : 'месяцев'}
                  </button>
                ))}
              </div>
            </div>

            {/* Total */}
            <div className="bg-gradient-to-r from-purple-500/20 to-blue-500/20 rounded-xl p-4">
              <div className="flex justify-between items-center mb-2">
                <span className="text-tg-text">Итого к оплате:</span>
                <span className="text-2xl font-bold text-tg-text">
                  {calculatePrice()}₽
                </span>
              </div>
              {selectedPlanData?.price_yearly && months === 12 && (
                <p className="text-xs text-tg-hint">
                  Экономия {selectedPlanData.price_monthly * 12 - selectedPlanData.price_yearly}₽
                  при годовой оплате!
                </p>
              )}
            </div>

            {/* Create Payment Button */}
            <button
              onClick={handleCreatePayment}
              disabled={createPaymentMutation.isPending || !selectedPlanData}
              className="w-full px-4 py-3 bg-purple-500 rounded-xl text-white font-medium disabled:opacity-50 flex items-center justify-center gap-2"
            >
              {createPaymentMutation.isPending ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Создание...
                </>
              ) : (
                <>
                  <CreditCard className="w-4 h-4" />
                  Оплатить
                </>
              )}
            </button>
          </motion.div>
        )}

        {/* Payment History */}
        {paymentsData?.items && paymentsData.items.length > 0 && (
          <div className="bg-tg-secondary-bg rounded-2xl p-4">
            <h3 className="font-semibold text-tg-text mb-3">История платежей</h3>
            <div className="space-y-2">
              {paymentsData.items
                .filter((p: any) => p.status !== 'pending' && p.status !== 'processing')
                .slice(0, 5)
                .map((payment: any) => (
                  <div
                    key={payment.id}
                    className="flex items-center justify-between p-3 bg-tg-bg rounded-xl"
                  >
                    <div>
                      <p className="text-sm font-medium text-tg-text">
                        {payment.plan_id?.toUpperCase()} • {payment.period_months} мес.
                      </p>
                      <p className="text-xs text-tg-hint">
                        {new Date(payment.created_at).toLocaleDateString('ru-RU')}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-semibold text-tg-text">
                        {payment.amount} {payment.currency || 'RUB'}
                      </p>
                      {payment.status === 'completed' ? (
                        <CheckCircle2 className="w-4 h-4 text-green-500 inline ml-1" />
                      ) : payment.status === 'failed' ? (
                        <AlertCircle className="w-4 h-4 text-red-500 inline ml-1" />
                      ) : null}
                    </div>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

