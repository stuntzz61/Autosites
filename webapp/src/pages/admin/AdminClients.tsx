import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { clientsApi, sitesApi } from '@/api/client'
import {
  UserPlus,
  Users,
  Copy,
  Check,
  RefreshCw,
  Key,
  Trash2,
  Search,
  ExternalLink,
  Building2,
  Globe,
  Eye,
  EyeOff,
  Wand2,
  AlertCircle,
  X
} from 'lucide-react'
import { useState, useEffect } from 'react'
import toast from 'react-hot-toast'
import { motion, AnimatePresence } from 'framer-motion'

interface ClientData {
  registered: boolean
  login?: string
  company_name?: string
  client_name?: string
  created_at?: string
  cms_site_id?: string
  editor_url?: string
}

interface SiteData {
  id: string
  company_name: string
  client_name?: string
  domain?: string
  preview_url?: string
  deploy_status: string
  request_id?: string
}

export default function AdminClients() {
  const queryClient = useQueryClient()
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedSite, setSelectedSite] = useState<SiteData | null>(null)
  const [showCreateModal, setShowCreateModal] = useState(false)

  // Fetch sites with active status (deployed)
  const { data: sitesData, isLoading: sitesLoading } = useQuery({
    queryKey: ['admin', 'sites', 'active'],
    queryFn: () => sitesApi.adminList({
      deploy_status: 'active',
      page: 1,
      limit: 200
    }).then(res => res.data),
  })

  const sites: SiteData[] = sitesData?.items || []

  // Filter sites by search
  const filteredSites = sites.filter((site) => {
    if (!searchQuery) return true
    const query = searchQuery.toLowerCase()
    return (
      site.company_name?.toLowerCase().includes(query) ||
      site.domain?.toLowerCase().includes(query) ||
      site.client_name?.toLowerCase().includes(query)
    )
  })

  if (sitesLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="skeleton h-24 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold text-tg-text">Клиенты CMS</h1>
        <span className="text-sm text-tg-hint">
          Сайтов: {filteredSites.length}
        </span>
      </div>

      {/* Description */}
      <div className="p-3 bg-blue-500/10 border border-blue-500/20 rounded-xl">
        <p className="text-sm text-blue-400">
          Здесь вы можете создавать учетные записи для клиентов,
          чтобы они могли редактировать свои сайты через studio.wenlix.ru
        </p>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-tg-hint" />
        <input
          type="text"
          placeholder="Поиск по компании, домену..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 bg-tg-secondary-bg rounded-xl text-tg-text placeholder-tg-hint border border-tg-separator"
        />
      </div>

      {/* Sites with Client Status */}
      {filteredSites.length === 0 ? (
        <div className="text-center py-12">
          <Users className="w-16 h-16 mx-auto mb-4 text-tg-hint opacity-50" />
          <p className="text-tg-hint">Активных сайтов нет</p>
          <p className="text-sm text-tg-hint mt-2">
            Сначала задеплойте сайт, затем можно создать клиента
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {filteredSites.map((site) => (
            <SiteClientCard
              key={site.id}
              site={site}
              onCreateClient={() => {
                setSelectedSite(site)
                setShowCreateModal(true)
              }}
            />
          ))}
        </div>
      )}

      {/* Create Client Modal */}
      <AnimatePresence>
        {showCreateModal && selectedSite && (
          <CreateClientModal
            site={selectedSite}
            onClose={() => {
              setShowCreateModal(false)
              setSelectedSite(null)
            }}
            onSuccess={() => {
              queryClient.invalidateQueries({ queryKey: ['client', selectedSite.id] })
              setShowCreateModal(false)
              setSelectedSite(null)
            }}
          />
        )}
      </AnimatePresence>
    </div>
  )
}

// Site card with client status
function SiteClientCard({ site, onCreateClient }: { site: SiteData; onCreateClient: () => void }) {
  const { data: clientData, isLoading } = useQuery({
    queryKey: ['client', site.id],
    queryFn: () => clientsApi.getBySite(site.id).then(res => res.data as ClientData),
  })

  const isRegistered = clientData?.registered

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="bg-tg-secondary-bg rounded-2xl p-4 space-y-3"
    >
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <Building2 className="w-4 h-4 text-tg-hint" />
            <h3 className="font-semibold text-tg-text">{site.company_name}</h3>
          </div>
          {site.domain && (
            <a
              href={`https://${site.domain}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-tg-link flex items-center gap-1 mt-1"
            >
              <Globe className="w-3 h-3" />
              {site.domain}
            </a>
          )}
        </div>

        {/* Status Badge */}
        {isLoading ? (
          <div className="w-20 h-6 skeleton rounded-lg" />
        ) : isRegistered ? (
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium bg-green-500 text-white">
            <Check className="w-3 h-3" />
            Клиент есть
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium bg-orange-500 text-white">
            <AlertCircle className="w-3 h-3" />
            Нет клиента
          </span>
        )}
      </div>

      {/* Client Info or Create Button */}
      {isLoading ? (
        <div className="h-16 skeleton rounded-xl" />
      ) : isRegistered && clientData ? (
        <ClientInfo clientData={clientData} siteId={site.id} />
      ) : (
        <button
          onClick={onCreateClient}
          className="w-full py-3 bg-blue-500/20 hover:bg-blue-500/30 text-blue-500 rounded-xl font-medium flex items-center justify-center gap-2 transition-colors"
        >
          <UserPlus className="w-5 h-5" />
          Создать клиента
        </button>
      )}
    </motion.div>
  )
}

// Client info display
function ClientInfo({ clientData, siteId }: { clientData: ClientData; siteId: string }) {
  const queryClient = useQueryClient()
  const [copied, setCopied] = useState(false)

  const resetPasswordMutation = useMutation({
    mutationFn: () => clientsApi.resetPassword(siteId),
    onSuccess: (res) => {
      const newPassword = res.data?.new_password
      if (newPassword) {
        navigator.clipboard.writeText(newPassword)
        toast.success(`Пароль сброшен и скопирован: ${newPassword}`)
      } else {
        toast.success('Пароль сброшен')
      }
      queryClient.invalidateQueries({ queryKey: ['client', siteId] })
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка сброса пароля')
    },
  })

  const copyLogin = () => {
    if (clientData.login) {
      navigator.clipboard.writeText(clientData.login)
      setCopied(true)
      toast.success('Логин скопирован')
      setTimeout(() => setCopied(false), 2000)
    }
  }

  return (
    <div className="space-y-3">
      {/* Login */}
      <div className="flex items-center justify-between p-3 bg-tg-bg rounded-xl">
        <div>
          <span className="text-xs text-tg-hint block">Логин</span>
          <span className="font-mono text-tg-text">{clientData.login}</span>
        </div>
        <button
          onClick={copyLogin}
          className="p-2 hover:bg-tg-secondary-bg rounded-lg transition-colors"
        >
          {copied ? (
            <Check className="w-4 h-4 text-green-500" />
          ) : (
            <Copy className="w-4 h-4 text-tg-hint" />
          )}
        </button>
      </div>

      {/* Actions */}
      <div className="flex gap-2">
        <button
          onClick={() => resetPasswordMutation.mutate()}
          disabled={resetPasswordMutation.isPending}
          className="flex-1 py-2 px-3 bg-orange-500/20 text-orange-500 rounded-lg text-sm font-medium hover:bg-orange-500/30 disabled:opacity-50 flex items-center justify-center gap-1"
        >
          <Key className="w-4 h-4" />
          Сбросить пароль
        </button>

        {clientData.editor_url && (
          <a
            href={clientData.editor_url}
            target="_blank"
            rel="noopener noreferrer"
            className="py-2 px-3 bg-blue-500/20 text-blue-500 rounded-lg text-sm font-medium hover:bg-blue-500/30 flex items-center gap-1"
          >
            <ExternalLink className="w-4 h-4" />
            Редактор
          </a>
        )}
      </div>

      {/* Created at */}
      {clientData.created_at && (
        <p className="text-xs text-tg-hint">
          Создан: {new Date(clientData.created_at).toLocaleDateString('ru-RU')}
        </p>
      )}
    </div>
  )
}

// Create client modal
function CreateClientModal({
  site,
  onClose,
  onSuccess
}: {
  site: SiteData
  onClose: () => void
  onSuccess: () => void
}) {
  const [login, setLogin] = useState('')
  const [password, setPassword] = useState('')
  const [companyName, setCompanyName] = useState(site.company_name)
  const [clientName, setClientName] = useState(site.client_name || '')
  const [showPassword, setShowPassword] = useState(false)
  const [credentialsCopied, setCredentialsCopied] = useState(false)

  // Generate login from company name on mount
  useEffect(() => {
    if (site.company_name) {
      clientsApi.generateLogin(site.company_name)
        .then(res => setLogin(res.data?.login || ''))
        .catch(() => {})
    }
  }, [site.company_name])

  // Generate initial password on mount
  useEffect(() => {
    clientsApi.generatePassword(12)
      .then(res => setPassword(res.data?.password || ''))
      .catch(() => {})
  }, [])

  const generatePasswordMutation = useMutation({
    mutationFn: (length: number = 12) => clientsApi.generatePassword(length),
    onSuccess: (res) => {
      setPassword(res.data?.password || '')
      toast.success('Пароль сгенерирован')
    },
    onError: () => {
      toast.error('Ошибка генерации пароля')
    },
  })

  const createClientMutation = useMutation({
    mutationFn: () => clientsApi.register({
      site_id: site.id,
      company_name: companyName,
      client_name: clientName || undefined,
      login: login || undefined,
      password: password || undefined,
    }),
    onSuccess: (res) => {
      const credentials = res.data?.credentials
      if (credentials) {
        const text = `Логин: ${credentials.login}\nПароль: ${credentials.password}\nРедактор: ${credentials.editor_url}`
        navigator.clipboard.writeText(text)
        toast.success('Клиент создан! Данные скопированы')
      } else {
        toast.success('Клиент создан')
      }
      onSuccess()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка создания клиента')
    },
  })

  const copyCredentials = () => {
    const text = `Логин: ${login}\nПароль: ${password}\nРедактор: https://studio.wenlix.ru/`
    navigator.clipboard.writeText(text)
    setCredentialsCopied(true)
    toast.success('Данные скопированы')
    setTimeout(() => setCredentialsCopied(false), 2000)
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.95, opacity: 0 }}
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-md bg-tg-bg rounded-2xl p-5 space-y-4 max-h-[90vh] overflow-y-auto"
      >
        {/* Header */}
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-bold text-tg-text">Создать клиента</h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-tg-secondary-bg rounded-xl transition-colors"
          >
            <X className="w-5 h-5 text-tg-hint" />
          </button>
        </div>

        {/* Site info */}
        <div className="p-3 bg-tg-secondary-bg rounded-xl">
          <div className="flex items-center gap-2 mb-1">
            <Building2 className="w-4 h-4 text-tg-hint" />
            <span className="font-medium text-tg-text">{site.company_name}</span>
          </div>
          {site.domain && (
            <span className="text-sm text-tg-hint">{site.domain}</span>
          )}
        </div>

        {/* Form */}
        <div className="space-y-4">
          {/* Company Name */}
          <div>
            <label className="block text-sm font-medium text-tg-hint mb-1">
              Название компании
            </label>
            <input
              type="text"
              value={companyName}
              onChange={(e) => setCompanyName(e.target.value)}
              className="w-full px-4 py-2 bg-tg-secondary-bg rounded-xl text-tg-text border border-tg-separator focus:border-blue-500 outline-none"
            />
          </div>

          {/* Client Name */}
          <div>
            <label className="block text-sm font-medium text-tg-hint mb-1">
              Имя клиента (опционально)
            </label>
            <input
              type="text"
              value={clientName}
              onChange={(e) => setClientName(e.target.value)}
              placeholder="Иван Иванов"
              className="w-full px-4 py-2 bg-tg-secondary-bg rounded-xl text-tg-text border border-tg-separator focus:border-blue-500 outline-none placeholder-tg-hint"
            />
          </div>

          {/* Login */}
          <div>
            <label className="block text-sm font-medium text-tg-hint mb-1">
              Логин
            </label>
            <div className="relative">
              <input
                type="text"
                value={login}
                onChange={(e) => setLogin(e.target.value)}
                placeholder="company-name"
                className="w-full px-4 py-2 bg-tg-secondary-bg rounded-xl text-tg-text border border-tg-separator focus:border-blue-500 outline-none placeholder-tg-hint font-mono"
              />
            </div>
            <p className="text-xs text-tg-hint mt-1">
              Латинские буквы, цифры и символ подчеркивания
            </p>
          </div>

          {/* Password */}
          <div>
            <label className="block text-sm font-medium text-tg-hint mb-1">
              Пароль
            </label>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="w-full px-4 py-2 pr-10 bg-tg-secondary-bg rounded-xl text-tg-text border border-tg-separator focus:border-blue-500 outline-none font-mono"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-tg-hint hover:text-tg-text"
                >
                  {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
              <button
                onClick={() => generatePasswordMutation.mutate(12)}
                disabled={generatePasswordMutation.isPending}
                className="px-3 py-2 bg-purple-500/20 text-purple-500 rounded-xl hover:bg-purple-500/30 disabled:opacity-50 transition-colors"
                title="Сгенерировать пароль"
              >
                <Wand2 className="w-5 h-5" />
              </button>
            </div>
          </div>

          {/* Copy credentials before creating */}
          {login && password && (
            <button
              onClick={copyCredentials}
              className="w-full py-2 px-4 bg-tg-secondary-bg hover:bg-gray-700/50 rounded-xl text-sm font-medium text-tg-text flex items-center justify-center gap-2 transition-colors border border-tg-separator"
            >
              {credentialsCopied ? (
                <>
                  <Check className="w-4 h-4 text-green-500" />
                  Скопировано!
                </>
              ) : (
                <>
                  <Copy className="w-4 h-4" />
                  Скопировать данные
                </>
              )}
            </button>
          )}
        </div>

        {/* Actions */}
        <div className="flex gap-3 pt-2">
          <button
            onClick={onClose}
            className="flex-1 py-3 bg-tg-secondary-bg text-tg-text rounded-xl font-medium hover:bg-gray-700/50 transition-colors"
          >
            Отмена
          </button>
          <button
            onClick={() => createClientMutation.mutate()}
            disabled={!login || !password || !companyName || createClientMutation.isPending}
            className="flex-1 py-3 bg-blue-500 text-white rounded-xl font-medium hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 transition-colors"
          >
            {createClientMutation.isPending ? (
              <RefreshCw className="w-5 h-5 animate-spin" />
            ) : (
              <>
                <UserPlus className="w-5 h-5" />
                Создать
              </>
            )}
          </button>
        </div>
      </motion.div>
    </motion.div>
  )
}

