import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { sitesApi } from '@/api/client'
import {
  Globe,
  Power,
  RotateCcw,
  Trash2,
  ExternalLink,
  AlertCircle,
  CheckCircle2,
  Clock,
  XCircle,
  RefreshCw,
  Search,
  Filter
} from 'lucide-react'
import { useState } from 'react'
import toast from 'react-hot-toast'
import { motion } from 'framer-motion'

export default function AdminSites() {
  const queryClient = useQueryClient()
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('all')

  const { data, isLoading } = useQuery({
    queryKey: ['admin', 'sites', statusFilter],
    queryFn: () => sitesApi.adminList({
      deploy_status: statusFilter === 'all' ? undefined : statusFilter,
      page: 1,
      limit: 100
    }).then(res => res.data),
  })

  const sites = data?.items || []

  // Filter by search query
  const filteredSites = sites.filter((site: any) => {
    if (!searchQuery) return true
    const query = searchQuery.toLowerCase()
    return (
      site.company_name?.toLowerCase().includes(query) ||
      site.domain?.toLowerCase().includes(query) ||
      site.preview_slug?.toLowerCase().includes(query)
    )
  })

  const deployMutation = useMutation({
    mutationFn: (siteId: string) => sitesApi.deploy(siteId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'sites'] })
      toast.success('Деплой запущен')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка деплоя')
    },
  })

  const stopMutation = useMutation({
    mutationFn: (siteId: string) => sitesApi.stop(siteId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'sites'] })
      toast.success('Сайт остановлен')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (siteId: string) => sitesApi.delete(siteId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'sites'] })
      toast.success('Сайт удален')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка удаления')
    },
  })

  const getStatusBadge = (status: string) => {
    const badges = {
      active: { icon: CheckCircle2, color: 'bg-green-500', label: 'Активен' },
      deploying: { icon: RefreshCw, color: 'bg-blue-500', label: 'Деплоится' },
      pending: { icon: Clock, color: 'bg-orange-500', label: 'Ожидает' },
      failed: { icon: XCircle, color: 'bg-red-500', label: 'Ошибка' },
      stopped: { icon: Power, color: 'bg-gray-500', label: 'Остановлен' },
      none: { icon: AlertCircle, color: 'bg-gray-400', label: 'Не задеплоен' },
    }
    const badge = badges[status as keyof typeof badges] || badges.none
    const Icon = badge.icon

    return (
      <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium ${badge.color} text-white`}>
        <Icon className="w-3 h-3" />
        {badge.label}
      </span>
    )
  }

  if (isLoading) {
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
        <h1 className="text-2xl font-bold text-tg-text">Управление сайтами</h1>
        <span className="text-sm text-tg-hint">
          Всего: {filteredSites.length}
        </span>
      </div>

      {/* Search and Filter */}
      <div className="space-y-2">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-tg-hint" />
          <input
            type="text"
            placeholder="Поиск по названию, домену..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 bg-tg-secondary-bg rounded-xl text-tg-text placeholder-tg-hint border border-tg-separator"
          />
        </div>

        <div className="flex gap-2 overflow-x-auto pb-2">
          {['all', 'active', 'deploying', 'failed', 'stopped', 'none'].map((status) => (
            <button
              key={status}
              onClick={() => setStatusFilter(status)}
              className={`px-3 py-1 rounded-lg text-sm font-medium whitespace-nowrap ${
                statusFilter === status
                  ? 'bg-black dark:bg-white text-white dark:text-black'
                  : 'bg-tg-secondary-bg text-tg-hint'
              }`}
            >
              {status === 'all' ? 'Все' : getStatusBadge(status).props.children[1]}
            </button>
          ))}
        </div>
      </div>

      {/* Sites List */}
      {filteredSites.length === 0 ? (
        <div className="text-center py-12">
          <Globe className="w-16 h-16 mx-auto mb-4 text-tg-hint opacity-50" />
          <p className="text-tg-hint">Сайты не найдены</p>
        </div>
      ) : (
        <div className="space-y-3">
          {filteredSites.map((site: any) => (
            <motion.div
              key={site.id}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              className="bg-tg-secondary-bg rounded-2xl p-4 space-y-3"
            >
              {/* Header */}
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <h3 className="font-semibold text-tg-text">{site.company_name}</h3>
                  <p className="text-sm text-tg-hint mt-1">
                    {site.client_name && `${site.client_name} • `}
                    ID: {site.id.slice(0, 8)}
                  </p>
                </div>
                {getStatusBadge(site.deploy_status)}
              </div>

              {/* URLs */}
              {(site.preview_url || site.domain) && (
                <div className="flex flex-wrap gap-2">
                  {site.preview_url && (
                    <a
                      href={site.preview_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 px-3 py-1.5 bg-tg-bg rounded-lg text-sm text-tg-link"
                    >
                      <ExternalLink className="w-3 h-3" />
                      Preview
                    </a>
                  )}
                  {site.domain && (
                    <a
                      href={`https://${site.domain}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 px-3 py-1.5 bg-tg-bg rounded-lg text-sm text-tg-link"
                    >
                      <Globe className="w-3 h-3" />
                      {site.domain}
                    </a>
                  )}
                </div>
              )}

              {/* Info */}
              <div className="grid grid-cols-2 gap-2 text-xs text-tg-hint">
                <div>
                  <span className="block">План:</span>
                  <span className="font-medium text-tg-text capitalize">{site.hosting_plan || 'trial'}</span>
                </div>
                <div>
                  <span className="block">Срок:</span>
                  <span className="font-medium text-tg-text">
                    {site.hosting_expires_at
                      ? new Date(site.hosting_expires_at).toLocaleDateString('ru-RU')
                      : '—'}
                  </span>
                </div>
                {site.server_name && (
                  <>
                    <div>
                      <span className="block">Сервер:</span>
                      <span className="font-medium text-tg-text">{site.server_name}</span>
                    </div>
                    {site.container_port && (
                      <div>
                        <span className="block">Порт:</span>
                        <span className="font-medium text-tg-text">{site.container_port}</span>
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* Actions */}
              <div className="flex flex-wrap gap-2 pt-2 border-t border-tg-separator">
                {site.deploy_status === 'active' && (
                  <>
                    <button
                      onClick={() => stopMutation.mutate(site.id)}
                      disabled={stopMutation.isPending}
                      className="flex-1 px-3 py-2 bg-orange-500/20 text-orange-500 rounded-lg text-sm font-medium hover:bg-orange-500/30 disabled:opacity-50"
                    >
                      <Power className="w-4 h-4 inline mr-1" />
                      Остановить
                    </button>
                    <button
                      onClick={() => deployMutation.mutate(site.id)}
                      disabled={deployMutation.isPending}
                      className="flex-1 px-3 py-2 bg-blue-500/20 text-blue-500 rounded-lg text-sm font-medium hover:bg-blue-500/30 disabled:opacity-50"
                    >
                      <RotateCcw className="w-4 h-4 inline mr-1" />
                      Перезапустить
                    </button>
                  </>
                )}

                {site.deploy_status === 'stopped' && (
                  <button
                    onClick={() => deployMutation.mutate(site.id)}
                    disabled={deployMutation.isPending}
                    className="flex-1 px-3 py-2 bg-green-500/20 text-green-500 rounded-lg text-sm font-medium hover:bg-green-500/30 disabled:opacity-50"
                  >
                    <Power className="w-4 h-4 inline mr-1" />
                    Запустить
                  </button>
                )}

                {(site.deploy_status === 'failed' || site.deploy_status === 'none') && (
                  <button
                    onClick={() => deployMutation.mutate(site.id)}
                    disabled={deployMutation.isPending}
                    className="flex-1 px-3 py-2 bg-blue-500/20 text-blue-500 rounded-lg text-sm font-medium hover:bg-blue-500/30 disabled:opacity-50"
                  >
                    <RefreshCw className="w-4 h-4 inline mr-1" />
                    Задеплоить
                  </button>
                )}

                <button
                  onClick={() => {
                    if (confirm('Удалить сайт навсегда? Это действие нельзя отменить.')) {
                      deleteMutation.mutate(site.id)
                    }
                  }}
                  disabled={deleteMutation.isPending}
                  className="px-3 py-2 bg-red-500/20 text-red-500 rounded-lg text-sm font-medium hover:bg-red-500/30 disabled:opacity-50"
                >
                  <Trash2 className="w-4 h-4 inline mr-1" />
                  Удалить
                </button>
              </div>

              {/* Error */}
              {site.last_error && (
                <div className="p-2 bg-red-500/10 border border-red-500/20 rounded-lg">
                  <p className="text-xs text-red-500">{site.last_error}</p>
                </div>
              )}
            </motion.div>
          ))}
        </div>
      )}
    </div>
  )
}

