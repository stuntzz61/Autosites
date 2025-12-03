import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { adminApi } from '@/api/client'
import { Search, FileText, Archive, Trash2, ChevronRight } from 'lucide-react'
import toast from 'react-hot-toast'

export default function AdminRequests() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [selectedIds, setSelectedIds] = useState<string[]>([])

  const { data, isLoading } = useQuery({
    queryKey: ['admin', 'requests'],
    queryFn: () => adminApi.requests.list({ limit: 100 }).then(res => res.data),
  })

  const { data: searchResults } = useQuery({
    queryKey: ['admin', 'requests', 'search', search],
    queryFn: () => adminApi.requests.search(search).then(res => res.data),
    enabled: search.length > 2,
  })

  const massArchiveMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massArchive(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'requests'] })
      setSelectedIds([])
      toast.success('Заявки архивированы')
    },
  })

  const massDeleteMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massDelete(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'requests'] })
      setSelectedIds([])
      toast.success('Заявки удалены')
    },
  })

  const requests = search.length > 2 ? searchResults : data?.items

  const toggleSelect = (id: string) => {
    setSelectedIds(prev =>
      prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
    )
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="skeleton h-20 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Все заявки</h1>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Поиск по компании или клиенту..."
          className="w-full pl-10 pr-4 py-3 bg-tg-secondary-bg rounded-xl text-tg-text placeholder:text-tg-hint"
        />
      </div>

      {/* Mass actions */}
      {selectedIds.length > 0 && (
        <div className="flex gap-2 p-3 bg-tg-secondary-bg rounded-xl">
          <span className="text-sm text-tg-text flex-1">
            Выбрано: {selectedIds.length}
          </span>
          <button
            onClick={() => massArchiveMutation.mutate(selectedIds)}
            className="p-2 rounded-lg bg-orange-500/20 text-orange-500"
          >
            <Archive className="w-4 h-4" />
          </button>
          <button
            onClick={() => {
              if (confirm('Удалить выбранные заявки?')) {
                massDeleteMutation.mutate(selectedIds)
              }
            }}
            className="p-2 rounded-lg bg-red-500/20 text-red-500"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Requests list */}
      <div className="space-y-3">
        {requests?.length === 0 ? (
          <p className="text-center text-tg-hint py-8">Нет заявок</p>
        ) : (
          requests?.map((request: any) => (
            <div
              key={request.id}
              className="bg-tg-secondary-bg rounded-2xl p-4 flex items-center gap-3"
            >
              <input
                type="checkbox"
                checked={selectedIds.includes(request.id)}
                onChange={() => toggleSelect(request.id)}
                className="w-5 h-5 rounded"
              />
              <div
                className="flex-1 cursor-pointer"
                onClick={() => navigate(`/requests/${request.id}`)}
              >
                <div className="flex items-center gap-2">
                  <FileText className="w-4 h-4 text-tg-hint" />
                  <p className="font-medium text-tg-text">
                    {request.company_name || 'Без названия'}
                  </p>
                </div>
                <p className="text-sm text-tg-hint">
                  {request.client_name} • {request.status}
                </p>
                {request.manager_first_name && (
                  <p className="text-xs text-tg-hint">
                    Менеджер: {request.manager_first_name}
                  </p>
                )}
              </div>
              <ChevronRight className="w-5 h-5 text-tg-hint" />
            </div>
          ))
        )}
      </div>
    </div>
  )
}

