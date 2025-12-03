// Unified status definitions for the entire application

export const STATUS = {
  DRAFT: 'draft',
  COLLECTING_INFO: 'collecting_info',
  COLLECTING_PHOTOS: 'collecting_photos',
  AWAITING_PHOTOS: 'awaiting_photos',
  READY: 'ready_to_generate',
  IN_QUEUE: 'in_queue',
  GENERATING: 'generating',
  SUCCESS: 'success',
  ERROR: 'error',
  ARCHIVED: 'archived',
  CLOSED: 'closed',
} as const

export type StatusType = typeof STATUS[keyof typeof STATUS]

// Status display configuration
export const statusConfig: Record<string, { label: string; color: string; icon: string }> = {
  [STATUS.DRAFT]: { label: 'Черновик', color: 'gray', icon: 'clock' },
  [STATUS.COLLECTING_INFO]: { label: 'Сбор данных', color: 'blue', icon: 'clock' },
  [STATUS.COLLECTING_PHOTOS]: { label: 'Сбор фото', color: 'blue', icon: 'image' },
  [STATUS.AWAITING_PHOTOS]: { label: 'Ожидание фото', color: 'amber', icon: 'image' },
  [STATUS.READY]: { label: 'Готов к генерации', color: 'green', icon: 'check' },
  [STATUS.IN_QUEUE]: { label: 'В очереди', color: 'purple', icon: 'clock' },
  [STATUS.GENERATING]: { label: 'Генерация', color: 'purple', icon: 'loader' },
  [STATUS.SUCCESS]: { label: 'Готово', color: 'emerald', icon: 'check-circle' },
  [STATUS.ERROR]: { label: 'Ошибка', color: 'red', icon: 'alert-circle' },
  [STATUS.ARCHIVED]: { label: 'Архив', color: 'gray', icon: 'archive' },
  [STATUS.CLOSED]: { label: 'Закрыто', color: 'gray', icon: 'x' },
}

// Normalize legacy status names to current ones
export function normalizeStatus(status: string | undefined | null): StatusType {
  if (!status) return STATUS.DRAFT
  
  const mapping: Record<string, StatusType> = {
    // Legacy names
    'generated_ok': STATUS.SUCCESS,
    'generated_error': STATUS.ERROR,
    'ready': STATUS.READY,
    'in_progress': STATUS.GENERATING,
    'pending': STATUS.DRAFT,
    // Current names (passthrough)
    [STATUS.DRAFT]: STATUS.DRAFT,
    [STATUS.COLLECTING_INFO]: STATUS.COLLECTING_INFO,
    [STATUS.COLLECTING_PHOTOS]: STATUS.COLLECTING_PHOTOS,
    [STATUS.AWAITING_PHOTOS]: STATUS.AWAITING_PHOTOS,
    [STATUS.READY]: STATUS.READY,
    [STATUS.IN_QUEUE]: STATUS.IN_QUEUE,
    [STATUS.GENERATING]: STATUS.GENERATING,
    [STATUS.SUCCESS]: STATUS.SUCCESS,
    [STATUS.ERROR]: STATUS.ERROR,
    [STATUS.ARCHIVED]: STATUS.ARCHIVED,
    [STATUS.CLOSED]: STATUS.CLOSED,
  }
  
  return mapping[status.toLowerCase()] || STATUS.DRAFT
}

// Get status from request (handles both old and new storage)
export function getRequestStatus(request: any): StatusType {
  const payloadStatus = request?.payload?.site?.meta?.status
  const fieldStatus = request?.status
  return normalizeStatus(payloadStatus || fieldStatus)
}

// Status groups for filtering
export const STATUS_GROUPS = {
  ACTIVE: [STATUS.DRAFT, STATUS.COLLECTING_INFO, STATUS.COLLECTING_PHOTOS, STATUS.AWAITING_PHOTOS, STATUS.READY],
  IN_PROGRESS: [STATUS.IN_QUEUE, STATUS.GENERATING],
  COMPLETED: [STATUS.SUCCESS],
  PROBLEM: [STATUS.ERROR],
  INACTIVE: [STATUS.ARCHIVED, STATUS.CLOSED],
}

// Can send to generation
export function canGenerate(status: StatusType): boolean {
  return STATUS_GROUPS.ACTIVE.includes(status)
}

// Can edit
export function canEdit(status: StatusType): boolean {
  return STATUS_GROUPS.ACTIVE.includes(status)
}

// Can delete (for managers)
export function canDelete(status: StatusType, isAdmin: boolean): boolean {
  if (isAdmin) return true
  return [STATUS.DRAFT, STATUS.ERROR].includes(status)
}

