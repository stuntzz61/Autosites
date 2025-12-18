-- =====================================================
-- Migration: Add chat_id to requests table
-- =====================================================

-- Add chat_id column to requests table
ALTER TABLE requests
ADD COLUMN IF NOT EXISTS chat_id BIGINT;

-- Add index for chat_id
CREATE INDEX IF NOT EXISTS idx_requests_chat_id ON requests(chat_id);

-- Add comment
COMMENT ON COLUMN requests.chat_id IS 'Telegram chat ID where request was created (for notifications)';

