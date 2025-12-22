-- Migration: Add tariff selection to requests
-- Description: Adds tariff field to requests table for standard/premium generation

-- Add tariff column to requests
ALTER TABLE requests
ADD COLUMN IF NOT EXISTS tariff VARCHAR(20) DEFAULT 'standard' CHECK (tariff IN ('standard', 'premium'));

-- Add index for tariff queries
CREATE INDEX IF NOT EXISTS idx_requests_tariff ON requests(tariff);

-- Comment on column
COMMENT ON COLUMN requests.tariff IS 'Generation tariff: standard (free) or premium (paid)';

-- Update existing requests to have standard tariff
UPDATE requests SET tariff = 'standard' WHERE tariff IS NULL;










