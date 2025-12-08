-- =====================================================
-- Migration: Make request_id optional in client_sites
-- Allows creating client_sites from deploy callbacks
-- without existing request (for imported deploys)
-- =====================================================

-- Make request_id nullable
ALTER TABLE client_sites
    ALTER COLUMN request_id DROP NOT NULL;

-- Drop the CASCADE constraint and recreate with SET NULL
ALTER TABLE client_sites
    DROP CONSTRAINT IF EXISTS client_sites_request_id_fkey;

ALTER TABLE client_sites
    ADD CONSTRAINT client_sites_request_id_fkey
    FOREIGN KEY (request_id)
    REFERENCES requests(id)
    ON DELETE SET NULL;

-- Add index on deploy_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_client_sites_deploy_id_btree ON client_sites(deploy_id);

COMMENT ON COLUMN client_sites.request_id IS 'Optional: linked request. NULL for imported sites without request.';

