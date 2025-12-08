-- =====================================================
-- Migration: Integration Improvements
-- Ensures all required fields for deploy integration exist
-- =====================================================

-- Add server_host to client_sites if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'client_sites' AND column_name = 'server_host'
    ) THEN
        ALTER TABLE client_sites ADD COLUMN server_host VARCHAR(255);
        COMMENT ON COLUMN client_sites.server_host IS 'IP адрес сервера для DNS настройки домена';
    END IF;
END $$;

-- Add container_port to client_sites if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'client_sites' AND column_name = 'container_port'
    ) THEN
        ALTER TABLE client_sites ADD COLUMN container_port INTEGER;
        COMMENT ON COLUMN client_sites.container_port IS 'Порт контейнера на сервере';
    END IF;
END $$;

-- Create index for faster lookups by preview_slug if not exists
CREATE INDEX IF NOT EXISTS idx_client_sites_preview_slug ON client_sites(preview_slug);

-- Create index for deploy_id lookups
CREATE INDEX IF NOT EXISTS idx_client_sites_deploy_id ON client_sites(deploy_id);

-- Ensure qr_image_data field exists in hosting_transactions
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'hosting_transactions' AND column_name = 'qr_image_data'
    ) THEN
        ALTER TABLE hosting_transactions ADD COLUMN qr_image_data TEXT;
        COMMENT ON COLUMN hosting_transactions.qr_image_data IS 'Base64 encoded QR image for payment';
    END IF;
END $$;

-- Update view for active sites with expiring hosting (include server info)
CREATE OR REPLACE VIEW expiring_sites AS
SELECT
    cs.id,
    cs.company_name,
    cs.domain,
    cs.preview_url,
    cs.server_host,
    cs.server_name,
    cs.hosting_plan,
    cs.hosting_expires_at,
    cs.hosting_auto_renew,
    cs.manager_id,
    u.first_name as manager_first_name,
    u.last_name as manager_last_name,
    u.tg_id as manager_tg_id,
    EXTRACT(DAY FROM cs.hosting_expires_at - NOW()) as days_remaining
FROM client_sites cs
JOIN users u ON u.id = cs.manager_id
WHERE cs.deploy_status = 'active'
  AND cs.hosting_expires_at IS NOT NULL
  AND cs.hosting_expires_at <= NOW() + INTERVAL '14 days'
ORDER BY cs.hosting_expires_at ASC;

COMMENT ON VIEW expiring_sites IS 'Sites with hosting expiring within 14 days';

