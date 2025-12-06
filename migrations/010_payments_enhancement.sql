-- =====================================================
-- Migration: Payments Enhancement - QR codes and payment tracking
-- =====================================================

-- Add payment fields to hosting_transactions
ALTER TABLE hosting_transactions
  ADD COLUMN IF NOT EXISTS qr_code_url TEXT,
  ADD COLUMN IF NOT EXISTS payment_url TEXT,
  ADD COLUMN IF NOT EXISTS qr_image_data TEXT,  -- Base64 encoded QR image
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP,  -- QR code expiration
  ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS payment_system VARCHAR(50) DEFAULT 'manual';  -- manual, yookassa, etc.

-- Add indexes for payment queries
CREATE INDEX IF NOT EXISTS idx_hosting_transactions_status ON hosting_transactions(status);
CREATE INDEX IF NOT EXISTS idx_hosting_transactions_expires ON hosting_transactions(expires_at);
CREATE INDEX IF NOT EXISTS idx_hosting_transactions_created ON hosting_transactions(created_at DESC);

-- Add auto-disable/delete tracking to client_sites
ALTER TABLE client_sites
  ADD COLUMN IF NOT EXISTS payment_warning_sent_at TIMESTAMP,  -- When warning was sent (2 weeks before expiry)
  ADD COLUMN IF NOT EXISTS auto_disabled_at TIMESTAMP,  -- When site was auto-disabled (2 weeks after expiry)
  ADD COLUMN IF NOT EXISTS scheduled_for_deletion_at TIMESTAMP;  -- When site will be deleted (2 months after expiry)

CREATE INDEX IF NOT EXISTS idx_client_sites_scheduled_deletion ON client_sites(scheduled_for_deletion_at);
CREATE INDEX IF NOT EXISTS idx_client_sites_payment_warning ON client_sites(payment_warning_sent_at);

-- Function to calculate next payment warning date
CREATE OR REPLACE FUNCTION should_send_payment_warning()
RETURNS TABLE(site_id UUID, company_name VARCHAR, manager_tg_id BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status = 'active'
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at <= NOW() + INTERVAL '14 days'
    AND cs.hosting_expires_at > NOW()
    AND (cs.payment_warning_sent_at IS NULL OR cs.payment_warning_sent_at < cs.hosting_expires_at - INTERVAL '13 days');
END;
$$ LANGUAGE plpgsql;

-- Function to get sites that should be auto-disabled
CREATE OR REPLACE FUNCTION sites_to_auto_disable()
RETURNS TABLE(site_id UUID, company_name VARCHAR, manager_tg_id BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status = 'active'
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at < NOW() - INTERVAL '14 days'
    AND cs.auto_disabled_at IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to get sites scheduled for deletion
CREATE OR REPLACE FUNCTION sites_to_delete()
RETURNS TABLE(site_id UUID, company_name VARCHAR, manager_tg_id BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status IN ('active', 'stopped')
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at < NOW() - INTERVAL '60 days'
    AND cs.scheduled_for_deletion_at IS NOT NULL
    AND cs.scheduled_for_deletion_at <= NOW();
END;
$$ LANGUAGE plpgsql;

COMMENT ON COLUMN hosting_transactions.qr_code_url IS 'URL QR кода для оплаты';
COMMENT ON COLUMN hosting_transactions.payment_url IS 'URL для прямого перехода к оплате';
COMMENT ON COLUMN hosting_transactions.expires_at IS 'Дата истечения QR кода';
COMMENT ON COLUMN client_sites.payment_warning_sent_at IS 'Когда отправлено предупреждение об истечении (за 2 недели)';
COMMENT ON COLUMN client_sites.auto_disabled_at IS 'Когда сайт был автоматически отключен (через 2 недели после истечения)';
COMMENT ON COLUMN client_sites.scheduled_for_deletion_at IS 'Когда сайт будет удален (через 2 месяца после истечения)';

