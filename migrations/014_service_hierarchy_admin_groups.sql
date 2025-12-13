-- =====================================================
-- Migration: Service Hierarchy, Admin-Manager Groups, Anti-nuke
-- =====================================================

-- 1. Service Categories (для иерархии услуг)
CREATE TABLE IF NOT EXISTS service_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES service_categories(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    icon VARCHAR(50),
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_service_categories_parent ON service_categories(parent_id);

-- 2. Admin Groups (группы для связи админов с менеджерами)
CREATE TABLE IF NOT EXISTS admin_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 3. User Group Membership (связь пользователей с группами)
CREATE TABLE IF NOT EXISTS user_group_membership (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id UUID NOT NULL REFERENCES admin_groups(id) ON DELETE CASCADE,
    role VARCHAR(20) DEFAULT 'member' CHECK (role IN ('admin', 'member')),
    added_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_user_group_user ON user_group_membership(user_id);
CREATE INDEX IF NOT EXISTS idx_user_group_group ON user_group_membership(group_id);

-- 4. Add group_id to users table for quick lookup
ALTER TABLE users ADD COLUMN IF NOT EXISTS admin_group_id UUID REFERENCES admin_groups(id) ON DELETE SET NULL;

-- 5. Deletion Audit Log (для anti-nuke)
CREATE TABLE IF NOT EXISTS deletion_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_type VARCHAR(50) NOT NULL, -- 'delete_request', 'delete_manager', 'mass_delete', etc.
    target_type VARCHAR(50) NOT NULL, -- 'request', 'user', 'site', etc.
    target_id UUID,
    target_ids UUID[], -- для массовых операций
    target_count INTEGER DEFAULT 1,
    performed_by UUID NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    reason TEXT,
    ip_address VARCHAR(45),
    user_agent TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deletion_audit_performer ON deletion_audit_log(performed_by);
CREATE INDEX IF NOT EXISTS idx_deletion_audit_type ON deletion_audit_log(action_type);
CREATE INDEX IF NOT EXISTS idx_deletion_audit_created ON deletion_audit_log(created_at DESC);

-- 6. Anti-nuke settings
CREATE TABLE IF NOT EXISTS anti_nuke_settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    setting_key VARCHAR(100) UNIQUE NOT NULL,
    setting_value TEXT NOT NULL,
    description TEXT,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Default anti-nuke settings
INSERT INTO anti_nuke_settings (setting_key, setting_value, description) VALUES
    ('max_bulk_delete_requests', '10', 'Maximum number of requests that can be deleted at once'),
    ('max_bulk_delete_managers', '1', 'Maximum number of managers that can be deleted at once (must be 1 for protection)'),
    ('require_confirmation_above', '5', 'Require double confirmation for deleting more than N items'),
    ('min_managers_count', '1', 'Minimum number of managers that must remain in the system'),
    ('deletion_cooldown_seconds', '30', 'Cooldown between bulk delete operations')
ON CONFLICT (setting_key) DO NOTHING;

-- 7. Add photo_required field to additional_services
ALTER TABLE additional_services ADD COLUMN IF NOT EXISTS photo_required BOOLEAN DEFAULT FALSE;

-- 8. Add category fields to services in requests
-- This is handled via JSONB payload, no schema change needed

-- 9. Triggers for updated_at
CREATE TRIGGER set_timestamp_service_categories
    BEFORE UPDATE ON service_categories
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_timestamp();

CREATE TRIGGER set_timestamp_admin_groups
    BEFORE UPDATE ON admin_groups
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_timestamp();

-- Comments
COMMENT ON TABLE service_categories IS 'Hierarchical service categories for requests';
COMMENT ON TABLE admin_groups IS 'Groups linking admins with managers they can manage';
COMMENT ON TABLE user_group_membership IS 'Many-to-many relationship between users and groups';
COMMENT ON TABLE deletion_audit_log IS 'Audit log for all deletion operations (anti-nuke)';
COMMENT ON TABLE anti_nuke_settings IS 'Configuration for anti-nuke protection';

