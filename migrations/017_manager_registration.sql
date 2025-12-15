-- Migration: Manager Registration and Workspace Provisioning
-- Description: Adds manager profile data (FIO, phone) and workspace/tenant system
-- for self-registration via invite links

-- ==================== Manager Profiles ====================

-- Add profile fields to existing users table
ALTER TABLE users
ADD COLUMN IF NOT EXISTS full_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS phone VARCHAR(50),
ADD COLUMN IF NOT EXISTS phone_verified BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS registration_completed_at TIMESTAMP WITH TIME ZONE;

-- Create index for phone lookup
CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone) WHERE phone IS NOT NULL;

-- ==================== Workspaces (Tenants) ====================

CREATE TABLE IF NOT EXISTS workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Basic info
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,

    -- Owner (manager)
    owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Settings
    settings JSONB DEFAULT '{}',

    -- Limits/quotas
    max_requests INT DEFAULT 100,
    max_sites INT DEFAULT 10,
    max_storage_mb INT DEFAULT 1000,

    -- Status
    status VARCHAR(50) DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_slug ON workspaces(slug);
CREATE INDEX IF NOT EXISTS idx_workspaces_status ON workspaces(status);

-- ==================== Link Users to Workspaces ====================

-- Add workspace_id to users table
ALTER TABLE users
ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_users_workspace ON users(workspace_id);

-- ==================== Manager Invites (Extended) ====================

-- Add registration-specific fields to invite_codes
ALTER TABLE invite_codes
ADD COLUMN IF NOT EXISTS requires_registration BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS registration_data JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS activated_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS activated_by UUID REFERENCES users(id) ON DELETE SET NULL;

-- ==================== Workspace Resources ====================

-- Optionally track workspace-specific resources
CREATE TABLE IF NOT EXISTS workspace_resources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,

    resource_type VARCHAR(50) NOT NULL, -- 'request', 'site', 'storage', etc.
    resource_id UUID, -- Reference to the actual resource

    -- Usage tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workspace_resources_workspace ON workspace_resources(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspace_resources_type ON workspace_resources(workspace_id, resource_type);

-- ==================== Updated_at Triggers ====================

CREATE OR REPLACE FUNCTION update_workspaces_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_workspaces_updated_at ON workspaces;
CREATE TRIGGER trigger_workspaces_updated_at
    BEFORE UPDATE ON workspaces
    FOR EACH ROW
    EXECUTE FUNCTION update_workspaces_updated_at();

-- ==================== Comments ====================

COMMENT ON TABLE workspaces IS 'Tenant workspaces for manager isolation';
COMMENT ON COLUMN workspaces.slug IS 'URL-friendly unique identifier';
COMMENT ON COLUMN workspaces.settings IS 'Workspace-specific settings (JSON)';
COMMENT ON COLUMN workspaces.max_requests IS 'Maximum number of requests allowed';
COMMENT ON COLUMN workspaces.max_sites IS 'Maximum number of deployed sites';

COMMENT ON COLUMN users.full_name IS 'Manager full name (FIO) for registration';
COMMENT ON COLUMN users.phone IS 'Manager phone number';
COMMENT ON COLUMN users.workspace_id IS 'Workspace/tenant this user belongs to';
COMMENT ON COLUMN users.registration_completed_at IS 'When registration form was completed';

COMMENT ON COLUMN invite_codes.requires_registration IS 'Whether this invite requires full registration form';
COMMENT ON COLUMN invite_codes.activated_at IS 'When the invite was first activated';
COMMENT ON COLUMN invite_codes.activated_by IS 'User who activated this invite';

