-- Migration: Invite Codes System for Manager Registration
-- Description: Adds invite codes functionality allowing admins to create
--              invite links that auto-register managers into specific groups

-- Enable UUID extension if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create invite_codes table first (before referencing it)
CREATE TABLE IF NOT EXISTS invite_codes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- The actual invite code (e.g., "ABC123XY")
    code VARCHAR(20) UNIQUE NOT NULL,

    -- Optional human-readable name
    name VARCHAR(255),

    -- Which group will new users be added to
    group_id UUID REFERENCES admin_groups(id) ON DELETE SET NULL,

    -- Who created this invite code
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,

    -- Usage limits
    max_uses INT, -- NULL = unlimited
    uses_count INT DEFAULT 0,

    -- Expiration
    expires_at TIMESTAMPTZ,

    -- Auto-approve users registered with this code
    auto_approve BOOLEAN DEFAULT FALSE,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Private notes for admin
    notes TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create invite_code_usage table to track who used which code
CREATE TABLE IF NOT EXISTS invite_code_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invite_code_id UUID NOT NULL REFERENCES invite_codes(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    used_at TIMESTAMPTZ DEFAULT NOW(),

    -- Each user can only use each code once
    UNIQUE(invite_code_id, user_id)
);

-- Add registered_via_code column to users table (after invite_codes exists)
ALTER TABLE users
ADD COLUMN IF NOT EXISTS registered_via_code UUID REFERENCES invite_codes(id) ON DELETE SET NULL;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_invite_codes_code ON invite_codes(code);
CREATE INDEX IF NOT EXISTS idx_invite_codes_group_id ON invite_codes(group_id);
CREATE INDEX IF NOT EXISTS idx_invite_codes_created_by ON invite_codes(created_by);
CREATE INDEX IF NOT EXISTS idx_invite_codes_active ON invite_codes(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_invite_code_usage_code ON invite_code_usage(invite_code_id);
CREATE INDEX IF NOT EXISTS idx_invite_code_usage_user ON invite_code_usage(user_id);
CREATE INDEX IF NOT EXISTS idx_users_registered_via ON users(registered_via_code) WHERE registered_via_code IS NOT NULL;

-- Create trigger to update updated_at
CREATE OR REPLACE FUNCTION update_invite_codes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_invite_codes_updated_at ON invite_codes;
CREATE TRIGGER trigger_invite_codes_updated_at
    BEFORE UPDATE ON invite_codes
    FOR EACH ROW
    EXECUTE FUNCTION update_invite_codes_updated_at();

-- Comment on tables
COMMENT ON TABLE invite_codes IS 'Invite codes for manager registration with group assignment';
COMMENT ON TABLE invite_code_usage IS 'Tracks which users used which invite codes';

-- Comment on columns
COMMENT ON COLUMN invite_codes.code IS 'Unique invite code string, auto-generated uppercase alphanumeric';
COMMENT ON COLUMN invite_codes.group_id IS 'Admin group to auto-assign new managers to';
COMMENT ON COLUMN invite_codes.max_uses IS 'Maximum number of registrations allowed, NULL for unlimited';
COMMENT ON COLUMN invite_codes.auto_approve IS 'If true, users registering with this code are auto-approved';
COMMENT ON COLUMN users.registered_via_code IS 'Reference to the invite code used during registration';
