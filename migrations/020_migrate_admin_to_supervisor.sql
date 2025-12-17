-- =====================================================
-- Migration: Migrate existing 'admin' role to 'supervisor'
-- =====================================================

-- Update all users with 'admin' role to 'supervisor'
-- This maintains backward compatibility while using new role system
UPDATE users
SET role = 'supervisor'
WHERE role = 'admin';

-- Note: user_group_membership.role is for group membership role (admin/member),
-- not user role, so it doesn't need to be changed

