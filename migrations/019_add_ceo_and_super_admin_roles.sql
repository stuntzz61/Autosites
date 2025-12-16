-- =====================================================
-- Migration: Add new role hierarchy (Owner, Director, Supervisor, Manager)
-- =====================================================

-- Drop the old CHECK constraint
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;

-- Add new CHECK constraint with new role hierarchy
ALTER TABLE users
ADD CONSTRAINT users_role_check
CHECK (role IN ('guest', 'manager', 'supervisor', 'director', 'owner'));

-- Add comment
COMMENT ON COLUMN users.role IS 'User role hierarchy: owner (владелец), director (директор), supervisor (супервайзер/team lead), manager (менеджер), guest';

