-- Migration: Add email field to users table for manager registration
-- Description: Adds email column to store manager email during registration

ALTER TABLE users
ADD COLUMN IF NOT EXISTS email VARCHAR(255);

-- Create index for email lookup
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email) WHERE email IS NOT NULL;

-- Add comment
COMMENT ON COLUMN users.email IS 'Manager email address for registration';

