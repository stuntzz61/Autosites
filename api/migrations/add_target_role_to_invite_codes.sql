ALTER TABLE invite_codes
ADD COLUMN IF NOT EXISTS target_role VARCHAR(20) DEFAULT 'manager';

UPDATE invite_codes
SET target_role = 'manager'
WHERE target_role IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'invite_codes_target_role_check'
    ) THEN
        ALTER TABLE invite_codes
        ADD CONSTRAINT invite_codes_target_role_check
        CHECK (target_role IN ('manager', 'supervisor', 'director'));
    END IF;
END$$;

COMMENT ON COLUMN invite_codes.target_role
IS 'The role to assign to users who register with this invite code';
