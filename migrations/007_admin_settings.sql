-- Admin settings table for admin authentication
CREATE TABLE IF NOT EXISTS admin_settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default admin password (changeme - should be changed immediately)
INSERT INTO admin_settings (key, value) VALUES ('admin_password', 'changeme')
ON CONFLICT (key) DO NOTHING;

-- Index for fast lookup
CREATE INDEX IF NOT EXISTS idx_admin_settings_key ON admin_settings(key);

