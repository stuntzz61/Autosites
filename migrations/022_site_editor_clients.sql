-- Migration: Site Editor Clients
-- Таблица для хранения информации о клиентах, зарегистрированных для редактирования сайтов

-- Таблица клиентов редактора
CREATE TABLE IF NOT EXISTS site_editor_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_id UUID NOT NULL REFERENCES client_sites(id) ON DELETE CASCADE,
    auth_user_id VARCHAR(100),  -- ID пользователя в auth-service
    login VARCHAR(100) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    client_name VARCHAR(255),
    client_contact VARCHAR(255),
    telegram_id VARCHAR(50),
    cms_site_id UUID,  -- ID сайта в cms-service
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT unique_site_editor_client UNIQUE (site_id)
);

-- Добавляем колонку cms_site_id в client_sites если её нет
ALTER TABLE client_sites
ADD COLUMN IF NOT EXISTS cms_site_id UUID;

-- Добавляем колонку archive_s3_key в client_sites если её нет
ALTER TABLE client_sites
ADD COLUMN IF NOT EXISTS archive_s3_key VARCHAR(500);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_site_editor_clients_auth_user ON site_editor_clients(auth_user_id);
CREATE INDEX IF NOT EXISTS idx_site_editor_clients_login ON site_editor_clients(login);
CREATE INDEX IF NOT EXISTS idx_client_sites_cms_site_id ON client_sites(cms_site_id);

