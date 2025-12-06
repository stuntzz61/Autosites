-- =====================================================
-- Migration: Client Sites - связь заявок с деплоями
-- =====================================================

-- 1. Таблица сайтов клиентов (связь request → deployment)
CREATE TABLE IF NOT EXISTS client_sites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Связь с заявкой
    request_id UUID NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
    manager_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Данные клиента (денормализовано для быстрого доступа)
    company_name VARCHAR(255) NOT NULL,
    client_name VARCHAR(255),
    client_contact VARCHAR(255),

    -- Данные деплоя (из main-deploy-node)
    deploy_id VARCHAR(36),                    -- ID деплоя в deploy-node
    preview_slug VARCHAR(50),                 -- abc12345 для preview URL
    preview_url TEXT,                         -- https://abc12345.autosites.ru

    -- Домен клиента
    domain VARCHAR(255),                      -- mysite.ru
    domain_status VARCHAR(30) DEFAULT 'none', -- none, pending, active, expired
    ssl_enabled BOOLEAN DEFAULT FALSE,

    -- Статусы
    generation_status VARCHAR(30) DEFAULT 'pending',  -- pending, generating, completed, error
    deploy_status VARCHAR(30) DEFAULT 'none',         -- none, pending, deploying, active, failed, stopped

    -- Хостинг
    hosting_plan VARCHAR(30) DEFAULT 'trial',         -- trial, basic, pro, enterprise
    hosting_expires_at TIMESTAMP,                     -- Дата окончания хостинга
    hosting_auto_renew BOOLEAN DEFAULT FALSE,

    -- Хранение архива
    archive_s3_key TEXT,                              -- Ключ архива в S3
    archive_size_bytes BIGINT,

    -- Сервер деплоя
    server_id VARCHAR(36),
    server_name VARCHAR(255),
    server_host VARCHAR(255),
    container_port INTEGER,

    -- Ошибки
    last_error TEXT,
    last_error_at TIMESTAMP,

    -- Метаданные
    notes TEXT,                                        -- Заметки менеджера
    metadata JSONB DEFAULT '{}',                       -- Дополнительные данные

    -- Временные метки
    generated_at TIMESTAMP,                            -- Когда сгенерирован
    deployed_at TIMESTAMP,                             -- Когда задеплоен
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_client_sites_request ON client_sites(request_id);
CREATE INDEX IF NOT EXISTS idx_client_sites_manager ON client_sites(manager_id);
CREATE INDEX IF NOT EXISTS idx_client_sites_deploy_id ON client_sites(deploy_id);
CREATE INDEX IF NOT EXISTS idx_client_sites_domain ON client_sites(domain);
CREATE INDEX IF NOT EXISTS idx_client_sites_preview_slug ON client_sites(preview_slug);
CREATE INDEX IF NOT EXISTS idx_client_sites_deploy_status ON client_sites(deploy_status);
CREATE INDEX IF NOT EXISTS idx_client_sites_hosting_expires ON client_sites(hosting_expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_client_sites_request_unique ON client_sites(request_id);

-- 2. История деплоев (для rollback и аудита)
CREATE TABLE IF NOT EXISTS deploy_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_site_id UUID NOT NULL REFERENCES client_sites(id) ON DELETE CASCADE,
    deploy_id VARCHAR(36) NOT NULL,

    action VARCHAR(30) NOT NULL,               -- deploy, rollback, redeploy, stop
    status VARCHAR(30) NOT NULL,               -- pending, running, success, failed

    archive_s3_key TEXT,
    build_output TEXT,
    error_message TEXT,

    initiated_by UUID REFERENCES users(id),    -- Кто инициировал

    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_deploy_history_site ON deploy_history(client_site_id);
CREATE INDEX IF NOT EXISTS idx_deploy_history_deploy_id ON deploy_history(deploy_id);

-- 3. Планы хостинга
CREATE TABLE IF NOT EXISTS hosting_plans (
    id VARCHAR(30) PRIMARY KEY,                -- trial, basic, pro, enterprise
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price_monthly DECIMAL(10, 2),
    price_yearly DECIMAL(10, 2),

    -- Лимиты
    max_sites INTEGER DEFAULT 1,
    storage_gb INTEGER DEFAULT 1,
    bandwidth_gb INTEGER DEFAULT 10,
    custom_domain BOOLEAN DEFAULT FALSE,
    ssl_included BOOLEAN DEFAULT FALSE,
    priority_support BOOLEAN DEFAULT FALSE,

    trial_days INTEGER DEFAULT 7,
    is_active BOOLEAN DEFAULT TRUE,
    sort_order INTEGER DEFAULT 0
);

-- Предзаполнение планов
INSERT INTO hosting_plans (id, name, description, price_monthly, price_yearly, max_sites, storage_gb, bandwidth_gb, custom_domain, ssl_included, priority_support, trial_days, sort_order)
VALUES
    ('trial', 'Пробный', '7 дней бесплатно для тестирования', 0, 0, 1, 1, 5, FALSE, FALSE, FALSE, 7, 0),
    ('basic', 'Базовый', 'Для небольших сайтов', 500, 5000, 1, 5, 50, TRUE, TRUE, FALSE, 0, 1),
    ('pro', 'Профессиональный', 'Для бизнеса', 1500, 15000, 5, 20, 200, TRUE, TRUE, TRUE, 0, 2),
    ('enterprise', 'Корпоративный', 'Для крупных проектов', 5000, 50000, -1, 100, -1, TRUE, TRUE, TRUE, 0, 3)
ON CONFLICT (id) DO NOTHING;

-- 4. Платежи/транзакции хостинга
CREATE TABLE IF NOT EXISTS hosting_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_site_id UUID NOT NULL REFERENCES client_sites(id) ON DELETE CASCADE,

    type VARCHAR(30) NOT NULL,                 -- payment, refund, extension
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'RUB',

    plan_id VARCHAR(30) REFERENCES hosting_plans(id),
    period_months INTEGER DEFAULT 1,

    status VARCHAR(30) DEFAULT 'pending',      -- pending, completed, failed, refunded
    payment_method VARCHAR(50),
    external_id VARCHAR(255),                  -- ID платежа во внешней системе

    notes TEXT,
    metadata JSONB DEFAULT '{}',

    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hosting_transactions_site ON hosting_transactions(client_site_id);

-- 5. Триггер для updated_at
CREATE TRIGGER set_timestamp_client_sites
    BEFORE UPDATE ON client_sites
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_timestamp();

-- 6. View для активных сайтов с истекающим хостингом
CREATE OR REPLACE VIEW expiring_sites AS
SELECT
    cs.id,
    cs.company_name,
    cs.domain,
    cs.preview_url,
    cs.hosting_plan,
    cs.hosting_expires_at,
    cs.hosting_auto_renew,
    cs.manager_id,
    u.first_name as manager_first_name,
    u.last_name as manager_last_name,
    u.tg_id as manager_tg_id,
    EXTRACT(DAY FROM cs.hosting_expires_at - NOW()) as days_remaining
FROM client_sites cs
JOIN users u ON u.id = cs.manager_id
WHERE cs.deploy_status = 'active'
  AND cs.hosting_expires_at IS NOT NULL
  AND cs.hosting_expires_at <= NOW() + INTERVAL '7 days'
ORDER BY cs.hosting_expires_at ASC;

-- 7. View для статистики сайтов
CREATE OR REPLACE VIEW sites_stats AS
SELECT
    COUNT(*) as total_sites,
    COUNT(*) FILTER (WHERE deploy_status = 'active') as active_sites,
    COUNT(*) FILTER (WHERE deploy_status = 'pending' OR deploy_status = 'deploying') as pending_sites,
    COUNT(*) FILTER (WHERE deploy_status = 'failed') as failed_sites,
    COUNT(*) FILTER (WHERE generation_status = 'generating') as generating_sites,
    COUNT(*) FILTER (WHERE hosting_plan = 'trial') as trial_sites,
    COUNT(*) FILTER (WHERE hosting_plan != 'trial' AND hosting_plan IS NOT NULL) as paid_sites,
    COUNT(*) FILTER (WHERE hosting_expires_at < NOW()) as expired_sites
FROM client_sites;

-- Комментарии
COMMENT ON TABLE client_sites IS 'Сайты клиентов: связь заявок с деплоями и хостингом';
COMMENT ON TABLE deploy_history IS 'История деплоев для аудита и rollback';
COMMENT ON TABLE hosting_plans IS 'Тарифные планы хостинга';
COMMENT ON TABLE hosting_transactions IS 'Платежи за хостинг';
COMMENT ON COLUMN client_sites.deploy_id IS 'ID деплоя в main-deploy-node';
COMMENT ON COLUMN client_sites.preview_slug IS 'Slug для preview URL (abc12345)';

