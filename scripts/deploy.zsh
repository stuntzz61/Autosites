#!/bin/zsh

# === НАСТРОЙКИ ===
SERVER="root@194.147.115.160"
SSH_PASS="wakfis-suFzug-jasge1"  # <-- Впишите сюда ваш пароль
# =================

echo "🚀 Подключаюсь к $SERVER..."

# Используем sshpass для автоматического ввода пароля
# Опция -o StrictHostKeyChecking=no предотвращает запрос подтверждения ключа (yes/no) при первом входе
sshpass -p "$SSH_PASS" ssh -t -o StrictHostKeyChecking=no $SERVER << 'ENDSSH'
    # Прерывать при ошибках
    set -e

    echo "📂 Переход в /opt/n8n..."
    cd /opt/n8n

    echo "⬇️ Обновление кода..."
    cd srv/autosites-bot/repo && git pull && cd /opt/n8n

    echo "🐳 Пересборка контейнеров (api, webapp, tg-bot)..."
    docker compose build --no-cache api webapp tg-bot

    echo "▶️ Перезапуск..."
    docker compose up -d api webapp tg-bot

    echo "✅ ВСЕ ГОТОВО! Сборка завершена."
ENDSSH
