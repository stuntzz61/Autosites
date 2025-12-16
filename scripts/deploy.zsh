#!/bin/zsh

# Адрес сервера
SERVER="root@194.147.115.160"

echo "🚀 Подключаюсь к $SERVER и начинаю обновление..."

# Подключение по SSH и выполнение команд
ssh -t $SERVER << 'ENDSSH'
    # Прерывать выполнение при ошибках (опционально, но рекомендуется)
    set -e

    echo "📂 Переход в /opt/n8n..."
    cd /opt/n8n

    echo "⬇️ Обновление репозитория..."
    cd srv/autosites-bot/repo && git pull && cd /opt/n8n

    echo "🐳 Пересборка контейнеров (api, webapp, tg-bot)..."
    docker compose build --no-cache api webapp tg-bot

    echo "▶️ Запуск контейнеров..."
    docker compose up -d api webapp tg-bot

    echo "✅ Сборка и запуск успешно завершены!"
ENDSSH
