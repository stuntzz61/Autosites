#!/bin/zsh

# ==========================================
# ⚙️  КОНФИГУРАЦИЯ
# ==========================================
SERVER="root@194.147.115.160"
SSH_PASS="wakfis-suFzug-jasge1" 
# ==========================================

# Определяем цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color

# Очистка экрана
clear

echo ""
echo " \033[1;36m=======================================================\033[0m"
echo " \033[1;37m   🚀  FULL DEPLOYMENT PIPELINE (CLEAN LOGS)          \033[0m"
echo " \033[1;36m=======================================================\033[0m"
echo ""

# ==============================================================================
# ЧАСТЬ 1: ЛОКАЛЬНЫЕ ОПЕРАЦИИ (GIT PUSH)
# ==============================================================================

echo -e "${CYAN}📦 [STEP 1/2] Local Git Operations${NC}"

if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Ошибка: Текущая папка не является git репозиторием!${NC}"
    exit 1
fi

# Получение сообщения коммита
if [ -n "$1" ]; then
    COMMIT_MSG="$1"
else
    if [ -z "$(git status --porcelain)" ]; then
        echo -e "${YELLOW}⚠️  Нет локальных изменений для коммита.${NC}"
        echo -e "${YELLOW}⏩ Пропускаем Git Push...${NC}"
        SKIP_GIT=true
    else
        echo -e "${YELLOW}📝 Введите сообщение коммита:${NC}"
        read -r COMMIT_MSG
        if [ -z "$COMMIT_MSG" ]; then
            echo -e "${RED}❌ Сообщение коммита не может быть пустым${NC}"
            exit 1
        fi
    fi
fi

if [ "$SKIP_GIT" != "true" ]; then
    printf "   %-30s" "git add ."
    git add .
    echo -e "${GREEN}✅ OK${NC}"

    printf "   %-30s" "git commit"
    git commit -m "$COMMIT_MSG" > /dev/null 2>&1
    if [ $? -eq 0 ]; then echo -e "${GREEN}✅ OK${NC}"; else echo -e "${RED}❌ (Нет изменений)${NC}"; fi

    BRANCH=$(git branch --show-current)
    printf "   %-30s" "git push origin $BRANCH"
    if git push origin "$BRANCH" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ OK${NC}"
    else
        echo -e "${YELLOW}⚠️  Пробую push -u...${NC}"
        if git push -u origin "$BRANCH" > /dev/null 2>&1; then echo -e "${GREEN}✅ OK${NC}"; else echo -e "${RED}❌ Ошибка Git Push${NC}"; exit 1; fi
    fi
fi

echo ""
echo -e "${GREEN}✅ Локальная часть завершена.${NC}"
echo ""

# ==============================================================================
# ЧАСТЬ 2: УДАЛЕННЫЕ ОПЕРАЦИИ (SSH DEPLOY)
# ==============================================================================

echo -e "${CYAN}🚀 [STEP 2/2] Remote Server Deployment ($SERVER)${NC}"

# Проверка sshpass
if ! command -v sshpass &> /dev/null; then
    echo -e "${RED}❌ Ошибка: нужен 'sshpass' (sudo apt install sshpass)${NC}"
    exit 1
fi

# ------------------------------------------------------------------
# ХАК ДЛЯ ТИШИНЫ: Создаем .hushlogin на сервере (без -t, чтобы не видеть баннер сейчас)
# ------------------------------------------------------------------
sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no -q $SERVER "touch ~/.hushlogin" 2>/dev/null

# Подключение и выполнение
# Добавлен флаг -q (quiet) для подавления системных сообщений SSH
sshpass -p "$SSH_PASS" ssh -t -q -o StrictHostKeyChecking=no $SERVER << 'ENDSSH'
    
    # Сброс настроек терминала для корректного отображения цветов после подавления баннера
    export TERM=xterm-256color

    # --- НАСТРОЙКИ ЦВЕТОВ ВНУТРИ СЕССИИ ---
    BOLD='\033[1m'
    RED='\033[1;31m'
    GREEN='\033[1;32m'
    BLUE='\033[1;34m'
    NC='\033[0m'
    
    LOG_FILE="/tmp/deploy_task.log"

    # --- ФУНКЦИЯ ВЫПОЛНЕНИЯ ---
    run_step() {
        TITLE=$1
        CMD=$2
        
        START_TIME=$(date +%s)
        printf "   ${BLUE}[PROCESS]${NC} %-45s " "$TITLE..."

        if eval "$CMD" > "$LOG_FILE" 2>&1; then
            END_TIME=$(date +%s)
            DURATION=$((END_TIME - START_TIME))
            printf "\r   ${GREEN}[SUCCESS]${NC} %-45s ${BOLD}(${DURATION}s)${NC}\n" "$TITLE"
            rm -f "$LOG_FILE"
        else
            printf "\r   ${RED}[ERROR]${NC}   %-45s\n" "$TITLE"
            echo ""
            echo "${RED}================= REMOTE LOG START =================${NC}"
            cat "$LOG_FILE"
            echo "${RED}================= REMOTE LOG END ===================${NC}"
            echo ""
            echo "${RED}❌ Деплой прерван.${NC}"
            rm -f "$LOG_FILE"
            exit 1
        fi
    }

    # --- НАЧАЛО РАБОТЫ ---
    cd /opt/n8n || { echo "❌ Папка /opt/n8n не найдена"; exit 1; }

    # 1. PULL
    run_step "Обновление Git (git pull)" "cd srv/autosites-bot/repo && git pull && cd /opt/n8n"

    # 2. BUILD
    run_step "Сборка контейнеров" "docker compose build --no-cache api webapp tg-bot"

    # 3. UP
    run_step "Перезапуск сервисов" "docker compose up -d api webapp tg-bot"

    echo ""
    echo "   ${GREEN}✨ SUCCESS! Сервер успешно обновлен.${NC}"
ENDSSH

echo ""
echo -e "${CYAN}🏁 Пайплайн завершен.${NC}"
