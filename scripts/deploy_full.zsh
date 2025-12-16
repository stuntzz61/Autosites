#!/bin/zsh

# ==========================================
# ⚙️  КОНФИГУРАЦИЯ
# ==========================================
SERVER="root@194.147.115.160"
SSH_PASS="wakfis-suFzug-jasge1" 
# ==========================================

# Определяем цвета для локального вывода
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
echo " \033[1;37m   🚀  FULL DEPLOYMENT PIPELINE (GIT + SERVER)        \033[0m"
echo " \033[1;36m=======================================================\033[0m"
echo ""

# ==============================================================================
# ЧАСТЬ 1: ЛОКАЛЬНЫЕ ОПЕРАЦИИ (GIT PUSH)
# ==============================================================================

echo -e "${CYAN}📦 [STEP 1/2] Local Git Operations${NC}"

# Проверка репозитория
if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Ошибка: Текущая папка не является git репозиторием!${NC}"
    exit 1
fi

# Получение сообщения коммита (из аргументов или ввод вручную)
if [ -n "$1" ]; then
    COMMIT_MSG="$1"
else
    # Если изменений нет, пропускаем коммит
    if [ -z "$(git status --porcelain)" ]; then
        echo -e "${YELLOW}⚠️  Нет локальных изменений для коммита.${NC}"
        echo -e "${YELLOW}⏩ Пропускаем шаг Git Push и переходим к деплою...${NC}"
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
    # 1. Add
    printf "   %-30s" "git add ."
    git add .
    echo -e "${GREEN}✅ OK${NC}"

    # 2. Commit
    printf "   %-30s" "git commit"
    git commit -m "$COMMIT_MSG" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ OK${NC}"
    else
        echo -e "${RED}❌ Ошибка (возможно, нет изменений)${NC}"
    fi

    # 3. Push
    BRANCH=$(git branch --show-current)
    printf "   %-30s" "git push origin $BRANCH"
    
    if git push origin "$BRANCH" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ OK${NC}"
    else
        echo -e "${YELLOW}⚠️  Обычный push не прошел. Пробую set-upstream...${NC}"
        if git push -u origin "$BRANCH"; then
             echo -e "${GREEN}✅ OK${NC}"
        else
             echo -e "${RED}❌ Критическая ошибка Git Push. Останавливаем деплой.${NC}"
             exit 1
        fi
    fi
fi

echo ""
echo -e "${GREEN}✅ Локальная часть завершена.${NC}"
echo ""

# ==============================================================================
# ЧАСТЬ 2: УДАЛЕННЫЕ ОПЕРАЦИИ (SSH DEPLOY)
# ==============================================================================

echo -e "${CYAN}🚀 [STEP 2/2] Remote Server Deployment ($SERVER)${NC}"

# Проверка наличия sshpass
if ! command -v sshpass &> /dev/null; then
    echo -e "${RED}❌ Ошибка: 'sshpass' не установлен. Выполните: sudo apt install sshpass${NC}"
    exit 1
fi

# Подключение и выполнение
sshpass -p "$SSH_PASS" ssh -t -o StrictHostKeyChecking=no $SERVER << 'ENDSSH'
    # --- НАСТРОЙКИ ЦВЕТОВ ВНУТРИ СЕССИИ (REMOTE) ---
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
            echo "${RED}❌ Деплой прерван из-за ошибки на сервере.${NC}"
            rm -f "$LOG_FILE"
            exit 1
        fi
    }

    # --- НАЧАЛО РАБОТЫ НА СЕРВЕРЕ ---
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
