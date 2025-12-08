#!/bin/bash
# Скрипт для git add, commit и push
# Использование: ./scripts/git-push.sh [commit message]
#
# Примеры:
#   ./scripts/git-push.sh "Update deploy scripts"
#   ./scripts/git-push.sh "Fix agent deployment"

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Проверка что мы в git репозитории
if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Ошибка: это не git репозиторий${NC}"
    exit 1
fi

# Получаем сообщение коммита
if [ -z "$1" ]; then
    echo -e "${YELLOW}📝 Введите сообщение коммита:${NC}"
    read -r COMMIT_MSG
    if [ -z "$COMMIT_MSG" ]; then
        echo -e "${RED}❌ Сообщение коммита не может быть пустым${NC}"
        exit 1
    fi
else
    COMMIT_MSG="$1"
fi

echo -e "${CYAN}📦 Git Add, Commit & Push${NC}"
echo "=========================================="
echo "   Message: $COMMIT_MSG"
echo "=========================================="
echo ""

# Проверка изменений
if [ -z "$(git status --porcelain)" ]; then
    echo -e "${YELLOW}⚠️  Нет изменений для коммита${NC}"
    exit 0
fi

# Показываем статус
echo -e "${YELLOW}📊 Статус изменений:${NC}"
git status --short
echo ""

# ===========================================
# Шаг 1: Add
# ===========================================
echo -e "${YELLOW}📝 Шаг 1: git add .${NC}"
git add .

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка при git add${NC}"
    exit 1
fi

echo -e "   ${GREEN}✅ Файлы добавлены${NC}"

# ===========================================
# Шаг 2: Commit
# ===========================================
echo ""
echo -e "${YELLOW}💾 Шаг 2: git commit${NC}"
echo "   Message: $COMMIT_MSG"

git commit -m "$COMMIT_MSG"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка при git commit${NC}"
    exit 1
fi

echo -e "   ${GREEN}✅ Коммит создан${NC}"

# ===========================================
# Шаг 3: Push
# ===========================================
echo ""
echo -e "${YELLOW}📤 Шаг 3: git push${NC}"

# Определяем текущую ветку
BRANCH=$(git branch --show-current)

if [ -z "$BRANCH" ]; then
    echo -e "${RED}❌ Не удалось определить текущую ветку${NC}"
    exit 1
fi

echo "   Ветка: $BRANCH"

# Пробуем push
if git push origin "$BRANCH"; then
    echo -e "   ${GREEN}✅ Изменения отправлены${NC}"
else
    echo -e "${YELLOW}⚠️  Push не удался, возможно нужно настроить upstream:${NC}"
    echo "   git push -u origin $BRANCH"
    read -p "Попробовать с -u? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if git push -u origin "$BRANCH"; then
            echo -e "   ${GREEN}✅ Изменения отправлены${NC}"
        else
            echo -e "${RED}❌ Ошибка при push${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}⚠️  Push пропущен${NC}"
        exit 1
    fi
fi

# ===========================================
# Готово
# ===========================================
echo ""
echo "=========================================="
echo -e "${GREEN}✅ Готово!${NC}"
echo ""
echo -e "${CYAN}📊 Информация:${NC}"
echo "   Ветка: $BRANCH"
echo "   Коммит: $(git rev-parse --short HEAD)"
echo "   Сообщение: $COMMIT_MSG"
echo ""

