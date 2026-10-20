#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
  echo "Ошибка: docker не установлен." >&2
  exit 1
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Создан .env из .env.example — укажите ROUTERAI_API_KEY."
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "Ошибка: не найден docker compose." >&2
  exit 1
fi

$COMPOSE up -d --build

echo ""
echo "Агент запущен: http://localhost:8095"
echo "Логи: $COMPOSE logs -f"
echo "Остановка: $COMPOSE down"
