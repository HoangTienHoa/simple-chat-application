#!/usr/bin/env bash
# Chạy trên server: cập nhật image của app rồi khởi động lại stack.
# Cách dùng: remote-deploy.sh <image>
set -euo pipefail

IMAGE="$1"
cd "${DEPLOY_DIR:-$HOME/kafka-chat}"

# Ghi APP_IMAGE vào .env, giữ nguyên các biến khác (APP_PORT, UI_PORT...)
touch .env
grep -v '^APP_IMAGE=' .env > .env.tmp || true
echo "APP_IMAGE=$IMAGE" >> .env.tmp
mv .env.tmp .env

docker compose pull app
docker compose up -d --remove-orphans

for _ in $(seq 1 30); do
  if docker compose exec -T app wget -qO- http://localhost:3000/api/users > /dev/null 2>&1; then
    echo "Deploy OK: $IMAGE"
    docker image prune -f > /dev/null
    exit 0
  fi
  sleep 2
done

echo "App không phản hồi sau 60 giây" >&2
docker compose logs --tail 50 app >&2
exit 1
