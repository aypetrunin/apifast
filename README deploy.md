Ок, делаем **langgraph** и **mcpserver**, релиз **v1.0.1**.
Пойдём строго по чеклисту: **тег → Actions соберёт → pull → обновим prod.env → деплой**.

> Важно: теги создаём **в репозиториях сервисов**, не в root `zena`.

---

# A) Релиз `langgraph` → `v1.0.1`

## A1) Обнови main и посмотри SHA

```bash
cd ~/petrunin/zena/langgraph
git checkout main
git pull
git rev-parse --short HEAD
git log -1 --oneline
```

Запомни SHA (например `13c0fff`).

## A2) Создай и запушь тег `v1.0.1`

```bash
git tag v1.0.1
git push origin v1.0.1
```

➡️ Это должно запустить GitHub Actions в **репозитории langgraph** и опубликовать:

* `ghcr.io/aypetrunin/langgraph:sha-<newsha>`
* `ghcr.io/aypetrunin/langgraph:v1.0.1`

## A3) Проверь, что тег появился (на машине деплоя)

```bash
docker pull ghcr.io/aypetrunin/langgraph:v1.0.1
docker images ghcr.io/aypetrunin/langgraph
```

---

# B) Релиз `mcpserver` → `v1.0.1`

## B1) Обнови main и посмотри SHA

```bash
cd ~/petrunin/zena/mcpserver
git checkout main
git pull
git rev-parse --short HEAD
git log -1 --oneline
```

## B2) Создай и запушь тег `v1.0.1`

```bash
git tag v1.0.1
git push origin v1.0.1
```

➡️ Запустится Actions в **mcpserver** и появится:

* `ghcr.io/aypetrunin/mcpserver:sha-<newsha>`
* `ghcr.io/aypetrunin/mcpserver:v1.0.1`

## B3) Проверь, что тег появился

```bash
docker pull ghcr.io/aypetrunin/mcpserver:v1.0.1
docker images ghcr.io/aypetrunin/mcpserver
```

---

# C) Обновляем root `zena` (prod.env) на новые релизы

Открываем `deploy/prod.env` и меняем **только эти строки**:

```env
LANGGRAPH_IMAGE=ghcr.io/aypetrunin/langgraph:v1.0.1
MCPSERVER_IMAGE=ghcr.io/aypetrunin/mcpserver:v1.0.1
```

Коммитим в root:

```bash
cd ~/petrunin/zena
git add deploy/prod.env
git commit -m "Release langgraph & mcpserver v1.0.1"
git push
```

---

# D) Деплой prod

```bash
cd ~/petrunin/zena
docker compose -p zena-prod -f docker-compose.prod.yml --env-file deploy/prod.env pull
docker compose -p zena-prod -f docker-compose.prod.yml --env-file deploy/prod.env up -d
```

Проверка:

```bash
docker compose -p zena-prod -f docker-compose.prod.yml --env-file deploy/prod.env ps
curl -i http://localhost:8123/ | head -n 20
```

---

## ⚠️ Важная ловушка: “тег v1.0.1 уже существует”

Если ты уже когда-то ставил `v1.0.1`, то git скажет, что тег существует. Тогда нужно:

### удалить локальный и удалённый тег и создать заново (нежелательно для релизов)

```bash
git tag -d v1.0.1
git push origin :refs/tags/v1.0.1
git tag v1.0.1
git push origin v1.0.1
```

✅ Но правильнее в таких случаях выпускать `v1.0.2`.
(Если вдруг упрёмся — скажу, как лучше.)

---

## Давай начнём с langgraph

Сделай в **langgraph** шаги A1–A2 и пришли вывод:

* `git rev-parse --short HEAD`
* результат `git push origin v1.0.1`

И я сразу скажу, что дальше (и если где-то конфликт тега/прав доступа).
