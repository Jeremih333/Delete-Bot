# Как запустить бота и что заполнить

## 1) Что уже создано в репозитории
- `bot/main.py` — основной бот (команды `/start`, `/premium`, `/settings`, `/dev`, `/scan`).
- `bot/worker_scan.py` — фоновый worker для hybrid-mode (GitHub Actions).
- `bot/db.py` — хранение подписок/настроек/ролей/scan_jobs.
- `.github/workflows/bot-hybrid.yml` — расписание worker-задач.
- `.env.example` — образец переменных.

## 2) Что создать в Telegram
1. Через @BotFather создать бота и получить `BOT_TOKEN`.
2. Узнать `BOT_USERNAME` (без `@`).
3. Узнать два Telegram ID разработчиков (для `/dev`) и записать в `DEV_TELEGRAM_ID` и `DEV_TELEGRAM_ID_2` (по одному ID в каждую переменную).

## 3) Что создать в GitHub Secrets (Settings -> Secrets and variables -> Actions)
Обязательные:
- `BOT_TOKEN`
- `BOT_USERNAME`
- `DEV_TELEGRAM_ID` и `DEV_TELEGRAM_ID_2`
- `TARIF_MESSAGE_1`
- `TARIF_MESSAGE_3`
- `TARIF_MESSAGE_6`
- `TARIF_MESSAGE_12`

Рекомендуемые:
- `SUPPORT_URL` (по умолчанию `https://t.me/kiojomi`)
- `HYBRID_QUEUE_THRESHOLD` (по умолчанию `1000`)
- `HYBRID_SCAN_SOFT_TIMEOUT_MS` (по умолчанию `30000`)

## 4) Формат тарифных ссылок
В `TARIF_MESSAGE_*` добавьте готовые Telegram-ссылки, которые открывают сообщение модератору с предзаполненным текстом (например `https://t.me/<username>?text=<encoded_text>`).

## 5) Deploy на Render Free
1. Create Web Service -> подключить этот репозиторий.
2. Build command:
   `pip install -r requirements.txt`
3. Start command:
   `python bot/main.py`
4. В Render Environment Variables добавить все переменные из `.env.example`.
5. В Telegram добавить бота в чат/супергруппу и выдать права администратора (ban/delete).

## 6) Hybrid-mode и worker
- Основной бот отвечает в realtime на Render.
- Фоновый worker запускается в GitHub Actions (`bot-hybrid.yml`) каждые 10 минут.
- Если очередь scan_jobs растёт, бот продолжает складывать задачи в очередь, worker постепенно разгружает.

## 7) Что делать вам после деплоя
1. Проверить `/start` в личке.
2. В чате открыть `/settings`.
3. Проверить `/premium` и что кнопки ведут на ваши `TARIF_MESSAGE_*` ссылки.
4. Проверить `/dev` с вашим ID: выдать premium тестовому пользователю.
5. Запустить `/scan` и проверить, что запись появилась в `scan_jobs`.

## 8) Важно
Текущая версия содержит базовый каркас scan-worker (queue + completion). Реальную массовую проверку участников и удаление нужно подключить отдельным модулем Telegram API/данных участников.

## 9) Как отправить вашу ветку в `main` (default)

### Вариант A (через GitHub интерфейс, рекомендуемый)
1. `git push -u origin <your-branch>`
2. Откройте репозиторий на GitHub -> кнопка **Compare & pull request**.
3. Убедитесь, что:
   - base branch: `main`
   - compare branch: `<your-branch>`
4. Нажмите **Create pull request** -> **Merge pull request** -> **Confirm merge**.
5. Перейдите в **Settings -> Branches** и проверьте, что default branch = `main`.

### Вариант B (через git локально)
```bash
git checkout main
git pull origin main
git merge --no-ff <your-branch>
git push origin main
```

### Если GitHub говорит про conflict
```bash
git checkout <your-branch>
git fetch origin
git rebase origin/main
# решить конфликты, затем
git push --force-with-lease
```
После этого снова откройте PR и выполните merge.

## 10) Если GitHub показывает конфликт именно в `.env.example`, `bot/config.py`, `bot/main.py`
Используйте в этих файлах итоговый вариант из ветки `work`, где уже поддержаны два разработчика:

- `.env.example`: переменная `DEV_TELEGRAM_ID` и `DEV_TELEGRAM_ID_2` (по одному ID в каждую переменную), плюс опциональный legacy-комментарий `DEV_TELEGRAM_IDS`.
- `bot/config.py`: чтение `DEV_TELEGRAM_ID` и `DEV_TELEGRAM_ID_2` с optional fallback на `DEV_TELEGRAM_IDS`.
- `bot/main.py`: проверка доступа к `/dev` через список `cfg.dev_telegram_ids`.

Быстрые команды:
```bash
git checkout work
git fetch origin
git rebase origin/main
# если конфликт:
# 1) открыть конфликтный файл
# 2) оставить корректный финальный блок (как выше)
# 3) затем:
git add .env.example bot/config.py bot/main.py
git rebase --continue
git push --force-with-lease
```

## 11) Пошагово: как снять конфликт в PR (CLI)

Если в PR конфликтуют только эти файлы:
- `.env.example`
- `bot/config.py`
- `bot/main.py`

сделайте так:

```bash
# 1) Перейти в вашу рабочую ветку
git checkout work

# 2) Подтянуть актуальный main
git fetch origin

# 3) Начать rebase (или merge) на свежий main
git rebase origin/main
```

Когда git остановится на конфликте:

```bash
# посмотреть конфликтные файлы
git status

# 4) для .env.example и bot/config.py обычно берём вашу (feature) версию
# (в rebase: "--ours" = ветка main, "--theirs" = ваш коммит)
git checkout --theirs .env.example bot/config.py

# 5) bot/main.py открыть вручную и оставить финальную логику:
# - is_dev(user_id) -> user_id in cfg.dev_telegram_ids
# - проверка /dev только через список dev_telegram_ids

# 6) отметить как решённые
git add .env.example bot/config.py bot/main.py

# 7) продолжить rebase
git rebase --continue

# 8) после завершения отправить ветку
git push --force-with-lease
```

Если используете merge вместо rebase:
```bash
git merge origin/main
# при конфликте:
# "--ours" = ваша текущая ветка (work), "--theirs" = main
git checkout --ours .env.example bot/config.py
# bot/main.py правим вручную

git add .env.example bot/config.py bot/main.py
git commit
git push
```

> Важно про `ours/theirs`: в `rebase` и `merge` их смысл меняется. Если сомневаетесь — откройте файл и проверьте, что осталась логика `DEV_TELEGRAM_ID` + `DEV_TELEGRAM_ID_2` (и optional fallback `DEV_TELEGRAM_IDS`).

## 12) Полная инструкция: подключение бесплатной Cloudflare D1

Начиная с текущей версии, бот поддерживает 2 backend-режима БД:
- `DB_BACKEND=sqlite` (локальный файл)
- `DB_BACKEND=d1` (Cloudflare D1)

### 12.1 Создайте D1 базу в Cloudflare
1. Зайдите в Cloudflare Dashboard.
2. Перейдите: **Workers & Pages -> D1 SQL Database**.
3. Нажмите **Create database**.
4. Скопируйте `Database ID`.

### 12.2 Получите нужные Cloudflare значения
Вам понадобятся 3 значения для Render:
- `CF_ACCOUNT_ID`
  - Dashboard -> URL вида `https://dash.cloudflare.com/<ACCOUNT_ID>/...`
  - или справа в боковой панели аккаунта.
- `D1_DATABASE_ID`
  - из карточки созданной D1 базы.
- `D1_API_TOKEN`
  - Profile -> API Tokens -> Create Token (custom).

Рекомендуемые права токена:
- Account -> D1 -> Edit
- Account -> Workers Scripts -> Read (опционально)
- Scope: только ваш аккаунт.

### 12.3 Настройте Render Environment
В Render Web Service добавьте:

```env
DB_BACKEND=d1
CF_ACCOUNT_ID=<ваш_account_id>
D1_DATABASE_ID=<ваш_d1_database_id>
D1_API_TOKEN=<ваш_api_token>
```

И оставьте остальные переменные бота (`BOT_TOKEN`, `BOT_USERNAME`, тарифы и т.д.).

### 12.4 Что ставить в DB_PATH
Когда `DB_BACKEND=d1`, `DB_PATH` не используется (можно оставить `bot.db` как заглушку).

### 12.5 Инициализация таблиц
При старте:
- бот вызывает `db.init()`;
- schema создаётся автоматически в D1;
- те же таблицы используются и для worker.

### 12.6 GitHub Actions worker + D1
Чтобы worker тоже писал в ту же D1:
1. Откройте GitHub -> Settings -> Secrets and variables -> Actions.
2. Добавьте:
   - `DB_BACKEND` = `d1`
   - `CF_ACCOUNT_ID`
   - `D1_DATABASE_ID`
   - `D1_API_TOKEN`
3. Убедитесь, что workflow пробрасывает эти env (если нет — добавьте в job env).

### 12.7 Быстрый self-check
После деплоя:
1. Выполните `/scan` в чате.
2. Запустите workflow `bot-hybrid-worker` вручную.
3. Проверьте, что job из `pending` становится `done`.

### 12.8 Частые ошибки
- `DB_BACKEND=d1`, но не задан один из `CF_ACCOUNT_ID / D1_DATABASE_ID / D1_API_TOKEN`.
- Неправильные права API Token (нет D1 Edit).
- Опечатка в `DB_BACKEND` (должно быть ровно `d1`).
