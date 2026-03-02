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
3. Узнать два Telegram ID разработчиков (для `/dev`) и записать в `DEV_TELEGRAM_IDS` через запятую (пример: `12345,67890`).

## 3) Что создать в GitHub Secrets (Settings -> Secrets and variables -> Actions)
Обязательные:
- `BOT_TOKEN`
- `BOT_USERNAME`
- `DEV_TELEGRAM_IDS`
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
