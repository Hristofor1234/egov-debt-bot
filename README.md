## Требования к серверу

Для установки и запуска требуется:

- Linux server
- Python 3.11
- Git
- Python venv
- доступ в интернет
- возможность установить зависимости Playwright

---

## Установка

### 1. Клонировать репозиторий

```bash
git clone <URL_РЕПОЗИТОРИЯ>
cd egov-debt-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
playwright install-deps
```


## Настройка окружения

После установки проекта и зависимостей необходимо создать рабочий файл настроек `.env`. Для этого в корне проекта нужно выполнить команду:

```bash
cp .env.example .env

BOT_TOKEN=<REAL_TELEGRAM_BOT_TOKEN>
HEADLESS=true

MIN_DELAY_SECONDS=10
MAX_DELAY_SECONDS=18
BATCH_SIZE=20
BATCH_PAUSE_SECONDS=240
MAX_RETRIES=3
MAX_CONSECUTIVE_ERRORS=4
```
### Запуск бота вручную

source .venv/bin/activate

python bot.py

### Проверка одного человека в чате

Вместо Excel можно отправить боту обычное сообщение с ФИО и одним ИИН:

```text
Иванов Иван Иванович
123456789012
```

Бот проверит запрос через eGov и ответит прямо в чате. Такие запросы, как и
Excel-файлы, обрабатываются по одному в порядке поступления.

### Что нужно для постоянной работы

Нужно запускать его как systemd service на Linux.

## Docker

Создайте `.env` по шаблону `.env.example`, затем запустите:

```bash
docker compose up -d --build
docker compose logs -f bot
```

Данные, Excel-файлы и отладочные снимки сохраняются в `data/` рядом с
контейнером и не попадают в Git. Контейнер обрабатывает файлы строго по одному
в порядке поступления. Его healthcheck открывает только стартовую страницу eGov
и проверяет наличие поля ИИН, не отправляя никаких персональных данных.

### Тесты

```bash
python -m unittest discover -s tests -v
```
