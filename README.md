# payment-service

Асинхронный микросервис процессинга платежей (тестовое задание).

## 1. Кратко

Клиент создаёт платёж через HTTP API и сразу получает `202 Accepted`. Дальше сервис обрабатывает платёж
асинхронно: эмулирует платёжный шлюз (задержка 2–5 с, 90% успешных) и сообщает результат клиенту
webhook'ом на `webhook_url`. Есть гарантия доставки событий (Transactional Outbox), идемпотентность на
уровне API и consumer'а, повторы с экспоненциальной задержкой и Dead Letter Queue.

**Стек:** Python 3.12, FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), Alembic, PostgreSQL 16,
RabbitMQ 4 + FastStream, httpx, Docker Compose, uv, ruff, mypy (strict), pytest + testcontainers.

## 2. Архитектура

```
 client
   │ POST /api/v1/payments  (X-API-Key, Idempotency-Key)
   ▼
 ┌──────┐   одна транзакция   ┌──────────────────────────────────────┐
 │ api  │ ──────────────────▶ │ PostgreSQL                           │
 └──────┘                     │   payments (status=pending)          │
   ▲ 202                      │   outbox   (published_at IS NULL)    │
   │                          └──────────────────────────────────────┘
                                 ▲  poll, FOR UPDATE SKIP LOCKED  │      ▲ UPDATE status,
                                 │                                ▼      │ webhook_attempts, ...
 ┌─────────────────────────────── consumer ───────────────────────────────────────────┐
 │  outbox relay ──publish (confirm, persistent)──┐                                   │
 │                                                │                                   │
 │  subscriber(payments.new) ─▶ gateway emulation ─▶ DB ─▶ POST webhook_url ──▶ client │
 └────────────────────────────────────────────────┼───────────▲───────┬──────────────┘
                                                  ▼           │       │ ошибка
 RabbitMQ                                                     │       │
   exchange "payments" ──rk payments.new──▶ queue payments.new          │
        ▲                                     │ reject (попытки исчерпаны / PermanentError)
        │                                     ▼
        │                         exchange "payments.dlx" ──▶ queue payments.dlq
        │
        │ DLX после истечения TTL
   queue payments.retry.1000ms (TTL 1 s) ◀─┐
   queue payments.retry.2000ms (TTL 2 s) ◀─┴── exchange "payments.retry" ◀── consumer (x-attempt+1)
```

Сервисы: `postgres`, `rabbitmq`, `api` (FastAPI, к RabbitMQ не обращается, на старте выполняет
`alembic upgrade head`), `consumer` (FastStream: объявляет топологию, запускает outbox relay,
обрабатывает очередь).

### Слои

```
api/, consumer/            транспорт: HTTP и RabbitMQ, сборка зависимостей (composition root)
        │
services/, outbox/relay    бизнес-логика: работает с доменом и портами, ничего не знает об SQL
        │  UnitOfWork, PaymentRepository, OutboxRepository  (domain/repositories.py, Protocol)
        ▼
db/repositories, db/uow    доступ к данным: SQLAlchemy, запросы, маппинг ORM → доменные модели
```

- Доменные модели (`domain/models.py`) это обычные dataclass'ы. ORM-модели не выходят за пределы `db/`.
- `UnitOfWork` задаёт границу транзакции: репозитории работают в его сессии, без `commit()` всё
  откатывается. Поэтому платёж и outbox-сообщение пишутся атомарно, а у consumer'а каждый шаг
  остаётся отдельной короткой транзакцией.
- Сервисы зависят только от протоколов, так что бизнес-правила тестируются на in-memory
  репозиториях без БД (`tests/unit/fakes.py`).

### Ключевые гарантии

- **Outbox, at-least-once.** API в одной транзакции пишет платёж и строку в `outbox`, поэтому после `202`
  событие не теряется. Relay забирает неопубликованные строки (`FOR UPDATE SKIP LOCKED`), публикует их с
  publisher confirms, `mandatory` и persistent-сообщениями и только после этого проставляет
  `published_at`. Если процесс упадёт между publish и commit, сообщение уйдёт повторно; это нормально
  при at-least-once, дубль поглощает consumer.
- **Идемпотентность API.** Заголовок `Idempotency-Key` обязателен. Вместе с платежом хранится sha256
  канонического тела запроса. Повтор с тем же ключом и тем же телом даёт `202` с существующим платежом
  и заголовком `Idempotent-Replayed: true`. Тот же ключ с другим телом даёт
  `422 idempotency_key_reused`. Гонку одинаковых ключей решает `INSERT … ON CONFLICT DO NOTHING`.
- **Идемпотентность consumer'а: повторная доставка не списывает деньги второй раз.** Шлюз вызывается
  только для платежа в статусе `pending`, а результат применяется условным
  `UPDATE … WHERE status='pending'`. Если платёж уже в терминальном статусе и webhook не доставлен,
  отправляется только webhook. Если webhook уже доставлен, сообщение просто подтверждается (ack).
- **Retry: 3 попытки с экспоненциальной задержкой.** При ошибке (webhook не 2xx или таймаут, ошибка БД)
  consumer публикует копию сообщения в `payments.retry` с `x-attempt+1` и подтверждает оригинал.
  Сообщение лежит в delay-очереди без consumer'ов, пока не истечёт TTL (1 с, затем 2 с), после чего
  через DLX возвращается в `payments.new`. Всего получается 3 доставки. Плагины RabbitMQ не нужны,
  слот prefetch не занимается, состояние retry переживает рестарт consumer'а.
- **DLQ.** Когда попытки исчерпаны, а также при `PermanentError` (битое сообщение или неизвестный
  платёж) сообщение отклоняется через `reject(requeue=False)` и через `payments.dlx` попадает в
  `payments.dlq`. Статус платежа сохраняется, `webhook_delivered_at` остаётся `null`.
- **Отказ шлюза не ретраится.** 10% отказов являются бизнес-результатом (`status=failed`), а не
  технической ошибкой. Повтор не изменил бы исход и по сути означал бы повторное списание. Webhook со
  `status=failed` отправляется как обычно.

## 3. Запуск

Нужны Docker с Compose v2, `make` и `curl`.

```bash
cp .env.example .env   # необязательно: в docker-compose.yml уже есть значения по умолчанию
make up                # postgres, rabbitmq, api, consumer (build + ожидание healthcheck'ов)
make up-dev            # то же самое + webhook-sink (нужен для демо ниже и для make e2e)
make logs              # логи api и consumer
make down              # остановить стек и удалить volumes
```

| Сервис | Адрес с хоста | Назначение |
|---|---|---|
| api | http://localhost:8000 (Swagger: `/docs`) | HTTP API |
| rabbitmq | `localhost:5672`, UI http://localhost:15672 (guest/guest) | брокер |
| postgres | `localhost:5432` (payments/payments) | БД |
| webhook-sink (профиль `dev`) | http://localhost:8081 (внутри стека `http://webhook-sink:8080`) | тестовый приёмник webhook'ов |

У webhook-sink три эндпоинта: `POST /hook` всегда отвечает 200, `POST /fail` всегда отвечает 500,
`GET /received` возвращает всё, что пришло.

## 4. Примеры API

Ниже реальные ответы, полученные на поднятом `make up-dev` (заголовки и тела сокращены, значение
`Idempotency-Key` в примерах упрощено). API-ключ по умолчанию
`dev-api-key`.

**Создание платежа**

```bash
curl -i -w '\n' -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-1234' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.50","currency":"RUB","description":"Order #1234",
       "metadata":{"order_id":"1234"},"webhook_url":"http://webhook-sink:8080/hook"}'
```
```
HTTP/1.1 202 Accepted

{"payment_id":"10e8fa4e-2b38-4136-b671-c1faef4f7bab","status":"pending","created_at":"2026-10-08T12:31:59.662740Z"}
```

**Повтор с тем же ключом и тем же телом.** Выполните ту же команду ещё раз — новый платёж не
создаётся, возвращается прежний:

```
HTTP/1.1 202 Accepted
idempotent-replayed: true

{"payment_id":"10e8fa4e-2b38-4136-b671-c1faef4f7bab","status":"pending","created_at":"2026-10-08T12:31:59.662740Z"}
```

**Тот же ключ, другое тело:**

```bash
curl -i -w '\n' -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-1234' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"200.00","currency":"RUB","description":"Order #1234",
       "metadata":{"order_id":"1234"},"webhook_url":"http://webhook-sink:8080/hook"}'
```
```
HTTP/1.1 422 Unprocessable Entity

{"detail":{"code":"idempotency_key_reused","message":"Idempotency-Key was already used with a different request body"}}
```

**Получение платежа** (через ~5 с, когда обработка завершилась). Подставьте `payment_id` из ответа на
создание — id в примерах ниже из другого запуска:

```bash
curl -s localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'; echo
```
```json
{
  "payment_id": "10e8fa4e-2b38-4136-b671-c1faef4f7bab",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Order #1234",
  "metadata": {"order_id": "1234"},
  "status": "succeeded",
  "idempotency_key": "order-1234",
  "webhook_url": "http://webhook-sink:8080/hook",
  "webhook_attempts": 1,
  "webhook_delivered_at": "2026-10-08T12:32:02.211130Z",
  "created_at": "2026-10-08T12:31:59.662740Z",
  "processed_at": "2026-10-08T12:32:02.196637Z"
}
```

**Запрос без ключа или с неверным ключом**

```bash
curl -i -w '\n' localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000
```
```
HTTP/1.1 401 Unauthorized

{"detail":{"code":"unauthorized","message":"Missing or invalid API key"}}
```

**Несуществующий платёж**

```bash
curl -i -w '\n' localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000 -H 'X-API-Key: dev-api-key'
```
```
HTTP/1.1 404 Not Found

{"detail":{"code":"payment_not_found","message":"Payment not found"}}
```

**Полученные webhook'и**

```bash
curl -s -w '\n' localhost:8081/received
```
```json
{"received": [{"event": "payment.processed",
               "payment_id": "10e8fa4e-2b38-4136-b671-c1faef4f7bab",
               "status": "succeeded", "amount": "100.50", "currency": "RUB",
               "processed_at": "2026-10-08T12:32:02.196637Z",
               "metadata": {"order_id": "1234"}}],
 "failed": [...]}
```

Healthcheck работает без авторизации: `curl localhost:8000/health` отвечает `{"status":"ok"}`.

## 5. Демонстрация retry и DLQ

Создаём платёж с webhook'ом, который всегда отвечает 500:

```bash
curl -s -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: dlq-demo-1' -H 'Content-Type: application/json' \
  -d '{"amount":"10.00","currency":"USD","description":"DLQ demo","webhook_url":"http://webhook-sink:8080/fail"}'; echo
# {"payment_id":"22ab955a-f806-4f28-ad86-1f997d077f4e","status":"pending",...}
```

Ключ `dlq-demo-1` фиксированный: для повторной демонстрации без `make down` поменяйте его, иначе
вернётся уже созданный платёж. Ждём ~10 с: шлюз отвечает за 2–5 с, затем идут 3 доставки с паузами 1 с и 2 с. Смотрим платёж:

```bash
curl -s localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'; echo
```
```json
{
  "payment_id": "22ab955a-f806-4f28-ad86-1f997d077f4e",
  "status": "succeeded",
  "webhook_url": "http://webhook-sink:8080/fail",
  "webhook_attempts": 3,
  "webhook_delivered_at": null,
  "processed_at": "2026-10-08T12:32:15.547242Z",
  "...": "..."
}
```

Количество сообщений в DLQ (каждый такой платёж увеличивает его на 1; без `jq` можно так):

```bash
curl -s -u guest:guest localhost:15672/api/queues/%2F/payments.dlq \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["messages"])'
# с jq:  ... | jq .messages
```
```
1
```

В логах consumer'а видна вся цепочка: `docker compose logs consumer | grep <payment_id>`

```
INFO    payment processed payment_id=22ab955a-… status=succeeded
WARNING webhook delivery failed payment_id=22ab955a-…
WARNING processing failed, retry 2/3 in 1000ms payment_id=22ab955a-… error=WebhookDeliveryError('… got HTTP 500')
WARNING webhook delivery failed payment_id=22ab955a-…
WARNING processing failed, retry 3/3 in 2000ms payment_id=22ab955a-… error=WebhookDeliveryError('… got HTTP 500')
WARNING webhook delivery failed payment_id=22ab955a-…
ERROR   attempts exhausted, dead-lettering payment_id=22ab955a-… attempt=3
```

Обратите внимание, что шлюз вызывается один раз: `processed_at` не меняется, и при повторах
отправляется только webhook. Сообщения в DLQ можно посмотреть в UI RabbitMQ:
Queues → `payments.dlq` → Get messages.

## 6. Конфигурация

Все настройки задаются через переменные окружения (`src/payment_service/core/config.py`). Для Docker
Compose часть из них можно переопределить в `.env`.

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | обязательна | `postgresql+asyncpg://user:pass@host:5432/db` |
| `RABBIT_URL` | обязательна | `amqp://user:pass@host:5672/` |
| `API_KEY` | обязательна (в compose `dev-api-key`) | ключ для заголовка `X-API-Key` |
| `RETRY_MAX_ATTEMPTS` | `3` | общее число доставок сообщения до DLQ |
| `RETRY_BASE_DELAY_MS` | `1000` | база экспоненциальной задержки (1 с, 2 с, …) |
| `OUTBOX_POLL_INTERVAL_S` | `0.5` | интервал опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | максимум строк outbox за одну итерацию |
| `GATEWAY_MIN_DELAY_S` | `2` | минимальная задержка эмулятора шлюза |
| `GATEWAY_MAX_DELAY_S` | `5` | максимальная задержка эмулятора шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | вероятность `succeeded` |
| `WEBHOOK_TIMEOUT_S` | `5` | таймаут HTTP-запроса webhook'а |
| `CONSUMER_PREFETCH` | `10` | prefetch для `payments.new` |
| `LOG_LEVEL` | `INFO` | уровень логирования |

## 7. Тесты и проверки

Для локального запуска нужен [uv](https://docs.astral.sh/uv/). Зависимости ставятся командой `make install`.

| Команда | Что делает |
|---|---|
| `make check` | `ruff check` + `ruff format --check` + `mypy --strict` (src, tests, scripts) + все тесты |
| `make test-unit` | unit-тесты, Docker не нужен |
| `make test-int` | интеграционные тесты на настоящих PostgreSQL и RabbitMQ через testcontainers (нужен Docker). Чтобы использовать уже запущенные сервисы, задайте `TEST_DATABASE_URL` / `TEST_RABBIT_URL` |
| `make e2e` | end-to-end smoke по поднятому стеку, сначала нужен `make up-dev` |

Что покрыто:

- **unit:** конфигурация, валидация схем (amount/currency/metadata/url), канонический хэш запроса,
  формат событий, эмулятор шлюза, отправка webhook'ов, генерация RabbitMQ-топологии; бизнес-логика
  сервиса платежей, обработчика и outbox relay на in-memory репозиториях.
- **integration**: миграции и модели; репозитории и unit of work (конфликт ключа, откат без
  commit, условные UPDATE, `SKIP LOCKED`); API целиком
  (auth, 400/401/404/413/415/422, порядок проверок, replay, `/health`); сервис платежей (платёж и outbox в
  одной транзакции, конкурентные запросы с одним ключом); outbox relay (публикация, `SKIP LOCKED`, обработка ошибок); топология
  RabbitMQ; обработка платежа (идемпотентность, отказ шлюза, отсутствие повторного списания);
  consumer на настоящем брокере (retry через TTL/DLX, DLQ после исчерпания попыток, PermanentError
  сразу в DLQ). `TestRabbitBroker` не эмулирует TTL и DLX, поэтому такие тесты используют настоящий
  RabbitMQ.

## 8. Структура проекта

```
src/payment_service/
  core/        настройки (pydantic-settings), логирование
  domain/      доменные модели, enum'ы, события, ошибки, порты репозиториев (без I/O)
  db/          слой доступа к данным: модели SQLAlchemy, сессии, репозитории, unit of work
  services/    бизнес-логика: payments (идемпотентность + outbox), gateway, webhook, processing
  broker/      топология RabbitMQ и фабрика брокера (единственное место с именами очередей)
  outbox/      outbox relay (DB → RabbitMQ)
  api/         FastAPI: app, зависимости, схемы, роуты
  consumer/    FastStream: subscriber, маршрутизация retry/DLQ, запуск relay
  devtools/    webhook_sink (только для dev и e2e)
migrations/    Alembic (async)
tests/unit/, tests/integration/
```

## 9. Принятые решения и допущения

- `GET /health` открыт без API-ключа, иначе не работают healthcheck'и контейнеров. Это единственное
  исключение, все `/api/v1/*` требуют `X-API-Key`.
- «3 попытки» означают 3 доставки сообщения в сумме (первая + 2 повтора через 1 с и 2 с), после чего
  сообщение уходит в DLQ.
- Отказ шлюза (`failed`) является нормальным исходом: webhook отправляется со `status=failed`, retry
  не выполняется.
- Webhook доставляется по схеме at-least-once, получатель должен дедуплицировать по `payment_id`.
- Outbox relay работает фоновой задачей внутри `consumer`, потому что в задании зафиксирован набор из
  4 сервисов. Масштабировать consumer безопасно благодаря `SKIP LOCKED` и идемпотентной обработке.
- Шлюз вызывается с `payment_id` как ключом идемпотентности: повторный вызов для того же платежа
  возвращает прежний результат, а не списывает заново (так работают реальные платёжные шлюзы;
  эмулятор это воспроизводит). Это закрывает окно между списанием и записью статуса в БД.
- Если не удалось опубликовать сообщение в retry-очередь, оно сразу уходит в DLQ, а не возвращается
  в очередь: иначе получился бы бесконечный цикл мгновенных повторов.
- Известное ограничение: перекладывание через DLX в classic-очередях RabbitMQ работает как
  at-most-once при падении самого брокера. Решение: quorum-очереди с
  `x-dead-letter-strategy=at-least-once`, для тестового задания избыточно.
- Защита от SSRF для `webhook_url` и очистка опубликованных строк outbox сознательно не входят в объём
  задания.

### Что можно улучшить

- Метрики (Prometheus), трейсинг (OpenTelemetry), алерт на глубину DLQ.
- Фоновая очистка или архивирование опубликованных строк `outbox`.
- `LISTEN/NOTIFY` вместо поллинга outbox, чтобы снизить задержку и нагрузку на БД.
- API-ключи на клиента, ротация, авторизация `GET` по владельцу платежа.
- SSRF-защита `webhook_url`: allowlist или запрет приватных диапазонов, защита от DNS rebinding.
- HMAC-подпись webhook'ов (`X-Signature`) и инструмент для re-drive сообщений из DLQ.
