# 🐟 FishBot - Telegram Fishing Bot with PostgreSQL

## 🚀 Запуск с PostgreSQL

### Вариант 1: Docker (Рекомендуется)

1. **Установите Docker Desktop**
2. **Запустите PostgreSQL:**
   ```bash
   docker-compose up -d
   ```
3. **Запустите бота:**
   ```bash
   python bot.py
   ```

### Вариант 2: Локальная установка PostgreSQL

1. **Скачайте PostgreSQL:** https://www.postgresql.org/download/windows/
2. **Установите PostgreSQL** с паролем `password`
3. **Создайте базу данных:**
   ```sql
   CREATE DATABASE fishbot;
   ```
4. **Запустите бота:**
   ```bash
   python bot.py
   ```

### Вариант 3: Онлайн PostgreSQL

1. **Зарегистрируйтесь на сервисе:** ElephantSQL, Supabase, Railway
2. **Получите данные подключения**
3. **Обновите .env файл:**
   ```env
   DB_HOST=your-host
   DB_PORT=5432
   DB_NAME=fishbot
   DB_USER=your-username
   DB_PASSWORD=your-password
   ```
4. **Запустите бота:**
   ```bash
   python bot.py
   ```

## 📝 Конфигурация

Файл `.env` содержит настройки подключения к PostgreSQL:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fishbot
DB_USER=postgres
DB_PASSWORD=password
BOT_TOKEN=?
```

## 🔧 Установка зависимостей

```bash
pip install -r requirements.txt
```

## ✅ Проверка перед деплоем

Перед пушем/выкаткой выполните единый smoke-check совместимости БД:

```bash
python scripts/predeploy_db_check.py
```

Что проверяется:
- `SQLite` (локальный тестовый режим) — всегда
- `PostgreSQL` (серверный режим) — если задан `DATABASE_URL`

Пример для сервера:

```bash
set DATABASE_URL=postgresql://user:password@host:5432/dbname
python scripts/predeploy_db_check.py
```

## 🎮 Функцииональность

- 🎣 Рыбалка с сохранением в PostgreSQL
- 🪱 Система наживок с инвентарем
- 📍 Локации с популяцией рыб
- 💰 Экономика монет и звезд
- 📊 Статистика и таблица лидеров
- ⭐ Гарантированный улов
- ⏰ Кулдаун между попытками

## 🐳 Преимущества PostgreSQL

- ✅ Масштабируемость
- ✅ Надежность данных
- ✅ Параллельные запросы
- ✅ Транзакции
- ✅ Индексы для производительности

## 🚨 Возможные проблемы

1. **Connection refused** - PostgreSQL сервер не запущен
2. **Authentication failed** - неверный пароль
3. **Database does not exist** - база данных не создана
4. **ModuleNotFoundError** - psycopg2 не установлен

## 🛠️ Отладка

Проверьте подключение к PostgreSQL:

```python
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="fishbot",
        user="postgres",
        password="password"
    )
    print("✅ Подключение к PostgreSQL успешно!")
    conn.close()
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
```
