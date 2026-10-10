-- ====================================================================
-- Safe Buffer PostgreSQL Schema & Functions (v1.0 Production)
-- Архитектура безопасной буферной очереди онлайн-записи
-- Репозиторий: https://github.com/Samvel-cyber1/safe-buffer-architecture
-- ====================================================================

-- 1. Пользовательские перечисления (Enums)
DO $$ BEGIN
    CREATE TYPE booking_status AS ENUM ('pending', 'accepted', 'confirmed_in_mis', 'rescheduled', 'cancelled');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE slot_window AS ENUM ('morning', 'afternoon', 'evening');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- 2. Филиалы клиники
CREATE TABLE IF NOT EXISTS branches (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'Europe/Moscow',
    work_start TIME NOT NULL DEFAULT '08:00',
    work_end TIME NOT NULL DEFAULT '20:00',
    emergency_phone VARCHAR(20) NOT NULL
);

-- Тестовый филиал по умолчанию для бенчмарков
INSERT INTO branches (id, name, emergency_phone)
VALUES ('central', 'Центральный филиал', '+78000000000')
ON CONFLICT (id) DO NOTHING;

-- 3. Пользователи и клиенты
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    tg_user_id BIGINT UNIQUE,
    phone VARCHAR(20) NOT NULL,
    phone_source VARCHAR(20) NOT NULL DEFAULT 'manual' 
        CHECK (phone_source IN ('telegram_contact', 'manual', 'anonymized')),
    first_name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4. Буферная очередь заявок
CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    idempotency_key UUID NOT NULL UNIQUE,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    branch_id VARCHAR(50) REFERENCES branches(id),
    service_group VARCHAR(100) NOT NULL DEFAULT 'consultation'
        CHECK (service_group IN ('therapy', 'surgery', 'orthopedics', 'orthodontics', 'hygiene', 'consultation')),
    preferred_date DATE NOT NULL,
    preferred_window slot_window NOT NULL,
    urgency_flag BOOLEAN DEFAULT FALSE,
    client_session_id VARCHAR(64),
    patient_note TEXT,
    status booking_status DEFAULT 'pending',
    accepted_by BIGINT,
    accepted_at TIMESTAMPTZ,
    notified_at TIMESTAMPTZ,
    notify_claimed_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    retry_count INT DEFAULT 0,
    last_error TEXT,
    alert_sent BOOLEAN DEFAULT FALSE,
    admin_chat_id BIGINT,
    admin_message_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для Outbox очереди с блокировкой строк (SKIP LOCKED)
CREATE INDEX IF NOT EXISTS idx_bookings_outbox ON bookings (status, next_attempt_at)
WHERE status = 'pending' AND notified_at IS NULL;

-- 5. Таблица фиксации согласий по 152-ФЗ
CREATE TABLE IF NOT EXISTS consents (
    id SERIAL PRIMARY KEY,
    booking_id INT REFERENCES bookings(id) ON DELETE RESTRICT,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    policy_version VARCHAR(20) NOT NULL,
    consent_text_hash VARCHAR(64) NOT NULL,
    agreed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    revoked_at TIMESTAMPTZ,
    legal_basis VARCHAR(100) DEFAULT '152-fz-art-9'
);

-- 6. Администраторы дежурного чата
CREATE TABLE IF NOT EXISTS admins (
    tg_user_id BIGINT PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'administrator',
    is_active BOOLEAN DEFAULT TRUE
);

-- 7. Хранимая процедура create_booking с сериализацией гонок (Advisory Locks)
CREATE OR REPLACE FUNCTION create_booking(
    p_idempotency_key UUID,
    p_tg_user_id BIGINT,
    p_client_session_id VARCHAR(64),
    p_phone VARCHAR(20),
    p_first_name VARCHAR(100),
    p_branch_id VARCHAR(50),
    p_service_group VARCHAR(100),
    p_preferred_date DATE,
    p_preferred_window slot_window,
    p_urgency_flag BOOLEAN,
    p_patient_note TEXT,
    p_server_policy_version VARCHAR(20),
    p_server_policy_hash VARCHAR(64),
    p_consent_agreed BOOLEAN,
    OUT o_booking_id INT,
    OUT o_status booking_status,
    OUT o_created_at TIMESTAMPTZ,
    OUT o_is_duplicate BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_user_id INT;
    v_existing_tg_id BIGINT;
    v_existing_session_id VARCHAR(64);
    v_existing_phone VARCHAR(20);
BEGIN
    -- Проверка согласия на уровне БД
    IF p_consent_agreed IS NOT TRUE THEN
        RAISE EXCEPTION 'Consent must be explicitly granted' USING ERRCODE = '22000';
    END IF;

    -- Сериализация по 64-битному хэшу ключа идемпотентности
    PERFORM pg_advisory_xact_lock(hashtextextended(p_idempotency_key::text, 0));

    -- 1. Проверка существования заявки по ключу
    SELECT b.id, b.status, b.created_at, b.client_session_id, u.tg_user_id, u.phone
    INTO o_booking_id, o_status, o_created_at, v_existing_session_id, v_existing_tg_id, v_existing_phone
    FROM bookings b
    LEFT JOIN users u ON u.id = b.user_id
    WHERE b.idempotency_key = p_idempotency_key;

    IF FOUND THEN
        -- Защита от коллизии для Telegram Mini App: сверка по tg_user_id
        IF p_tg_user_id IS NOT NULL AND v_existing_tg_id IS DISTINCT FROM p_tg_user_id THEN
            RAISE EXCEPTION 'Idempotency key collision for different tg_user_id' USING ERRCODE = '23505';
        END IF;

        -- Защита от коллизии для веб-виджета: сверка по связке (телефон + сессия браузера)
        IF p_tg_user_id IS NULL AND (
            v_existing_phone IS DISTINCT FROM p_phone OR 
            (v_existing_session_id IS NOT NULL AND v_existing_session_id IS DISTINCT FROM p_client_session_id)
        ) THEN
            RAISE EXCEPTION 'Idempotency key collision for different web client' USING ERRCODE = '23505';
        END IF;

        o_is_duplicate := TRUE;
        RETURN;
    END IF;

    -- 2. Создание или обновление пользователя (граница доверия phone_source)
    IF p_tg_user_id IS NOT NULL THEN
        INSERT INTO users (tg_user_id, phone, phone_source, first_name)
        VALUES (p_tg_user_id, p_phone, 'manual', p_first_name)
        ON CONFLICT (tg_user_id) DO UPDATE
        SET phone = CASE 
                WHEN users.phone_source = 'telegram_contact' THEN users.phone 
                ELSE EXCLUDED.phone 
            END,
            first_name = COALESCE(EXCLUDED.first_name, users.first_name),
            updated_at = CURRENT_TIMESTAMP
        RETURNING id INTO v_user_id;
    ELSE
        INSERT INTO users (phone, phone_source, first_name)
        VALUES (p_phone, 'manual', p_first_name)
        RETURNING id INTO v_user_id;
    END IF;

    -- 3. Создание заявки с начальной арендой для создающего воркфлоу
    INSERT INTO bookings (
        idempotency_key, user_id, branch_id, service_group,
        preferred_date, preferred_window, urgency_flag, client_session_id,
        patient_note, status, notify_claimed_at, next_attempt_at
    ) VALUES (
        p_idempotency_key, v_user_id, p_branch_id, p_service_group,
        p_preferred_date, p_preferred_window, p_urgency_flag, p_client_session_id,
        p_patient_note, 'pending', NOW(), NOW()
    ) RETURNING id, status, created_at INTO o_booking_id, o_status, o_created_at;

    -- 4. Фиксация согласия в реестре (152-ФЗ)
    INSERT INTO consents (booking_id, user_id, policy_version, consent_text_hash, legal_basis)
    VALUES (o_booking_id, v_user_id, p_server_policy_version, p_server_policy_hash, '152-fz-art-9');

    o_is_duplicate := FALSE;
END;
$$;

-- 8. Доверенная функция подтверждения нативного контакта ботом
CREATE OR REPLACE FUNCTION confirm_telegram_contact(
    p_tg_user_id BIGINT,
    p_phone VARCHAR(20)
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    INSERT INTO users (tg_user_id, phone, phone_source)
    VALUES (p_tg_user_id, p_phone, 'telegram_contact')
    ON CONFLICT (tg_user_id) DO UPDATE
    SET phone = EXCLUDED.phone,
        phone_source = 'telegram_contact',
        updated_at = CURRENT_TIMESTAMP;
END;
$$;

-- 9. Процедура деперсонализации (ст. 21 152-ФЗ)
CREATE OR REPLACE FUNCTION anonymize_user(p_user_id INT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE users
    SET phone = 'ANONYMIZED_' || id,
        first_name = 'DELETED',
        phone_source = 'anonymized',
        tg_user_id = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_user_id;

    UPDATE bookings
    SET patient_note = NULL,
        client_session_id = NULL
    WHERE user_id = p_user_id;
END;
$$;