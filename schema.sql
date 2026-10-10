-- ====================================================================
-- Safe Buffer PostgreSQL Schema & Functions
-- Архитектура безопасной буферной очереди онлайн-записи
-- ====================================================================

-- 1. Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    tg_user_id BIGINT UNIQUE,
    phone VARCHAR(32) NOT NULL,
    phone_source VARCHAR(32) NOT NULL DEFAULT 'manual' CHECK (phone_source IN ('telegram_contact', 'manual')),
    name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Таблица заявок
CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    idempotency_key UUID NOT NULL UNIQUE,
    client_session_id UUID,
    status VARCHAR(32) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'notified', 'accepted', 'confirmed_in_mis', 'cancelled')),
    branch_id INT NOT NULL,
    preferred_slot VARCHAR(64) NOT NULL,
    notified_at TIMESTAMP WITH TIME ZONE,
    notify_claimed_at TIMESTAMP WITH TIME ZONE,
    next_attempt_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    attempts_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Индекс для Outbox очереди с блокировкой строк
CREATE INDEX IF NOT EXISTS idx_bookings_outbox ON bookings (status, next_attempt_at)
WHERE status = 'pending' AND notified_at IS NULL;

-- 3. Таблица фиксации согласий по 152-ФЗ
CREATE TABLE IF NOT EXISTS consents (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    booking_id INT REFERENCES bookings(id),
    consent_version VARCHAR(32) NOT NULL,
    policy_hash VARCHAR(64) NOT NULL,
    ip_address INET,
    user_agent TEXT,
    agreed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Основная транзакционная функция создания заявки с сериализацией гонок
CREATE OR REPLACE FUNCTION create_booking(
    p_idempotency_key UUID,
    p_tg_user_id BIGINT,
    p_client_session_id UUID,
    p_phone VARCHAR,
    p_name VARCHAR,
    p_branch_id INT,
    p_preferred_slot VARCHAR,
    p_policy_hash VARCHAR,
    p_ip_address INET,
    p_user_agent TEXT,
    OUT out_booking_id INT,
    OUT out_is_duplicate BOOLEAN,
    OUT out_status VARCHAR
)
RETURNS RECORD
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_lock_key BIGINT;
    v_user_id INT;
    v_existing_booking_id INT;
    v_existing_status VARCHAR;
    v_existing_user_id INT;
    v_existing_session_id UUID;
BEGIN
    -- 1. Сериализация гонок через транзакционный Advisory Lock
    v_lock_key := hashtextextended(p_idempotency_key::text, 0);
    PERFORM pg_advisory_xact_lock(v_lock_key);

    -- 2. Проверка существующей заявки с данным ключом идемпотентности
    SELECT id, status, user_id, client_session_id
    INTO v_existing_booking_id, v_existing_status, v_existing_user_id, v_existing_session_id
    FROM bookings
    WHERE idempotency_key = p_idempotency_key;

    IF FOUND THEN
        -- Защита от коллизий ключа между разными пользователями
        IF p_tg_user_id IS NOT NULL THEN
            IF v_existing_user_id != (SELECT id FROM users WHERE tg_user_id = p_tg_user_id) THEN
                RAISE EXCEPTION 'Idempotency key collision between different Telegram users'
                    USING ERRCODE = '23505';
            END IF;
        ELSE
            IF v_existing_session_id IS DISTINCT FROM p_client_session_id THEN
                RAISE EXCEPTION 'Idempotency key collision between different web sessions'
                    USING ERRCODE = '23505';
            END IF;
        END IF;

        -- Возврат существующей заявки (Duplicate Short-Circuit)
        out_booking_id := v_existing_booking_id;
        out_is_duplicate := TRUE;
        out_status := v_existing_status;
        RETURN;
    END IF;

    -- 3. Upsert пользователя (только со статусом 'manual')
    IF p_tg_user_id IS NOT NULL THEN
        INSERT INTO users (tg_user_id, phone, name, phone_source)
        VALUES (p_tg_user_id, p_phone, p_name, 'manual')
        ON CONFLICT (tg_user_id) DO UPDATE
        SET updated_at = NOW(),
            name = COALESCE(EXCLUDED.name, users.name),
            -- Не перезаписывать телефон, подтвержденный нативным событием бота
            phone = CASE WHEN users.phone_source = 'telegram_contact' THEN users.phone ELSE EXCLUDED.phone END
        RETURNING id INTO v_user_id;
    ELSE
        INSERT INTO users (phone, name, phone_source)
        VALUES (p_phone, p_name, 'manual')
        RETURNING id INTO v_user_id;
    END IF;

    -- 4. Создание новой заявки с начальной блокировкой аренды
    INSERT INTO bookings (
        user_id, idempotency_key, client_session_id, status,
        branch_id, preferred_slot, notify_claimed_at
    )
    VALUES (
        v_user_id, p_idempotency_key, p_client_session_id, 'pending',
        p_branch_id, p_preferred_slot, NOW()
    )
    RETURNING id, status INTO out_booking_id, out_status;

    out_is_duplicate := FALSE;

    -- 5. Фиксация согласия на обработку ПДн (152-ФЗ)
    INSERT INTO consents (
        user_id, booking_id, consent_version, policy_hash, ip_address, user_agent
    )
    VALUES (
        v_user_id, out_booking_id, '1.0', p_policy_hash, p_ip_address, p_user_agent
    );

    RETURN;
END;
$$;