"""
Скрипт воспроизводимого стресс-тестирования идемпотентности функции create_booking.
Проверяет 4 сценария:
1. Конкурентная гонка: 20 параллельных потоков с одним idempotency_key к PostgreSQL 15.x.
2. Повторный вызов: последовательный повтор запроса возвращает o_is_duplicate = True.
3. Коллизия в Telegram: тот же ключ от другого tg_user_id вызывает ошибку 23505.
4. Коллизия в Web: тот же ключ от другой веб-сессии / телефона вызывает ошибку 23505.
"""

import os
import sys
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor

# Параметры подключения к PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "safe_buffer")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

TOTAL_WORKERS = 20
SHARED_IDEMPOTENCY_KEY = str(uuid.uuid4())
SHARED_TG_USER_ID = 123456789
SHARED_SESSION_ID = str(uuid.uuid4())


def get_connection():
    """Создает безопасное соединение с базой данных."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5
    )
    conn.autocommit = True
    return conn


def execute_booking_call(
    worker_id: int,
    idempotency_key: str,
    tg_user_id: int | None,
    client_session_id: str | None,
    phone: str,
    first_name: str
):
    """Выполняет вызов create_booking в отдельном соединении с БД."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT * FROM create_booking(
                    p_idempotency_key := %s::uuid,
                    p_tg_user_id := %s,
                    p_client_session_id := %s,
                    p_phone := %s,
                    p_first_name := %s,
                    p_branch_id := %s,
                    p_service_group := %s,
                    p_preferred_date := CURRENT_DATE + INTERVAL '1 day',
                    p_preferred_window := %s::slot_window,
                    p_urgency_flag := %s,
                    p_patient_note := %s,
                    p_server_policy_version := %s,
                    p_server_policy_hash := %s,
                    p_consent_agreed := %s
                );
            """
            cur.execute(query, (
                idempotency_key,
                tg_user_id,
                client_session_id,
                phone,
                first_name,
                "central",
                "therapy",
                "morning",
                False,
                None,  # Strict Minimal: заметка отсутствует
                "v1.2",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                True
            ))
            row = cur.fetchone()
            return {"worker_id": worker_id, "success": True, "data": row, "error": None}
    except Exception as e:
        return {"worker_id": worker_id, "success": False, "data": None, "error": str(e)}
    finally:
        if conn:
            conn.close()


def run_benchmark():
    print(f"=== ТЕСТ 1: Стресс-тест конкурентности ({TOTAL_WORKERS} параллельных потоков) ===")
    print(f"Ключ идемпотентности: {SHARED_IDEMPOTENCY_KEY}")
    start_time = time.time()

    results = []
    with ThreadPoolExecutor(max_workers=TOTAL_WORKERS) as executor:
        futures = [
            executor.submit(
                execute_booking_call,
                i,
                SHARED_IDEMPOTENCY_KEY,
                SHARED_TG_USER_ID,
                None,
                "+79991112233",
                f"Воркер_{i}"
            )
            for i in range(1, TOTAL_WORKERS + 1)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.time() - start_time
    inserts = sum(1 for r in results if r["success"] and not r["data"]["o_is_duplicate"])
    duplicates = sum(1 for r in results if r["success"] and r["data"]["o_is_duplicate"])
    errors = sum(1 for r in results if not r["success"])

    print(f"Время выполнения: {elapsed:.3f} сек")
    print(f"Первичных созданий (is_duplicate = false): {inserts}")
    print(f"Опознанных дубликатов (is_duplicate = true):  {duplicates}")
    print(f"Ошибок транзакций / гонок unique_violation:     {errors}")

    if inserts != 1 or duplicates != (TOTAL_WORKERS - 1) or errors != 0:
        print("\n[FAIL] ТЕСТ 1 ПРОВАЛЕН: обнаружена гонка данных или сбой блокировки.")
        sys.exit(1)

    booking_id = next(r["data"]["o_booking_id"] for r in results if r["success"])
    print(f"[OK] ТЕСТ 1 ПРОЙДЕН: 1 запись в БД (ID: {booking_id}), 19 запросов вернули дубликат.\n")

    # ТЕСТ 2: Повторный последовательный вызов
    print("=== ТЕСТ 2: Последовательный повтор с тем же ключом ===")
    res_repeat = execute_booking_call(
        99, SHARED_IDEMPOTENCY_KEY, SHARED_TG_USER_ID, None, "+79991112233", "Повтор"
    )
    if res_repeat["success"] and res_repeat["data"]["o_is_duplicate"] and res_repeat["data"]["o_booking_id"] == booking_id:
        print(f"[OK] ТЕСТ 2 ПРОЙДЕН: заявка #{booking_id} распознана как дубликат без ошибок.\n")
    else:
        print(f"[FAIL] ТЕСТ 2 ПРОВАЛЕН: {res_repeat}\n")
        sys.exit(1)

    # ТЕСТ 3: Коллизия ключа от другого Telegram пользователя
    print("=== ТЕСТ 3: Защита от коллизии ключа (другой tg_user_id) ===")
    res_tg_collision = execute_booking_call(
        100, SHARED_IDEMPOTENCY_KEY, 987654321, None, "+79991112233", "Злоумышленник_TG"
    )
    if not res_tg_collision["success"] and "Idempotency key collision for different tg_user_id" in res_tg_collision["error"]:
        print("[OK] ТЕСТ 3 ПРОЙДЕН: коллизия ключа между Telegram ID заблокирована (код 23505).\n")
    else:
        print(f"[FAIL] ТЕСТ 3 ПРОВАЛЕН: ожидалась ошибка коллизии, получено: {res_tg_collision}\n")
        sys.exit(1)

    # ТЕСТ 4: Коллизия ключа в веб-канале (другая сессия / телефон)
    print("=== ТЕСТ 4: Защита от коллизии ключа в Web-канале ===")
    web_key = str(uuid.uuid4())
    # Первичная веб-запись
    res_web1 = execute_booking_call(
        101, web_key, None, SHARED_SESSION_ID, "+79992223344", "Веб_Пользователь_1"
    )
    assert res_web1["success"] and not res_web1["data"]["o_is_duplicate"], "Ошибка первичной веб-записи"

    # Попытка использовать тот же web_key с другим телефоном / сессией
    res_web_collision = execute_booking_call(
        102, web_key, None, "different_session_uuid", "+79993334455", "Веб_Пользователь_2"
    )
    if not res_web_collision["success"] and "Idempotency key collision for different web client" in res_web_collision["error"]:
        print("[OK] ТЕСТ 4 ПРОЙДЕН: коллизия ключа между разными веб-клиентами заблокирована (код 23505).\n")
    else:
        print(f"[FAIL] ТЕСТ 4 ПРОВАЛЕН: ожидалась ошибка коллизии, получено: {res_web_collision}\n")
        sys.exit(1)

    print("ИТОГ: ВСЕ 4 ТЕСТА УСПЕШНО ПРОЙДЕНЫ.")


if __name__ == "__main__":
    run_benchmark()
