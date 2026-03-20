# -*- coding: utf-8 -*-
"""
Подключение к Supabase (Postgres) и запросы для дашборда.
Нужна переменная окружения SUPABASE_DB_URL (Connection string из Supabase → Project Settings → Database).
Пример: postgresql://postgres.[ref]:[PASSWORD]@aws-0-eu-central-1.pooler.supabase.com:6543/postgres
Можно положить в .env в папке скрипта (при установленном python-dotenv).

ОСНОВНАЯ ТАБЛИЦА (сырьё для ETL, не для чтения дашборда):
  public."For dash" — лиды из AmoCRM. Дашборд к ней не обращается.
  Заливка кэшей: refresh_kpi_daily_region() и INSERT_* в этом файле — они читают «For dash».

ДАШБОРД — ТОЛЬКО КЭШ-ТАБЛИЦЫ:
  • kpi_daily_region — KPI, воронка, динамика по дням, by_region
  • kpi_cache_managers — топ брокеров
  • kpi_cache_stages — этапы сделок
  • kpi_cache_reasons — причины отказа
  • kpi_cache_formnames — формы
  • kpi_cache_utm — UTM
  • kpi_cache_landing — посадочные

  Если kpi_daily_region пуста (кэш не собран): дашборд показывает нули/пустые таблицы и подсказку
  «Обновить кэш» — без тяжёлых запросов к «For dash».

АРХИТЕКТУРА КЭША:
  - kpi_daily_region и остальные кэши обновляются через refresh_kpi_daily_region() (раз в 10–15 мин или по кнопке).
"""
import os
import re
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

import pandas as pd
import psycopg2

# ── Основные константы ────────────────────────────────────────────────────────
TABLE       = '"For dash"'
CACHE_TABLE = "public.kpi_daily_region"
CACHE_MANAGERS = "public.kpi_cache_managers"
CACHE_STAGES   = "public.kpi_cache_stages"
CACHE_REASONS  = "public.kpi_cache_reasons"
CACHE_FORMNAMES = "public.kpi_cache_formnames"
CACHE_UTM = "public.kpi_cache_utm"
CACHE_LANDING = "public.kpi_cache_landing"
# Ключ строки в "For dash": id или lead_id (в Supabase часто lead_id)
PK_COLUMN   = "lead_id"

REGION_EXPR = "COALESCE(NULLIF(TRIM(t.region_kvalifikacii), ''), NULLIF(TRIM(t.direction), ''), NULLIF(TRIM(t.region_klienta), ''))"
REGION_EXPR_NO_ALIAS = "COALESCE(NULLIF(TRIM(region_kvalifikacii), ''), NULLIF(TRIM(direction), ''), NULLIF(TRIM(region_klienta), ''))"

DIRECTION_QUAL_DATE_COLUMN = {
    "Крым":  "qualification_date_krym",
    "Сочи":  "qualification_date_sochi",
    "Анапа": "qualification_date_anapa",
    "Баку":  "qualification_date_baku",
}
ALLOWED_DIRECTIONS = list(DIRECTION_QUAL_DATE_COLUMN.keys())

# Одна дата для CTE кандидатов: один скан таблицы вместо 14 (UNION по всем датам — тяжёлый оверхед).
# Детальные отчёты (UTM, формы, посадки) считаются по лидам, созданным в периоде.
CANDIDATE_DATE_COLUMN = "lead_created_at"

# Глобальный фильтр: только лиды с осмысленным utm_source (везде: кэш и fallback), без прочерков
UTM_FILTER = (
    "utm_source IS NOT NULL AND TRIM(utm_source) != '' "
    "AND utm_source NOT ILIKE 'gck%' "
    "AND utm_source != 'volna' "
    "AND utm_source != 'Неизвестно' "
    "AND utm_source != '-' "
    "AND utm_source != '—'"
)
# Вариант с алиасом t для запросов FROM ... t
UTM_FILTER_T = UTM_FILTER.replace("utm_source", "t.utm_source")

FUNNEL_STAGES = [
    ("lead_created_at",       "Новая заявка"),
    ("pre_qual_date",         "Предквал"),
    ("kval_provedena",        "Квал проведена"),
    ("pokaz_naznachen",       "Показ назначен"),
    ("pokaz_proveden",        "Показ проведён"),
    ("pasport_poluchen",      "Паспорт получен"),
    ("objekt_zabronirovan",   "Объект забронирован"),
]
STAGE_COL_TO_LABEL = dict(FUNNEL_STAGES)

DB_SCHEMA_HINT = """
Таблица: public."For dash" (~419к строк, данные с 2022 года)
Часовой пояс: всегда используй AT TIME ZONE 'Europe/Moscow' для дат.
Ссылка на лид: 'https://estadel.amocrm.ru/leads/detail/' || lead_id::text AS ссылка

РЕГИОНЫ:
- Крым: COALESCE(region_kvalifikacii, direction, region_klienta) = 'Крым'
- Сочи: COALESCE(region_kvalifikacii, direction, region_klienta) = 'Сочи' ИЛИ tags @> '[{"name": "Первичные Сочи"}]'::jsonb
- Анапа: COALESCE(region_kvalifikacii, direction, region_klienta) = 'Анапа'
- Баку: COALESCE(region_kvalifikacii, direction, region_klienta) = 'Баку'

UTM ФИЛЬТР (всегда применяй):
utm_source IS NOT NULL AND TRIM(utm_source) != ''
AND utm_source NOT ILIKE 'gck%'
AND utm_source != 'volna'
AND utm_source != 'Неизвестно'
AND utm_source != '-' AND utm_source != '—'

ВОРОНКА (этапы по датам):
1. Лид: lead_created_at
2. Предквал: pre_qual_date
3. Квал Крым: qualification_date_krym, Сочи: qualification_date_sochi, Анапа: qualification_date_anapa, Баку: qualification_date_baku
4. Показ назначен: pokaz_naznachen
5. Показ проведён: COALESCE(pokaz_proveden, pokaz_proveden_date)
6. Паспорт: pasport_poluchen
7. Бронь: COALESCE(objekt_zabronirovan, data_oplaty_broni_1)
8. Комиссия получена: komissiya_poluchena

ТЕКУЩАЯ СТАДИЯ лида: prev_etap (TEXT) — текущий этап сделки в AmoCRM.
Примеры: 'Новая заявка', 'Взято в работу', 'Менеджер назначен',
'Позвонить сегодня', 'Позвонить через неделю', 'Объект забронирован', 'ПВ внесен'.
Используй: prev_etap AS текущая_стадия

ВАЖНО: g_byudzhet, g_pv — тип TEXT. Для сравнения с числом: NULLIF(g_byudzhet,'')::numeric > 10000
ВАЖНО: pv — тип NUMERIC, сравнивай напрямую: pv > 10000
ВАЖНО: COALESCE(NULLIF(g_pv,''), pv) не работает т.к. g_pv=text, pv=numeric.
Используй раздельно: NULLIF(g_pv,'') AS g_pv, pv AS pv_numeric
ВАЖНО: byudzhet_klienta, byudzhet_pokupki_klienta — тип TEXT, нужен CAST

КОНТАКТЫ КЛИЕНТА:
- telefon_klienta — телефон
- fio_klienta — ФИО
- nomer_wa — WhatsApp

ОТВЕТСТВЕННЫЙ:
- responsible_name — имя менеджера
- responsible_user_id — ID менеджера

КВАЛИФИКАЦИЯ (заполняется квалификатором):
- g_byudzhet, g_pv — бюджет и ПВ (общие), m_byudzhet, m_pv — Москва (все TEXT, см. ВАЖНО выше)
- pv NUMERIC — первоначальный взнос
- byudzhet_klienta, byudzhet_pokupki_klienta, byudzhet_na_pokupku — бюджет клиента
- g_cel, g_srok — цель и срок покупки
- g_santera — Санкт-Петербург/регион
- teplota_klienta — теплота клиента
- tip_investiciy — тип инвестиций

ОБЪЕКТЫ (до 3 объектов на лид):
- kompleks_1_select, kompleks_2_select, kompleks_3_select — комплекс
- stoimost_broni_1/2/3 — стоимость брони
- tochnaya_stoimost_1/2/3 — точная стоимость
- data_oplaty_broni_1/2/3 — дата оплаты брони
- obshaya_summa — общая сумма сделки
- obshaya_summa_komissiy — общая сумма комиссий

ТЕГИ (jsonb массив):
- tags @> '[{"name": "Первичные Сочи"}]'::jsonb — лид относится к Сочи
- Проверка тега: tags @> '[{"name": "ИМЯ_ТЕГА"}]'::jsonb

ПРИЧИНЫ ОТКАЗА:
- prichina_otkaza — основная причина
- kommentariy_k_otkazu — комментарий

ПРИМЕРЫ ЗАПРОСОВ:
-- Предквалы Сочи за февраль 2026 с бюджетом/ПВ:
SELECT 'https://estadel.amocrm.ru/leads/detail/' || lead_id::text AS ссылка,
  prev_etap AS стадия, g_pv AS пв, g_byudzhet AS бюджет,
  telefon_klienta AS телефон
FROM public."For dash"
WHERE (region_kvalifikacii = 'Сочи' OR direction = 'Сочи' OR tags @> '[{"name": "Первичные Сочи"}]'::jsonb)
AND pre_qual_date IS NOT NULL
AND (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN '2026-02-01' AND '2026-02-28'
AND (g_pv IS NOT NULL OR g_byudzhet IS NOT NULL OR pv IS NOT NULL OR byudzhet_klienta IS NOT NULL)
ORDER BY pre_qual_date DESC;

-- Квалы Крым за март 2026 по Яндексу:
SELECT 'https://estadel.amocrm.ru/leads/detail/' || lead_id::text AS ссылка,
  responsible_name AS менеджер, telefon_klienta AS телефон,
  (qualification_date_krym AT TIME ZONE 'Europe/Moscow')::date AS дата_квала
FROM public."For dash"
WHERE COALESCE(region_kvalifikacii, direction, region_klienta) = 'Крым'
AND qualification_date_krym IS NOT NULL
AND (qualification_date_krym AT TIME ZONE 'Europe/Moscow')::date BETWEEN '2026-03-01' AND '2026-03-31'
AND (utm_source ILIKE 'ya%' OR utm_source ILIKE 'yandex%')
ORDER BY qualification_date_krym DESC;
"""


def _empty_kpi_extended_row():
    """Один ряд нулей, если кэш пуст — без чтения «For dash»."""
    return pd.DataFrame(
        [
            {
                "leads": 0,
                "prequals": 0,
                "quals": 0,
                "pokaz_naznachen": 0,
                "pokaz": 0,
                "passports": 0,
                "broni": 0,
                "sdelki": 0,
                "komissi": 0.0,
                "summa": 0.0,
            }
        ]
    )


def _empty_funnel_stages_df():
    return pd.DataFrame([{"stage": label, "cnt": 0} for _, label in FUNNEL_STAGES])


# ── Engine ────────────────────────────────────────────────────────────────────
# Один psycopg2 pool на процесс. На этом окружении transaction pooler :6543
# рвёт SSL, поэтому локально принудительно используем session pooler :5432.
_engine_instance = None
_direct_engine_instance = None


def _normalize_db_url(url: str, prefer_session_pooler: bool = True) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith("postgresql"):
        url = url.replace("postgres://", "postgresql://", 1)
    if prefer_session_pooler and ".pooler.supabase.com:6543/" in url:
        url = url.replace(".pooler.supabase.com:6543/", ".pooler.supabase.com:5432/")
    return url


def _connect_kwargs_from_url(url: str, statement_timeout_ms: int) -> dict:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": parsed.username,
        "password": parsed.password,
        "dbname": parsed.path.lstrip("/"),
        "connect_timeout": 8,
        "sslmode": "require",
        "options": f"-c timezone=Europe/Moscow -c statement_timeout={statement_timeout_ms}",
    }


def _new_engine_config(url: str, statement_timeout_ms: int):
    return {"connect_kwargs": _connect_kwargs_from_url(url, statement_timeout_ms=statement_timeout_ms)}


def _connect_once(engine):
    return psycopg2.connect(**engine["connect_kwargs"])


def _fetch_df(conn, sql: str, params=None) -> pd.DataFrame:
    sql = re.sub(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)", r"%(\1)s", sql)
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        if cur.description is None:
            return pd.DataFrame()
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def get_engine():
    """Конфиг соединения для чтения кэш-таблиц дашборда."""
    global _engine_instance
    if _engine_instance is not None:
        return _engine_instance
    url = _normalize_db_url(os.environ.get("SUPABASE_DB_URL", ""), prefer_session_pooler=True)
    if not url:
        return None
    _engine_instance = _new_engine_config(url, statement_timeout_ms=120000)
    return _engine_instance


def get_direct_engine():
    """Конфиг для тяжёлого refresh. Если direct-хост недоступен — возвращаем None."""
    global _direct_engine_instance
    if _direct_engine_instance is not None:
        return _direct_engine_instance
    url = _normalize_db_url(os.environ.get("SUPABASE_DIRECT_URL", ""), prefer_session_pooler=False)
    if not url:
        return None
    _direct_engine_instance = _new_engine_config(url, statement_timeout_ms=600000)
    return _direct_engine_instance


def run_sql(engine, sql: str, params=None) -> pd.DataFrame:
    """Выполняет SQL через отдельное короткоживущее psycopg2 соединение."""
    last_err = None
    for attempt in range(3):
        conn = None
        try:
            conn = _connect_once(engine)
            df = _fetch_df(conn, sql, params=params)
            conn.rollback()
            conn.close()
            return df
        except Exception as e:
            last_err = e
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
            if attempt < 2 and any(k in str(e).lower() for k in ("timed out", "timeout", "could not receive", "operationalerror", "ssl connection")):
                time.sleep(2 * (attempt + 1))
                continue
            raise
    raise last_err


def _candidates_cte_sql():
    """Кандидаты = лиды у которых хоть одно событие попало в период."""
    cols = [
        "lead_created_at",
        "qualification_date",
        "qualification_date_krym",
        "qualification_date_sochi",
        "qualification_date_anapa",
        "qualification_date_baku",
        "pre_qual_date",
        "pasport_poluchen",
        "pokaz_proveden",
        "pokaz_proveden_date",
        "objekt_zabronirovan",
        "data_oplaty_broni_1",
        "sdelka_sostoyalas",
        "komissiya_poluchena",
    ]
    parts = [
        f"SELECT {PK_COLUMN} FROM public.{TABLE} WHERE (({col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AND ({UTM_FILTER})"
        for col in cols
    ]
    return " UNION ".join(parts)


def _region_filter_sql(region=None, region_list=None, use_table_alias=True):
    expr = REGION_EXPR if use_table_alias else REGION_EXPR_NO_ALIAS
    if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
        parts = [f"({expr} ILIKE :reg{i})" for i in range(len(region_list))]
        return " AND ( " + " OR ".join(parts) + " )", {f"reg{i}": f"%{r}%" for i, r in enumerate(region_list)}
    if region and region != "Все":
        return f" AND ( {expr} ILIKE :region )", {"region": f"%{region}%"}
    return "", {}


def _landing_expr(use_table_alias=True):
    t = "t." if use_table_alias else ""
    return f"""COALESCE(
      NULLIF(TRIM(SPLIT_PART(COALESCE({t}utm_referrer, {t}referrer, ''), '?', 1)), ''),
      '(без посадки)'
    )"""


def _funnel_stage_expr(col: str, region_cond: str = "") -> str:
    if col == "pokaz_proveden":
        base = "((t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)"
    elif col == "objekt_zabronirovan":
        base = "((t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)"
    else:
        base = f"((t.{col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)"
    return base + (f" AND ({region_cond})" if region_cond else "")


def get_responsible_id_to_name_map():
    import re
    sql_file = Path(__file__).resolve().parent / "update_supabase_responsibles.sql"
    if not sql_file.exists():
        return {}
    try:
        text_sql = sql_file.read_text(encoding="utf-8")
    except Exception:
        return {}
    pattern = re.compile(
        r"to_jsonb\('((?:[^']|'')*)'::text\)[^;]+responsible_user_id\s*=\s*(\d+)",
        re.IGNORECASE,
    )
    return {int(m.group(2)): m.group(1).replace("''", "'") for m in pattern.finditer(text_sql)}


# ══════════════════════════════════════════════════════════════════════════════
# КЭШИРУЮЩИЙ СЛОЙ: kpi_daily_region
# ══════════════════════════════════════════════════════════════════════════════

# Полная DDL кэша: таблица + индексы под все запросы дашборда
DDL_CACHE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
  region          TEXT,
  day             DATE,
  leads           INT DEFAULT 0,
  prequals        INT DEFAULT 0,
  quals           INT DEFAULT 0,
  pokaz_naznachen INT DEFAULT 0,
  shows           INT DEFAULT 0,
  passports       INT DEFAULT 0,
  broni           INT DEFAULT 0,
  deals           INT DEFAULT 0,
  commission      NUMERIC(18,2) DEFAULT 0,
  summa           NUMERIC(18,2) DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_daily_region_day ON {CACHE_TABLE} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_daily_region_region_day ON {CACHE_TABLE} (region, day);
CREATE INDEX IF NOT EXISTS idx_kpi_daily_region_day_region ON {CACHE_TABLE} (day, region);
"""

# Логика региона: тег «Первичные Сочи» → Сочи, иначе COALESCE по полям направления
_BASE_REGION = "COALESCE(NULLIF(TRIM(region_kvalifikacii),''), NULLIF(TRIM(direction),''), NULLIF(TRIM(region_klienta),''), '(не указан)')"
REGION_SQL = (
    "CASE WHEN COALESCE(tags, '[]'::jsonb) @> '[{\"name\": \"Первичные Сочи\"}]'::jsonb "
    f"THEN 'Сочи' ELSE {_BASE_REGION} END"
)
# Доп. INSERT в Сочи: тег «Первичные Сочи» и базовый регион ≠ Сочи (избегаем дублей)
_SOCHI_TAG_NOT_SOCHI = (
    f"COALESCE(tags, '[]'::jsonb) @> '[{{\"name\": \"Первичные Сочи\"}}]'::jsonb "
    f"AND {_BASE_REGION} != 'Сочи'"
)
QUAL_DATE_EXPR = "COALESCE(qualification_date_krym, qualification_date_sochi, qualification_date_anapa, qualification_date_baku, qualification_date)"

DDL_CACHE_MANAGERS = f"""
CREATE TABLE IF NOT EXISTS {CACHE_MANAGERS} (
  region       TEXT,
  day          DATE,
  broker_id    BIGINT,
  broker_name  TEXT,
  leads        INT DEFAULT 0,
  prequals     INT DEFAULT 0,
  quals        INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_managers_day ON {CACHE_MANAGERS} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_managers_region_day ON {CACHE_MANAGERS} (region, day);
"""

DDL_CACHE_STAGES = f"""
CREATE TABLE IF NOT EXISTS {CACHE_STAGES} (
  region  TEXT,
  day     DATE,
  stage   TEXT,
  cnt     INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_stages_day ON {CACHE_STAGES} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_stages_region_day ON {CACHE_STAGES} (region, day);
"""

DDL_CACHE_REASONS = f"""
CREATE TABLE IF NOT EXISTS {CACHE_REASONS} (
  region  TEXT,
  day     DATE,
  reason  TEXT,
  cnt     INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_reasons_day ON {CACHE_REASONS} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_reasons_region_day ON {CACHE_REASONS} (region, day);
"""

DDL_CACHE_FORMNAMES = f"""
CREATE TABLE IF NOT EXISTS {CACHE_FORMNAMES} (
  region          TEXT,
  day             DATE,
  formname        TEXT,
  leads           INT DEFAULT 0,
  quals           INT DEFAULT 0,
  prequals        INT DEFAULT 0,
  passports       INT DEFAULT 0,
  pokaz_naznachen INT DEFAULT 0,
  pokaz_proveden  INT DEFAULT 0,
  broni           INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_formnames_region_day ON {CACHE_FORMNAMES} (region, day);
"""

DDL_CACHE_UTM = f"""
CREATE TABLE IF NOT EXISTS {CACHE_UTM} (
  region       TEXT,
  day          DATE,
  event_type   TEXT,
  utm_source   TEXT,
  utm_medium   TEXT,
  utm_campaign TEXT,
  cnt          INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_utm_day ON {CACHE_UTM} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_utm_region_day ON {CACHE_UTM} (region, day);
"""

DDL_CACHE_LANDING = f"""
CREATE TABLE IF NOT EXISTS {CACHE_LANDING} (
  region          TEXT,
  day             DATE,
  landing         TEXT,
  leads           INT DEFAULT 0,
  prequals        INT DEFAULT 0,
  quals           INT DEFAULT 0,
  pokaz_naznachen INT DEFAULT 0,
  pokaz_proveden  INT DEFAULT 0,
  passports       INT DEFAULT 0,
  broni_cnt       INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_landing_day ON {CACHE_LANDING} (day);
CREATE INDEX IF NOT EXISTS idx_kpi_cache_landing_region_day ON {CACHE_LANDING} (region, day);
"""

_QUALS_FILTER_STR = (
    "qualification_date IS NOT NULL OR qualification_date_krym IS NOT NULL "
    "OR qualification_date_sochi IS NOT NULL OR qualification_date_anapa IS NOT NULL OR qualification_date_baku IS NOT NULL"
)

INSERT_CACHE_FORMNAMES = f"""
INSERT INTO {CACHE_FORMNAMES} (region, day, formname, leads, quals, prequals, passports, pokaz_naznachen, pokaz_proveden, broni)
SELECT
  {REGION_SQL} AS region,
  (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day,
  COALESCE(NULLIF(TRIM(formname), ''), '(пусто)') AS formname,
  COUNT(*)::int AS leads,
  COUNT(*) FILTER (WHERE {_QUALS_FILTER_STR})::int AS quals,
  COUNT(*) FILTER (WHERE pre_qual_date IS NOT NULL)::int AS prequals,
  COUNT(*) FILTER (WHERE pasport_poluchen IS NOT NULL)::int AS passports,
  COUNT(*) FILTER (WHERE pokaz_naznachen IS NOT NULL)::int AS pokaz_naznachen,
  COUNT(*) FILTER (WHERE pokaz_proveden IS NOT NULL OR pokaz_proveden_date IS NOT NULL)::int AS pokaz_proveden,
  COUNT(*) FILTER (WHERE objekt_zabronirovan IS NOT NULL OR data_oplaty_broni_1 IS NOT NULL)::int AS broni
FROM public."For dash"
WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER})
GROUP BY 1, 2, 3;
"""

_LANDING_EXPR_SQL = "COALESCE(NULLIF(TRIM(SPLIT_PART(COALESCE(utm_referrer, referrer, ''), '?', 1)), ''), '(без посадки)')"

# Landing: широкая таблица (region, day, landing) с отдельными счётчиками на каждый этап
INSERT_CACHE_LANDING = f"""
INSERT INTO {CACHE_LANDING} (region, day, landing, leads, prequals, quals, pokaz_naznachen, pokaz_proveden, passports, broni_cnt)
SELECT
  {REGION_SQL} AS region,
  (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day,
  {_LANDING_EXPR_SQL} AS landing,
  COUNT(*)::int AS leads,
  COUNT(*) FILTER (WHERE pre_qual_date IS NOT NULL)::int AS prequals,
  COUNT(*) FILTER (WHERE {_QUALS_FILTER_STR})::int AS quals,
  COUNT(*) FILTER (WHERE pokaz_naznachen IS NOT NULL)::int AS pokaz_naznachen,
  COUNT(*) FILTER (WHERE pokaz_proveden IS NOT NULL OR pokaz_proveden_date IS NOT NULL)::int AS pokaz_proveden,
  COUNT(*) FILTER (WHERE pasport_poluchen IS NOT NULL)::int AS passports,
  COUNT(*) FILTER (WHERE objekt_zabronirovan IS NOT NULL OR data_oplaty_broni_1 IS NOT NULL)::int AS broni_cnt
FROM public.{TABLE}
WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER})
GROUP BY 1, 2, 3;
"""
INSERT_CACHE_LANDING_SOCHI = f"""
INSERT INTO {CACHE_LANDING} (region, day, landing, leads, prequals, quals, pokaz_naznachen, pokaz_proveden, passports, broni_cnt)
SELECT
  'Сочи',
  (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day,
  {_LANDING_EXPR_SQL} AS landing,
  COUNT(*)::int AS leads,
  COUNT(*) FILTER (WHERE pre_qual_date IS NOT NULL)::int AS prequals,
  COUNT(*) FILTER (WHERE {_QUALS_FILTER_STR})::int AS quals,
  COUNT(*) FILTER (WHERE pokaz_naznachen IS NOT NULL)::int AS pokaz_naznachen,
  COUNT(*) FILTER (WHERE pokaz_proveden IS NOT NULL OR pokaz_proveden_date IS NOT NULL)::int AS pokaz_proveden,
  COUNT(*) FILTER (WHERE pasport_poluchen IS NOT NULL)::int AS passports,
  COUNT(*) FILTER (WHERE objekt_zabronirovan IS NOT NULL OR data_oplaty_broni_1 IS NOT NULL)::int AS broni_cnt
FROM public.{TABLE}
WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI})
GROUP BY 2, 3;
"""
INSERT_CACHE_LANDING_PARTS = [INSERT_CACHE_LANDING, INSERT_CACHE_LANDING_SOCHI]

# UTM: leads, prequals по lead_/pre_qual_date; quals по региональным датам с константой
INSERT_CACHE_UTM_LEADS = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT {REGION_SQL} AS region, (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day, 'lead',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER}) GROUP BY 1, 2, 4, 5, 6;
"""
INSERT_CACHE_UTM_LEADS_SOCHI = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Сочи', (lead_created_at AT TIME ZONE 'Europe/Moscow')::date, 'lead',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_PREQUALS = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT {REGION_SQL} AS region, (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date AS day, 'prequal',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE pre_qual_date IS NOT NULL AND ({UTM_FILTER}) GROUP BY 1, 2, 4, 5, 6;
"""
INSERT_CACHE_UTM_PREQUALS_SOCHI = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Сочи', (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date, 'prequal',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE pre_qual_date IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_QUALS_KRYM = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Крым', (qualification_date_krym AT TIME ZONE 'Europe/Moscow')::date, 'qual',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE qualification_date_krym IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_QUALS_SOCHI = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Сочи', (qualification_date_sochi AT TIME ZONE 'Europe/Moscow')::date, 'qual',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE qualification_date_sochi IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_QUALS_ANAPA = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Анапа', (qualification_date_anapa AT TIME ZONE 'Europe/Moscow')::date, 'qual',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE qualification_date_anapa IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_QUALS_BAKU = f"""
INSERT INTO {CACHE_UTM} (region, day, event_type, utm_source, utm_medium, utm_campaign, cnt)
SELECT 'Баку', (qualification_date_baku AT TIME ZONE 'Europe/Moscow')::date, 'qual',
  COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)'),
  COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)'),
  COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)'),
  COUNT(*)::int
FROM public.{TABLE} WHERE qualification_date_baku IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2, 3, 4, 5, 6;
"""
INSERT_CACHE_UTM_PARTS = [
    INSERT_CACHE_UTM_LEADS,
    INSERT_CACHE_UTM_LEADS_SOCHI,
    INSERT_CACHE_UTM_PREQUALS,
    INSERT_CACHE_UTM_PREQUALS_SOCHI,
    INSERT_CACHE_UTM_QUALS_KRYM,
    INSERT_CACHE_UTM_QUALS_SOCHI,
    INSERT_CACHE_UTM_QUALS_ANAPA,
    INSERT_CACHE_UTM_QUALS_BAKU,
]

INSERT_CACHE_MANAGERS = f"""
INSERT INTO {CACHE_MANAGERS} (region, day, broker_id, broker_name, leads, prequals, quals)
SELECT
  {REGION_SQL} AS region,
  (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day,
  responsible_user_id AS broker_id,
  COALESCE(NULLIF(TRIM(responsible_name), ''), 'ID ' || responsible_user_id::text) AS broker_name,
  COUNT(*)::int AS leads,
  COUNT(*) FILTER (WHERE pre_qual_date IS NOT NULL)::int AS prequals,
  COUNT(*) FILTER (WHERE {_QUALS_FILTER_STR})::int AS quals
FROM public."For dash"
WHERE lead_created_at IS NOT NULL AND responsible_user_id IS NOT NULL AND ({UTM_FILTER})
GROUP BY 1, 2, 3, 4;
"""

INSERT_CACHE_STAGES = f"""
INSERT INTO {CACHE_STAGES} (region, day, stage, cnt)
SELECT
  {REGION_SQL} AS region,
  (lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS day,
  COALESCE(NULLIF(TRIM(prev_etap), ''), NULLIF(TRIM(status_id::text), ''), '(не указан)') AS stage,
  COUNT(*) AS cnt
FROM public."For dash"
WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER})
GROUP BY 1, 2, 3;
"""

INSERT_CACHE_REASONS = f"""
INSERT INTO {CACHE_REASONS} (region, day, reason, cnt)
SELECT
  {REGION_SQL} AS region,
  (COALESCE(zakryto_ne_realizovano, closed_at) AT TIME ZONE 'Europe/Moscow')::date AS day,
  COALESCE(NULLIF(TRIM(COALESCE(prichina_otkaza, '')), ''), '(не указана)') AS reason,
  COUNT(*) AS cnt
FROM public."For dash"
WHERE COALESCE(zakryto_ne_realizovano, closed_at) IS NOT NULL AND ({UTM_FILTER})
GROUP BY 1, 2, 3;
"""

# KPI по событиям: каждое событие по своей дате. Для non-qual метрик — основной INSERT (с REGION_SQL)
# + доп. INSERT в Сочи для лидов с тегом «Первичные Сочи» (когда базовый регион ≠ Сочи).
# Квалы — по региональным датам, регион константой (без доп. Сочи).
_C = "leads, prequals, quals, pokaz_naznachen, shows, passports, broni, deals, commission, summa"

INSERT_CACHE_SQL_LEADS = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (lead_created_at AT TIME ZONE 'Europe/Moscow')::date, 1,0,0,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_LEADS_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (lead_created_at AT TIME ZONE 'Europe/Moscow')::date, 1,0,0,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE lead_created_at IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
INSERT_CACHE_SQL_PREQUALS = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date, 0,1,0,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE pre_qual_date IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_PREQUALS_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date, 0,1,0,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE pre_qual_date IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
INSERT_CACHE_SQL_QUALS_KRYM = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Крым', (qualification_date_krym AT TIME ZONE 'Europe/Moscow')::date, 0,0,COUNT(*)::int,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE qualification_date_krym IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2;
"""
INSERT_CACHE_SQL_QUALS_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (qualification_date_sochi AT TIME ZONE 'Europe/Moscow')::date, 0,0,COUNT(*)::int,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE qualification_date_sochi IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2;
"""
INSERT_CACHE_SQL_QUALS_ANAPA = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Анапа', (qualification_date_anapa AT TIME ZONE 'Europe/Moscow')::date, 0,0,COUNT(*)::int,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE qualification_date_anapa IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2;
"""
INSERT_CACHE_SQL_QUALS_BAKU = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Баку', (qualification_date_baku AT TIME ZONE 'Europe/Moscow')::date, 0,0,COUNT(*)::int,0,0,0,0,0,0,0
FROM public.{TABLE} WHERE qualification_date_baku IS NOT NULL AND ({UTM_FILTER}) GROUP BY 2;
"""
# pokaz_naznachen — по колонке pokaz_naznachen; shows — только COALESCE(pokaz_proveden, pokaz_proveden_date)
INSERT_CACHE_SQL_POKAZ_NAZNACHEN = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (pokaz_naznachen AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,1,0,0,0,0,0,0
FROM public.{TABLE} WHERE pokaz_naznachen IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_POKAZ_NAZNACHEN_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (pokaz_naznachen AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,1,0,0,0,0,0,0
FROM public.{TABLE} WHERE pokaz_naznachen IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
_SHOW_DAY_EXPR = "COALESCE(pokaz_proveden, pokaz_proveden_date)"
INSERT_CACHE_SQL_SHOWS = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, ({_SHOW_DAY_EXPR} AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,1,0,0,0,0,0
FROM public.{TABLE} WHERE {_SHOW_DAY_EXPR} IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_SHOWS_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', ({_SHOW_DAY_EXPR} AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,1,0,0,0,0,0
FROM public.{TABLE} WHERE {_SHOW_DAY_EXPR} IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
INSERT_CACHE_SQL_PASSPORTS = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,1,0,0,0,0
FROM public.{TABLE} WHERE pasport_poluchen IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_PASSPORTS_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,1,0,0,0,0
FROM public.{TABLE} WHERE pasport_poluchen IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
# Брони + summa: по objekt_zabronirovan/data_oplaty_broni_1; SUM(obshaya_summa)
_BRONI_DAY_EXPR = "COALESCE(objekt_zabronirovan, data_oplaty_broni_1)"
INSERT_CACHE_SQL_BRONI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, ({_BRONI_DAY_EXPR} AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,0,1,0,0,COALESCE(obshaya_summa,0)
FROM public.{TABLE} WHERE {_BRONI_DAY_EXPR} IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_BRONI_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', ({_BRONI_DAY_EXPR} AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,0,1,0,0,COALESCE(obshaya_summa,0)
FROM public.{TABLE} WHERE {_BRONI_DAY_EXPR} IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
INSERT_CACHE_SQL_DEALS = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,0,0,1,0,0
FROM public.{TABLE} WHERE sdelka_sostoyalas IS NOT NULL AND ({UTM_FILTER});
"""
# commission — по komissiya_poluchena; SUM(obshaya_summa_komissiy)
INSERT_CACHE_SQL_COMMISSION = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT {REGION_SQL}, (komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,0,0,0,COALESCE(obshaya_summa_komissiy,0),0
FROM public.{TABLE} WHERE komissiya_poluchena IS NOT NULL AND ({UTM_FILTER});
"""
INSERT_CACHE_SQL_COMMISSION_SOCHI = f"""
INSERT INTO {CACHE_TABLE} (region, day, {_C})
SELECT 'Сочи', (komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date, 0,0,0,0,0,0,0,0,COALESCE(obshaya_summa_komissiy,0),0
FROM public.{TABLE} WHERE komissiya_poluchena IS NOT NULL AND ({UTM_FILTER}) AND ({_SOCHI_TAG_NOT_SOCHI});
"""
INSERT_CACHE_SQL_KPI_PARTS = [
    INSERT_CACHE_SQL_LEADS,
    INSERT_CACHE_SQL_LEADS_SOCHI,
    INSERT_CACHE_SQL_PREQUALS,
    INSERT_CACHE_SQL_PREQUALS_SOCHI,
    INSERT_CACHE_SQL_QUALS_KRYM,
    INSERT_CACHE_SQL_QUALS_SOCHI,
    INSERT_CACHE_SQL_QUALS_ANAPA,
    INSERT_CACHE_SQL_QUALS_BAKU,
    INSERT_CACHE_SQL_POKAZ_NAZNACHEN,
    INSERT_CACHE_SQL_POKAZ_NAZNACHEN_SOCHI,
    INSERT_CACHE_SQL_SHOWS,
    INSERT_CACHE_SQL_SHOWS_SOCHI,
    INSERT_CACHE_SQL_PASSPORTS,
    INSERT_CACHE_SQL_PASSPORTS_SOCHI,
    INSERT_CACHE_SQL_BRONI,
    INSERT_CACHE_SQL_BRONI_SOCHI,
    INSERT_CACHE_SQL_DEALS,
    INSERT_CACHE_SQL_COMMISSION,
    INSERT_CACHE_SQL_COMMISSION_SOCHI,
]


def _run_ddl(conn, ddl_block):
    for stmt in ddl_block.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            with conn.cursor() as cur:
                cur.execute(stmt)


_ALL_CACHE_TABLES = (CACHE_TABLE, CACHE_MANAGERS, CACHE_STAGES, CACHE_REASONS, CACHE_FORMNAMES, CACHE_UTM, CACHE_LANDING)


def ensure_cache_table(engine):
    """Создаёт все таблицы кэша и индексы (только DDL, без ANALYZE — он занимает ~57с)."""
    conn = _connect_once(engine)
    try:
        _run_ddl(conn, DDL_CACHE_TABLE)
        _run_ddl(conn, DDL_CACHE_MANAGERS)
        _run_ddl(conn, DDL_CACHE_STAGES)
        _run_ddl(conn, DDL_CACHE_REASONS)
        _run_ddl(conn, DDL_CACHE_FORMNAMES)
        _run_ddl(conn, DDL_CACHE_UTM)
        _run_ddl(conn, DDL_CACHE_LANDING)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _is_connection_error(e):
    """Обрыв соединения (EOF, timeout) — имеет смысл повторить."""
    msg = str(e).lower()
    return (
        "eof detected" in msg
        or "connection" in msg
        or "ssl syscall" in msg
        or "timed out" in msg
        or "statement_timeout" in msg
    )


def _month_ranges(min_d, max_d):
    """Итератор (d_from, d_to) по месяцам от min_d до max_d включительно."""
    from datetime import date
    from calendar import monthrange
    if min_d is None or max_d is None or min_d > max_d:
        return
    y, m = min_d.year, min_d.month
    end_y, end_m = max_d.year, max_d.month
    while (y, m) <= (end_y, end_m):
        start = date(y, m, 1)
        last = monthrange(y, m)[1]
        stop = date(y, m, last)
        if start < min_d:
            start = min_d
        if stop > max_d:
            stop = max_d
        yield (start, stop)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def refresh_kpi_daily_region(engine):
    """Полная перезаливка всех кэш-таблиц из 'For dash'."""
    max_attempts = 3
    last_error = None
    for attempt in range(max_attempts):
        conn = None
        try:
            conn = _connect_once(engine)
            try:
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = '600000'")
                    for tbl in _ALL_CACHE_TABLES:
                        cur.execute(f"TRUNCATE {tbl}")
                    for sql in INSERT_CACHE_SQL_KPI_PARTS:
                        cur.execute(sql)
                    for sql in [INSERT_CACHE_MANAGERS, INSERT_CACHE_STAGES, INSERT_CACHE_REASONS, INSERT_CACHE_FORMNAMES]:
                        cur.execute(sql)
                    for sql in INSERT_CACHE_UTM_PARTS:
                        cur.execute(sql)
                    for sql in INSERT_CACHE_LANDING_PARTS:
                        cur.execute(sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                last_error = e
                if attempt < max_attempts - 1 and _is_connection_error(e):
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(5 * (attempt + 1))
                    continue
                try:
                    conn.close()
                except Exception:
                    pass
                return (False, str(e))
            conn.close()
            return (True, None)
        except Exception as e:
            last_error = e
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if attempt < max_attempts - 1 and _is_connection_error(e):
                time.sleep(5 * (attempt + 1))
                continue
            return (False, str(e))
    return (False, str(last_error) if last_error else "неизвестная ошибка")


def refresh_kpi_daily_region_chunked(engine):
    """Заполняет кэш по шагам с commit после каждого — меньше шанс таймаута/обрыва."""
    conn = None
    try:
        conn = _connect_once(engine)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300000'")
            for tbl in _ALL_CACHE_TABLES:
                cur.execute(f"TRUNCATE {tbl}")
        conn.commit()
        conn.close()
        for sql in INSERT_CACHE_SQL_KPI_PARTS:
            conn = _connect_once(engine)
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
        for sql in [INSERT_CACHE_MANAGERS, INSERT_CACHE_STAGES, INSERT_CACHE_REASONS, INSERT_CACHE_FORMNAMES]:
            conn = _connect_once(engine)
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
        for sql in INSERT_CACHE_UTM_PARTS:
            conn = _connect_once(engine)
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
        for sql in INSERT_CACHE_LANDING_PARTS:
            conn = _connect_once(engine)
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
        return (True, None)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return (False, str(e))


_CACHE_EMPTY_LOCK = threading.Lock()
_CACHE_EMPTY_RESULT = None
_CACHE_EMPTY_TS = 0.0
_CACHE_EMPTY_TTL = 60


def cache_is_empty(engine) -> bool:
    """Проверяет, пуст ли kpi_daily_region. Результат кэшируется на _CACHE_EMPTY_TTL сек.
    При ошибке COUNT не кэшируем — следующий вызов повторит запрос."""
    global _CACHE_EMPTY_RESULT, _CACHE_EMPTY_TS
    now = time.time()
    with _CACHE_EMPTY_LOCK:
        if _CACHE_EMPTY_TS and (now - _CACHE_EMPTY_TS) < _CACHE_EMPTY_TTL and _CACHE_EMPTY_RESULT is not None:
            return _CACHE_EMPTY_RESULT
    try:
        df = run_sql(engine, f"SELECT COUNT(*) AS cnt FROM {CACHE_TABLE}")
        empty = int(df["cnt"].iloc[0]) == 0
    except Exception:
        return True  # при ошибке считаем пустым, но не кэшируем — следующий вызов повторит
    with _CACHE_EMPTY_LOCK:
        _CACHE_EMPTY_RESULT, _CACHE_EMPTY_TS = empty, time.time()
    return empty


def _engine_for_heavy(engine):
    """Опционально прямое подключение для тяжёлого ETL (refresh). Для дашборда не используется."""
    direct = get_direct_engine()
    if direct is None:
        return engine
    conn = None
    try:
        conn = _connect_once(direct)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.rollback()
        conn.close()
        return direct
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        return engine


# При пустом кэше — читаем из VIEW (v_for_dash_daily, v_for_dash_by_region), созданных в Supabase
def _kpi_from_view_daily(engine, date_from, date_to):
    """Один ряд KPI из v_for_dash_daily (сумма total за период)."""
    try:
        df = run_sql(engine, """
            SELECT COALESCE(SUM(total), 0)::int AS leads
            FROM public.v_for_dash_daily
            WHERE day BETWEEN :d_from AND :d_to
        """, {"d_from": date_from, "d_to": date_to})
        n = int(df["leads"].iloc[0]) if not df.empty else 0
        return pd.DataFrame([{"leads": n, "prequals": 0, "quals": 0, "pokaz": 0, "passports": 0, "broni": 0, "sdelki": 0, "komissi": 0}])
    except Exception:
        return pd.DataFrame([{"leads": 0, "prequals": 0, "quals": 0, "pokaz": 0, "passports": 0, "broni": 0, "sdelki": 0, "komissi": 0}])

def _daily_series_from_view(engine, date_from, date_to):
    """График по дням из v_for_dash_daily (day, total -> date, leads)."""
    try:
        df = run_sql(engine, """
            SELECT day AS date, COALESCE(total, 0)::int AS leads
            FROM public.v_for_dash_daily
            WHERE day BETWEEN :d_from AND :d_to
            ORDER BY day
        """, {"d_from": date_from, "d_to": date_to})
        if df.empty:
            return pd.DataFrame(columns=["date", "leads", "quals", "prequals", "pokaz"])
        df["quals"] = 0
        df["prequals"] = 0
        df["pokaz"] = 0
        return df[["date", "leads", "quals", "prequals", "pokaz"]]
    except Exception:
        return pd.DataFrame(columns=["date", "leads", "quals", "prequals", "pokaz"])

def _by_region_from_view(engine):
    """Разбивка по регионам из v_for_dash_by_region (region, total)."""
    try:
        df = run_sql(engine, "SELECT region, total AS leads FROM public.v_for_dash_by_region ORDER BY leads DESC")
        if df.empty:
            return pd.DataFrame(columns=["region", "leads", "quals", "prequals", "pokaz"])
        df["quals"] = 0
        df["prequals"] = 0
        df["pokaz"] = 0
        return df[["region", "leads", "quals", "prequals", "pokaz"]]
    except Exception:
        return pd.DataFrame(columns=["region", "leads", "quals", "prequals", "pokaz"])


# ══════════════════════════════════════════════════════════════════════════════
# KPI — только из kpi_daily_region. Пустой кэш → нули (без «For dash»).
# ══════════════════════════════════════════════════════════════════════════════

def kpi_extended(engine, date_from, date_to, region=None, region_list=None):
    if cache_is_empty(engine):
        return _empty_kpi_extended_row()
    if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
        placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
        reg_filter = f"AND region IN ({placeholders})"
        reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
    elif region and region != "Все":
        reg_filter = "AND region ILIKE :region"
        reg_params = {"region": f"%{region}%"}
    else:
        reg_filter = ""
        reg_params = {}
    sql = f"""
    SELECT
      SUM(leads)           AS leads,
      SUM(prequals)        AS prequals,
      SUM(quals)           AS quals,
      SUM(pokaz_naznachen) AS pokaz_naznachen,
      SUM(shows)           AS pokaz,
      SUM(passports)       AS passports,
      SUM(broni)           AS broni,
      SUM(deals)           AS sdelki,
      SUM(commission)      AS komissi,
      SUM(summa)           AS summa
    FROM {CACHE_TABLE}
    WHERE day BETWEEN :d_from AND :d_to
      {reg_filter}
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def kpi_by_region(engine, date_from, date_to, region_list):
    if not (isinstance(region_list, (list, tuple)) and len(region_list) > 0):
        return pd.DataFrame()
    if cache_is_empty(engine):
        return pd.DataFrame(
            columns=[
                "region",
                "leads",
                "prequals",
                "quals",
                "pokaz_naznachen",
                "pokaz",
                "passports",
                "broni",
                "sdelki",
                "komissi",
                "summa",
            ]
        )
    placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
    reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
    sql = f"""
    SELECT
      region,
      SUM(leads)           AS leads,
      SUM(prequals)        AS prequals,
      SUM(quals)           AS quals,
      SUM(pokaz_naznachen) AS pokaz_naznachen,
      SUM(shows)           AS pokaz,
      SUM(passports)       AS passports,
      SUM(broni)           AS broni,
      SUM(deals)           AS sdelki,
      SUM(commission)      AS komissi,
      SUM(summa)           AS summa
    FROM {CACHE_TABLE}
    WHERE day BETWEEN :d_from AND :d_to
      AND region IN ({placeholders})
    GROUP BY region
    ORDER BY leads DESC
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def funnel_data(engine, date_from, date_to, region=None, region_list=None):
    return kpi_extended(engine, date_from, date_to, region=region, region_list=region_list)


def funnel_by_region(engine, date_from, date_to, region_list):
    return kpi_by_region(engine, date_from, date_to, region_list)


def daily_series(engine, date_from, date_to, region=None, region_list=None):
    if cache_is_empty(engine):
        return pd.DataFrame(columns=["date", "leads", "quals", "prequals", "pokaz_naznachen", "pokaz"])
    if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
        placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
        reg_filter = f"AND k.region IN ({placeholders})"
        reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
    elif region and region != "Все":
        reg_filter = "AND k.region ILIKE :region"
        reg_params = {"region": f"%{region}%"}
    else:
        reg_filter = ""
        reg_params = {}
    sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    )
    SELECT
      days.d AS date,
      COALESCE(SUM(k.leads),           0) AS leads,
      COALESCE(SUM(k.quals),           0) AS quals,
      COALESCE(SUM(k.prequals),        0) AS prequals,
      COALESCE(SUM(k.pokaz_naznachen), 0) AS pokaz_naznachen,
      COALESCE(SUM(k.shows),           0) AS pokaz
    FROM days
    LEFT JOIN {CACHE_TABLE} k ON k.day = days.d {reg_filter}
    GROUP BY days.d
    ORDER BY days.d
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def daily_series_by_region(engine, date_from, date_to, region_list):
    if not (isinstance(region_list, (list, tuple)) and len(region_list) > 0):
        return pd.DataFrame()
    if cache_is_empty(engine):
        return pd.DataFrame(columns=["date", "region", "leads", "quals", "prequals", "pokaz_naznachen", "pokaz"])
    placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
    reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
    regions_values = ", ".join(f"(:reg{i})" for i in range(len(region_list)))
    sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    ),
    regions AS (
      SELECT * FROM (VALUES {regions_values}) AS v(region)
    )
    SELECT
      days.d AS date,
      regions.region,
      COALESCE(SUM(k.leads),           0) AS leads,
      COALESCE(SUM(k.quals),           0) AS quals,
      COALESCE(SUM(k.prequals),        0) AS prequals,
      COALESCE(SUM(k.pokaz_naznachen), 0) AS pokaz_naznachen,
      COALESCE(SUM(k.shows),           0) AS pokaz
    FROM days
    CROSS JOIN regions
    LEFT JOIN {CACHE_TABLE} k ON k.day = days.d AND k.region = regions.region
    GROUP BY days.d, regions.region
    ORDER BY days.d, regions.region
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def by_region(engine, date_from, date_to):
    if cache_is_empty(engine):
        return pd.DataFrame(columns=["region", "leads", "quals", "prequals", "pokaz_naznachen", "pokaz"])
    sql = f"""
    SELECT
      COALESCE(region, '(не указан)') AS region,
      SUM(leads)           AS leads,
      SUM(quals)           AS quals,
      SUM(prequals)        AS prequals,
      SUM(pokaz_naznachen) AS pokaz_naznachen,
      SUM(shows)           AS pokaz
    FROM {CACHE_TABLE}
    WHERE day BETWEEN :d_from AND :d_to
    GROUP BY region
    ORDER BY leads DESC
    """
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to})


# ══════════════════════════════════════════════════════════════════════════════
# ДЕТАЛЬНЫЕ ЗАПРОСЫ — только кэш-таблицы (UTM, landing, formnames, managers, stages, reasons).
# Пустой kpi_daily_region → см. _empty_* / пустые DataFrame; «For dash» дашборд не трогает.
# ══════════════════════════════════════════════════════════════════════════════

def by_utm(engine, date_from, date_to, region=None, region_list=None, limit=20):
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND u.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND u.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT u.utm_source,
               COALESCE(SUM(u.cnt) FILTER (WHERE u.event_type = 'lead'),    0)::int AS leads,
               COALESCE(SUM(u.cnt) FILTER (WHERE u.event_type = 'prequal'), 0)::int AS prequals,
               COALESCE(SUM(u.cnt) FILTER (WHERE u.event_type = 'qual'),    0)::int AS quals
        FROM {CACHE_UTM} u
        WHERE u.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY u.utm_source
        ORDER BY leads DESC
        LIMIT :lim
        """
        params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
        return run_sql(engine, sql, params)
    return pd.DataFrame(columns=["utm_source", "leads", "prequals", "quals"])


def by_formname(engine, date_from, date_to, region=None, region_list=None, limit=25):
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND f.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND f.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT f.formname,
          SUM(f.leads)::int           AS leads,
          SUM(f.quals)::int           AS quals,
          SUM(f.prequals)::int        AS prequals,
          SUM(f.passports)::int       AS passports,
          SUM(f.pokaz_naznachen)::int AS pokaz_naznachen,
          SUM(f.pokaz_proveden)::int  AS pokaz_proveden,
          SUM(f.broni)::int           AS broni
        FROM {CACHE_FORMNAMES} f
        WHERE f.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY f.formname
        ORDER BY leads DESC
        LIMIT :lim
        """
        return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params})
    return pd.DataFrame(
        columns=[
            "formname",
            "leads",
            "quals",
            "prequals",
            "passports",
            "pokaz_naznachen",
            "pokaz_proveden",
            "broni",
        ]
    )


def by_landing(engine, date_from, date_to, region=None, region_list=None, limit=30):
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND l.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND l.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT l.landing,
               SUM(l.leads)::int           AS leads,
               SUM(l.prequals)::int        AS prequals,
               SUM(l.quals)::int           AS quals,
               SUM(l.pokaz_naznachen)::int AS pokaz_naznachen,
               SUM(l.pokaz_proveden)::int  AS pokaz_proveden,
               SUM(l.passports)::int       AS passports,
               SUM(l.broni_cnt)::int       AS broni
        FROM {CACHE_LANDING} l
        WHERE l.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY l.landing
        ORDER BY leads DESC
        LIMIT :lim
        """
        return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params})
    return pd.DataFrame(
        columns=[
            "landing",
            "leads",
            "prequals",
            "quals",
            "pokaz_naznachen",
            "pokaz_proveden",
            "passports",
            "broni",
        ]
    )


def by_source_key(engine, date_from, date_to, region=None, limit=20):
    """В дашборде не используется; отдельной кэш-таблицы нет — без «For dash» возвращаем пусто."""
    return pd.DataFrame(columns=["source_key", "leads", "quals"])


def top_managers(engine, date_from, date_to, region=None, region_list=None, limit=10):
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND m.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND m.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT
          m.broker_id,
          MAX(m.broker_name) AS broker_name,
          SUM(m.leads)::int    AS leads,
          SUM(m.prequals)::int AS prequals,
          SUM(m.quals)::int    AS quals,
          ROUND(100.0 * SUM(m.quals) / NULLIF(SUM(m.leads), 0), 1) AS conv_percent
        FROM {CACHE_MANAGERS} m
        WHERE m.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY m.broker_id
        ORDER BY SUM(m.quals) DESC
        LIMIT :lim
        """
        return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params})
    return pd.DataFrame(columns=["broker_id", "broker_name", "leads", "prequals", "quals", "conv_percent"])


def deal_stages_funnel(engine, date_from, date_to, region=None, region_list=None):
    """Воронка по этапам. Читает из kpi_daily_region (метрики через SUM колонок).
    Этапы menedzher_naznachen и vzyato_v_rabotu не хранятся в кэше — всегда 0."""
    if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
        placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
        reg_filter = f"AND region IN ({placeholders})"
        reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
    elif region and region != "Все":
        reg_filter = "AND region ILIKE :region"
        reg_params = {"region": f"%{region}%"}
    else:
        reg_filter = ""
        reg_params = {}

    if not cache_is_empty(engine):
        sql = f"""
        SELECT
          SUM(leads)           AS lead_created_at,
          SUM(prequals)        AS pre_qual_date,
          SUM(quals)           AS kval_provedena,
          SUM(pokaz_naznachen) AS pokaz_naznachen,
          SUM(shows)           AS pokaz_proveden,
          SUM(passports)       AS pasport_poluchen,
          SUM(broni)           AS objekt_zabronirovan,
          SUM(commission)      AS komissiya_poluchena
        FROM {CACHE_TABLE}
        WHERE day BETWEEN :d_from AND :d_to {reg_filter}
        """
        df_row = run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, **reg_params})
        if df_row.empty:
            return _empty_funnel_stages_df()
        row = df_row.iloc[0]
        result = []
        for col, label in FUNNEL_STAGES:
            result.append({"stage": label, "cnt": int(row.get(col) or 0)})
        return pd.DataFrame(result)

    return _empty_funnel_stages_df()


def deal_stages(engine, date_from, date_to, region=None, region_list=None, limit=12):
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND s.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND s.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT s.stage, SUM(s.cnt)::int AS cnt
        FROM {CACHE_STAGES} s
        WHERE s.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY s.stage
        ORDER BY cnt DESC
        LIMIT :lim
        """
        return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params})
    return pd.DataFrame(columns=["stage", "cnt"])


def rejection_reasons(engine, date_from, date_to, region=None, region_list=None, limit=15):
    """Причины отказа за период — только kpi_cache_reasons."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    if not cache_is_empty(engine):
        if isinstance(region_list, (list, tuple)) and len(region_list) > 0:
            placeholders = ", ".join(f":reg{i}" for i in range(len(region_list)))
            reg_filter = f"AND r.region IN ({placeholders})"
            reg_params = {f"reg{i}": r for i, r in enumerate(region_list)}
        elif region and region != "Все":
            reg_filter = "AND r.region ILIKE :region"
            reg_params = {"region": f"%{region}%"}
        else:
            reg_filter = ""
            reg_params = {}
        sql = f"""
        SELECT r.reason, SUM(r.cnt)::int AS cnt
        FROM {CACHE_REASONS} r
        WHERE r.day BETWEEN :d_from AND :d_to {reg_filter}
        GROUP BY r.reason
        ORDER BY cnt DESC
        LIMIT :lim
        """
        return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params})
    return pd.DataFrame(columns=["reason", "cnt"])


def date_bounds(engine):
    """Границы дат только из kpi_daily_region; пустой кэш — сегодня..сегодня (без «For dash»)."""
    if not cache_is_empty(engine):
        try:
            return run_sql(engine, f"SELECT MIN(day) AS min_d, MAX(day) AS max_d FROM {CACHE_TABLE}")
        except Exception:
            pass
    from datetime import date

    today = date.today()
    return pd.DataFrame({"min_d": [today], "max_d": [today]})


def region_list(engine):
    """Список регионов из kpi_daily_region; пустой кэш — фиксированный список для UI (без «For dash»)."""
    if not cache_is_empty(engine):
        try:
            df = run_sql(engine, f"SELECT DISTINCT region FROM {CACHE_TABLE} WHERE region IS NOT NULL AND TRIM(region) <> '' ORDER BY 1")
            return ["Все"] + df["region"].dropna().astype(str).tolist()
        except Exception:
            pass
    return ["Все", "Крым", "Сочи", "Анапа", "Баку"]


def load_all_parallel(engine, date_from, date_to, selected_regions):
    """Только основные данные (kpi, by_region, funnel, daily). UTM, landing и прочие детали грузятся отдельно (progressive loading)."""
    tasks = {
        "kpi":       lambda: kpi_extended(engine, date_from, date_to, region_list=selected_regions),
        "by_region": lambda: kpi_by_region(engine, date_from, date_to, selected_regions),
        "funnel":    lambda: funnel_by_region(engine, date_from, date_to, selected_regions),
        "daily":     lambda: daily_series_by_region(engine, date_from, date_to, selected_regions),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as e:
                results[name] = pd.DataFrame()
                print(f"[load_all_parallel] {name} ERROR: {e}")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# RAW / CTE по «For dash» — только для ETL (INSERT_CACHE_* / refresh_kpi_daily_region), не для UI.
# ══════════════════════════════════════════════════════════════════════════════

def _kpi_extended_raw(engine, date_from, date_to, region=None, region_list=None):
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else None
    quals_expr = f"(t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to" if qual_col else "(t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to"
    cte = _candidates_cte_sql()
    sql = f"""
    WITH candidates AS ({cte})
    SELECT
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE {quals_expr}) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.pokaz_naznachen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz_naznachen,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki,
      COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0) AS komissi,
      COALESCE(SUM(t.obshaya_summa) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0) AS summa
    FROM public.{TABLE} t
    WHERE t.{PK_COLUMN} IN (SELECT {PK_COLUMN} FROM candidates) {reg_filter}
    """
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, **reg_params})


def _kpi_by_region_raw(engine, date_from, date_to, region_list):
    reg_filter, reg_params = _region_filter_sql(region_list=region_list)
    cte = _candidates_cte_sql()
    use_dir = all(r in DIRECTION_QUAL_DATE_COLUMN for r in region_list)
    if use_dir:
        parts = []
        for i, direction in enumerate(region_list):
            qual_col = DIRECTION_QUAL_DATE_COLUMN[direction]
            parts.append(f"""
    SELECT :dir{i} AS region,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS leads,
      COUNT(*) FILTER (WHERE (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS prequals,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS sdelki,
      COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})), 0) AS komissi
    FROM public.{TABLE} t WHERE t.{PK_COLUMN} IN (SELECT {PK_COLUMN} FROM candidates)""")
            reg_params[f"dir{i}"] = direction
            reg_params[f"regdir{i}"] = f"%{direction}%"
        sql = "WITH candidates AS (" + cte + ") " + " UNION ALL ".join(p.strip() for p in parts) + " ORDER BY leads DESC"
    else:
        sql = f"""
        WITH candidates AS ({cte})
        SELECT {REGION_EXPR} AS region,
          COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
          COUNT(*) FILTER (WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
          COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
          COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki,
          COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0) AS komissi
        FROM public.{TABLE} t WHERE t.{PK_COLUMN} IN (SELECT {PK_COLUMN} FROM candidates) {reg_filter}
        GROUP BY {REGION_EXPR} ORDER BY leads DESC"""
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, **reg_params})


def _by_region_raw(engine, date_from, date_to):
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(region_kvalifikacii),''), NULLIF(TRIM(direction),''), NULLIF(TRIM(region_klienta),''), '(не указан)') AS region,
      COUNT(*) FILTER (WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals
    FROM public.{TABLE}
    WHERE ((lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
       OR (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)
      AND ({UTM_FILTER})
    GROUP BY 1 ORDER BY leads DESC
    """
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to})


def _daily_series_raw(engine, date_from, date_to, region=None, region_list=None):
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else "qualification_date"
    sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    ),
    lead_days AS (SELECT (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS leads FROM public.{TABLE} t WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({UTM_FILTER_T}) {reg_filter} GROUP BY 1),
    qual_days AS (SELECT (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS quals FROM public.{TABLE} t WHERE (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({UTM_FILTER_T}) {reg_filter} GROUP BY 1),
    prequal_days AS (SELECT (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS prequals FROM public.{TABLE} t WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({UTM_FILTER_T}) {reg_filter} GROUP BY 1)
    SELECT days.d AS date, COALESCE(ld.leads,0) AS leads, COALESCE(qd.quals,0) AS quals, COALESCE(pqd.prequals,0) AS prequals
    FROM days
    LEFT JOIN lead_days ld ON ld.d = days.d
    LEFT JOIN qual_days qd ON qd.d = days.d
    LEFT JOIN prequal_days pqd ON pqd.d = days.d
    ORDER BY days.d
    """
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to, **reg_params})


def _daily_series_by_region_raw(engine, date_from, date_to, region_list):
    return _daily_series_raw(engine, date_from, date_to)


kpi_period = kpi_extended
