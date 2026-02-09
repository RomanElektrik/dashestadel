# -*- coding: utf-8 -*-
"""
Подключение к Supabase (Postgres) и запросы для дашборда.
Нужна переменная окружения SUPABASE_DB_URL (Connection string из Supabase → Project Settings → Database).
Пример: postgresql://postgres.[ref]:[PASSWORD]@aws-0-eu-central-1.pooler.supabase.com:6543/postgres
Можно положить в .env в папке скрипта (при установленном python-dotenv).
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

TABLE = '"For dash"'
REGION_EXPR = "COALESCE(NULLIF(TRIM(t.region_kvalifikacii), ''), NULLIF(TRIM(t.direction), ''), NULLIF(TRIM(t.region_klienta), ''))"


REGION_EXPR_NO_ALIAS = "COALESCE(NULLIF(TRIM(region_kvalifikacii), ''), NULLIF(TRIM(direction), ''), NULLIF(TRIM(region_klienta), ''))"

# Как в «рабочий скрипт + квалы.js»: квалы по направлению = поле «Дата квалификации [направление]» в периоде
DIRECTION_QUAL_DATE_COLUMN = {
    "Крым": "qualification_date_krym",
    "Сочи": "qualification_date_sochi",
    "Анапа": "qualification_date_anapa",
    "Баку": "qualification_date_baku",
}
ALLOWED_DIRECTIONS = list(DIRECTION_QUAL_DATE_COLUMN.keys())


def _region_filter_sql(region=None, region_list=None, use_table_alias=True):
    """Возвращает (sql_fragment, params) для фильтра по региону или списку регионов.
    use_table_alias=True — для запросов с алиасом t, иначе для таблицы без алиаса."""
    expr = REGION_EXPR if use_table_alias else REGION_EXPR_NO_ALIAS
    if region_list and len(region_list) > 0:
        parts = [f"({expr} ILIKE :reg{i})" for i in range(len(region_list))]
        return " AND ( " + " OR ".join(parts) + " )", {f"reg{i}": f"%{r}%" for i, r in enumerate(region_list)}
    if region and region != "Все":
        return f" AND ( {expr} ILIKE :region )", {"region": f"%{region}%"}
    return "", {}


def get_engine():
    url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not url:
        return None
    if not url.startswith("postgresql"):
        url = url.replace("postgres://", "postgresql://", 1)
    # Pooler Supabase слушает на 6543, не 5432 — подменяем, если в секрете остался старый порт
    if "pooler.supabase.com" in url and ":5432/" in url:
        url = url.replace(":5432/", ":6543/", 1)
    # Важно: pooler + ssl могут быть медленными при создании коннекта.
    # Делаем быстрый фейл при проблемах сети + выставляем таймаут запросов.
    # NullPool не кэширует соединения — pool_pre_ping не нужен и может мешать
    return create_engine(
        url,
        poolclass=NullPool,
        connect_args={
            "connect_timeout": 30,
            "options": "-c timezone=Europe/Moscow -c statement_timeout=120000",
        },
    )


def run_sql(engine, sql: str, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def kpi_period(engine, date_from, date_to, region=None):
    """Лиды, квалы, предквалы, паспорта за период (по региону опционально)."""
    reg_filter = ""
    if region and region != "Все":
        reg_filter = f""" AND (
            COALESCE(NULLIF(TRIM(t.region_kvalifikacii), ''), NULLIF(TRIM(t.direction), ''), NULLIF(TRIM(t.region_klienta), ''))
            ILIKE :region
        )"""
    sql = f"""
    SELECT
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports
    FROM public.{TABLE} t
    WHERE 1=1
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    """
    params = {"d_from": date_from, "d_to": date_to}
    if region and region != "Все":
        params["region"] = f"%{region}%"
    return run_sql(engine, sql, params)


def by_region(engine, date_from, date_to):
    """Разбивка по региону за период."""
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(region_kvalifikacii), ''), NULLIF(TRIM(direction), ''), NULLIF(TRIM(region_klienta), ''), '(не указан)') AS region,
      COUNT(*) FILTER (WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports
    FROM public.{TABLE}
    WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
       OR (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
       OR (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
       OR (pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
    GROUP BY 1
    ORDER BY leads DESC
    """
    return run_sql(engine, sql, {"d_from": date_from, "d_to": date_to})


def by_utm(engine, date_from, date_to, region=None, region_list=None, limit=20):
    """Разбивка по UTM за период."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list, use_table_alias=False)
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(utm_source), ''), '(без source)') AS utm_source,
      COALESCE(NULLIF(TRIM(utm_medium), ''), '(без medium)') AS utm_medium,
      COALESCE(NULLIF(TRIM(utm_campaign), ''), '(без campaign)') AS utm_campaign,
      COUNT(*) FILTER (WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals
    FROM public.{TABLE}
    WHERE (
      (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
    ) {reg_filter}
    GROUP BY utm_source, utm_medium, utm_campaign
    ORDER BY leads DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def by_formname(engine, date_from, date_to, region=None, region_list=None, limit=25):
    """Разбивка по formname за период."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list, use_table_alias=False)
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(formname), ''), '(пусто)') AS formname,
      COUNT(*) FILTER (WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports
    FROM public.{TABLE}
    WHERE (
      (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
    ) {reg_filter}
    GROUP BY formname
    ORDER BY leads DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def _landing_expr(use_table_alias=True):
    """Выражение для посадочной: URL без query (utm_referrer или referrer)."""
    t = "t." if use_table_alias else ""
    # Убираем ? и всё после; пустая строка → (без посадки)
    return f"""COALESCE(
      NULLIF(TRIM(SPLIT_PART(COALESCE({t}utm_referrer, {t}referrer, ''), '?', 1)), ''),
      '(без посадки)'
    )"""


def by_landing(engine, date_from, date_to, region=None, region_list=None, limit=30):
    """Разбивка по посадочной странице (utm_referrer): лиды, предквалы, квалы, показ назначен, показ проведён, паспорт, бронь."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list, use_table_alias=True)
    landing_sql = _landing_expr(use_table_alias=True)
    sql = f"""
    SELECT
      {landing_sql} AS landing,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (t.pokaz_naznachen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz_naznachen,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz_proveden,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni
    FROM public.{TABLE} t
    WHERE (
      (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.pokaz_naznachen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
    ) {reg_filter}
    GROUP BY {landing_sql}
    ORDER BY leads DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def by_source_key(engine, date_from, date_to, region=None, limit=20):
    """Разбивка по source_key за период."""
    reg_filter = ""
    if region and region != "Все":
        reg_filter = """ AND (
            COALESCE(NULLIF(TRIM(region_kvalifikacii), ''), NULLIF(TRIM(direction), ''), NULLIF(TRIM(region_klienta), ''))
            ILIKE :region
        )"""
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(raw_data->>'source_key'), ''), '(пусто)') AS source_key,
      COUNT(*) FILTER (WHERE (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals
    FROM public.{TABLE}
    WHERE (
      (lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      OR (qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
    ) {reg_filter}
    GROUP BY source_key
    ORDER BY leads DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit}
    if region and region != "Все":
        params["region"] = f"%{region}%"
    return run_sql(engine, sql, params)


def date_bounds(engine):
    """Минимальная и максимальная дата по lead_created_at для выбора периода."""
    sql = f"""
    SELECT
      MIN((lead_created_at AT TIME ZONE 'Europe/Moscow')::date) AS min_d,
      MAX((lead_created_at AT TIME ZONE 'Europe/Moscow')::date) AS max_d
    FROM public.{TABLE}
    WHERE lead_created_at IS NOT NULL
    """
    return run_sql(engine, sql)


def region_list(engine):
    """Список регионов для фильтра."""
    sql = f"""
    SELECT DISTINCT
      COALESCE(NULLIF(TRIM(region_kvalifikacii), ''), NULLIF(TRIM(direction), ''), NULLIF(TRIM(region_klienta), '')) AS region
    FROM public.{TABLE}
    WHERE (region_kvalifikacii IS NOT NULL AND TRIM(region_kvalifikacii) <> '')
       OR (direction IS NOT NULL AND TRIM(direction) <> '')
       OR (region_klienta IS NOT NULL AND TRIM(region_klienta) <> '')
    ORDER BY 1
    """
    df = run_sql(engine, sql)
    regions = ["Все"] + df["region"].dropna().astype(str).tolist()
    return regions


def kpi_extended(engine, date_from, date_to, region=None, region_list=None):
    """Расширенные KPI: лиды, квалы, предквалы, показы, брони, сделки, комиссии. Квалы по направлению — по полю даты квалификации направления (как в рабочем скрипте)."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else None
    quals_expr = f"(t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to" if qual_col else "(t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to"
    sql = f"""
    SELECT
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE {quals_expr}) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to 
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki,
      COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0) AS komissi
    FROM public.{TABLE} t
    WHERE 1=1
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.{qual_col or 'qualification_date'} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def kpi_by_region(engine, date_from, date_to, region_list):
    """KPI по каждому направлению из списка. Квалы считаются по полю «Дата квалификации [направление]» в периоде (как в рабочем скрипте)."""
    if not region_list:
        return pd.DataFrame()
    # Квалы по направлению = qualification_date_krym / _sochi / _anapa / _baku в периоде (как в рабочем скрипте)
    use_direction_qual_columns = all(r in DIRECTION_QUAL_DATE_COLUMN for r in region_list)
    reg_filter, reg_params = _region_filter_sql(region_list=region_list)
    if use_direction_qual_columns:
        # Квалы по направлению = все лиды, у которых дата квалификации по этому направлению в периоде (как в рабочем скрипте)
        parts = []
        for i, direction in enumerate(region_list):
            qual_col = DIRECTION_QUAL_DATE_COLUMN.get(direction)
            if not qual_col:
                continue
            qual_filter = f"((t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)"
            part = f"""
    SELECT
      :dir{i} AS region,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS leads,
      COUNT(*) FILTER (WHERE {qual_filter}) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS prequals,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS passports,
      COUNT(*) FILTER (WHERE ((t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AND ({REGION_EXPR} ILIKE :regdir{i})) AS pokaz,
      COUNT(*) FILTER (WHERE ((t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AND ({REGION_EXPR} ILIKE :regdir{i})) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS sdelki,
      COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})), 0) AS komissi
    FROM public.{TABLE} t
    WHERE (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
"""
            parts.append(part.strip())
            reg_params[f"dir{i}"] = direction
            reg_params[f"regdir{i}"] = f"%{direction}%"
        sql = " UNION ALL ".join(parts) + " ORDER BY leads DESC"
        params = {"d_from": date_from, "d_to": date_to, **reg_params}
        return run_sql(engine, sql, params)
    # старый вариант: регионы не из списка направлений
    sql = f"""
    SELECT
      {REGION_EXPR} AS region,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS passports,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki,
      COALESCE(SUM(t.obshaya_summa_komissiy) FILTER (WHERE (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0) AS komissi
    FROM public.{TABLE} t
    WHERE 1=1
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.komissiya_poluchena AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    GROUP BY {REGION_EXPR}
    ORDER BY leads DESC
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def funnel_data(engine, date_from, date_to, region=None, region_list=None):
    """Данные для воронки. Квалы по направлению — по полю даты квалификации направления (как в рабочем скрипте)."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else None
    quals_expr = f"(t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to" if qual_col else "(t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to"
    sql = f"""
    SELECT
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE {quals_expr}) AS quals,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to 
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki
    FROM public.{TABLE} t
    WHERE 1=1
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.{qual_col or 'qualification_date'} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def funnel_by_region(engine, date_from, date_to, region_list):
    """Воронка по каждому направлению. Квалы — по полю «Дата квалификации [направление]» в периоде (как в рабочем скрипте)."""
    if not region_list:
        return pd.DataFrame()
    use_direction_qual_columns = all(r in DIRECTION_QUAL_DATE_COLUMN for r in region_list)
    reg_filter, reg_params = _region_filter_sql(region_list=region_list)
    if use_direction_qual_columns:
        parts = []
        for i, direction in enumerate(region_list):
            qual_col = DIRECTION_QUAL_DATE_COLUMN.get(direction)
            if not qual_col:
                continue
            qual_filter = f"((t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to)"
            part = f"""
    SELECT
      :dir{i} AS region,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS leads,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS prequals,
      COUNT(*) FILTER (WHERE {qual_filter}) AS quals,
      COUNT(*) FILTER (WHERE ((t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AND ({REGION_EXPR} ILIKE :regdir{i})) AS pokaz,
      COUNT(*) FILTER (WHERE ((t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AND ({REGION_EXPR} ILIKE :regdir{i})) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to AND ({REGION_EXPR} ILIKE :regdir{i})) AS sdelki
    FROM public.{TABLE} t
    WHERE (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
"""
            parts.append(part.strip())
            reg_params[f"dir{i}"] = direction
            reg_params[f"regdir{i}"] = f"%{direction}%"
        sql = " UNION ALL ".join(parts) + " ORDER BY leads DESC"
        params = {"d_from": date_from, "d_to": date_to, **reg_params}
        return run_sql(engine, sql, params)
    sql = f"""
    SELECT
      {REGION_EXPR} AS region,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS prequals,
      COUNT(*) FILTER (WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS quals,
      COUNT(*) FILTER (WHERE (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS pokaz,
      COUNT(*) FILTER (WHERE (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
                          OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS broni,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki
    FROM public.{TABLE} t
    WHERE 1=1
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.pokaz_proveden_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.objekt_zabronirovan AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.data_oplaty_broni_1 AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    GROUP BY {REGION_EXPR}
    ORDER BY leads DESC
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def top_managers(engine, date_from, date_to, region=None, region_list=None, limit=10):
    """Топ менеджеров по квалам за период. Квалы по направлению — по полю даты квалификации направления."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else None
    quals_expr = f"(t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to" if qual_col else "(t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to"
    sql = f"""
    SELECT
      COALESCE(
        NULLIF(TRIM(t.raw_data->'_embedded'->'responsible'->>'name'), ''),
        NULLIF(TRIM(t.raw_data->>'ответственный'), ''),
        NULLIF(TRIM(t.raw_data->>'responsible_user_name'), ''),
        NULLIF(TRIM(t.raw_data->>'responsible_name'), ''),
        NULLIF(TRIM(t.raw_data->>'responsible_user'), ''),
        NULLIF(TRIM(t.raw_data->>'responsible'), ''),
        ('ID ' || t.responsible_user_id::text)
      ) AS broker_name,
      COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS leads,
      COUNT(*) FILTER (WHERE {quals_expr}) AS quals,
      COUNT(*) FILTER (WHERE (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to) AS sdelki,
      ROUND(
        100.0 * COUNT(*) FILTER (WHERE {quals_expr})
        / NULLIF(COUNT(*) FILTER (WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to), 0),
        1
      ) AS conv_percent
    FROM public.{TABLE} t
    WHERE t.responsible_user_id IS NOT NULL
      AND (
        (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
        OR ({quals_expr})
        OR (t.sdelka_sostoyalas AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      )
      {reg_filter}
    GROUP BY 1
    HAVING COUNT(*) FILTER (WHERE {quals_expr}) > 0
    ORDER BY quals DESC, sdelki DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def deal_stages(engine, date_from, date_to, region=None, region_list=None, limit=12):
    """Этапы сделок/лидов (по статусу) для выбранного периода."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    sql = f"""
    SELECT
      COALESCE(
        NULLIF(TRIM(t.raw_data->>'status_name'), ''),
        NULLIF(TRIM(t.raw_data->>'status'), ''),
        NULLIF(TRIM(t.prev_etap), ''),
        NULLIF(TRIM(t.prev_etap_id), ''),
        NULLIF(TRIM(t.status_id::text), ''),
        '(не указан)'
      ) AS stage,
      COUNT(*) AS cnt
    FROM public.{TABLE} t
    WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      {reg_filter}
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def rejection_reasons(engine, date_from, date_to, region=None, region_list=None, limit=15):
    """Причины отказа за период (по дате закрытия/не реализовано)."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    sql = f"""
    SELECT
      COALESCE(NULLIF(TRIM(t.prichina_otkaza), ''), '(не указана)') AS reason,
      COUNT(*) AS cnt
    FROM public.{TABLE} t
    WHERE (COALESCE(t.zakryto_ne_realizovano, t.closed_at) AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      AND t.prichina_otkaza IS NOT NULL
      AND TRIM(t.prichina_otkaza) <> ''
      {reg_filter}
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT :lim
    """
    params = {"d_from": date_from, "d_to": date_to, "lim": limit, **reg_params}
    return run_sql(engine, sql, params)


def daily_series(engine, date_from, date_to, region=None, region_list=None):
    """По дням: дата, лиды, квалы (по полю направления при выборе Крым/Сочи/Анапа/Баку), предквалы, паспорта."""
    reg_filter, reg_params = _region_filter_sql(region=region, region_list=region_list)
    qual_col = DIRECTION_QUAL_DATE_COLUMN.get(region) if region and region != "Все" else "qualification_date"
    sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    ),
    lead_days AS (
      SELECT (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS leads
      FROM public.{TABLE} t
      WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1
    ),
    qual_days AS (
      SELECT (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS quals
      FROM public.{TABLE} t
      WHERE (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1
    ),
    prequal_days AS (
      SELECT (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS prequals
      FROM public.{TABLE} t
      WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1
    ),
    passport_days AS (
      SELECT (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date AS d, COUNT(*) AS passports
      FROM public.{TABLE} t
      WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1
    )
    SELECT days.d AS date,
      COALESCE(ld.leads, 0) AS leads,
      COALESCE(qd.quals, 0) AS quals,
      COALESCE(pqd.prequals, 0) AS prequals,
      COALESCE(pd.passports, 0) AS passports
    FROM days
    LEFT JOIN lead_days ld ON ld.d = days.d
    LEFT JOIN qual_days qd ON qd.d = days.d
    LEFT JOIN prequal_days pqd ON pqd.d = days.d
    LEFT JOIN passport_days pd ON pd.d = days.d
    ORDER BY days.d
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)


def daily_series_by_region(engine, date_from, date_to, region_list):
    """По дням и направлениям: дата, регион, лиды, квалы (по полю даты квалификации направления), предквалы, паспорта."""
    if not region_list:
        return pd.DataFrame()
    use_direction_qual_columns = all(r in DIRECTION_QUAL_DATE_COLUMN for r in region_list)
    reg_filter, reg_params = _region_filter_sql(region_list=region_list)
    if use_direction_qual_columns:
        # regions = выбранные направления; qual_days = по qualification_date_krym/_sochi/... по дням
        regions_values = ", ".join(f"(:dir{i})" for i in range(len(region_list)))
        qual_parts = []
        for i, direction in enumerate(region_list):
            qual_col = DIRECTION_QUAL_DATE_COLUMN.get(direction)
            if not qual_col:
                continue
            qual_parts.append(f"""
      SELECT (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date AS d, :dir{i} AS region, COUNT(*) AS quals
      FROM public.{TABLE} t
      WHERE (t.{qual_col} AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to
      GROUP BY 1
""")
        qual_days_cte = " UNION ALL ".join(p.strip() for p in qual_parts)
        for i, direction in enumerate(region_list):
            reg_params[f"dir{i}"] = direction
        sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    ),
    regions AS (
      SELECT * FROM (VALUES {regions_values}) AS v(region)
    ),
    lead_days AS (
      SELECT (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS leads
      FROM public.{TABLE} t
      WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    ),
    qual_days AS (
      {qual_days_cte}
    ),
    prequal_days AS (
      SELECT (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS prequals
      FROM public.{TABLE} t
      WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    ),
    passport_days AS (
      SELECT (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS passports
      FROM public.{TABLE} t
      WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    )
    SELECT days.d AS date, regions.region,
      COALESCE(ld.leads, 0) AS leads,
      COALESCE(qd.quals, 0) AS quals,
      COALESCE(pqd.prequals, 0) AS prequals,
      COALESCE(pd.passports, 0) AS passports
    FROM days
    CROSS JOIN regions
    LEFT JOIN lead_days ld ON ld.d = days.d AND ld.region = regions.region
    LEFT JOIN qual_days qd ON qd.d = days.d AND qd.region = regions.region
    LEFT JOIN prequal_days pqd ON pqd.d = days.d AND pqd.region = regions.region
    LEFT JOIN passport_days pd ON pd.d = days.d AND pd.region = regions.region
    ORDER BY days.d, regions.region
    """
        params = {"d_from": date_from, "d_to": date_to, **reg_params}
        return run_sql(engine, sql, params)
    sql = f"""
    WITH days AS (
      SELECT generate_series(CAST(:d_from AS date), CAST(:d_to AS date), '1 day'::interval)::date AS d
    ),
    regions AS (
      SELECT DISTINCT {REGION_EXPR} AS region FROM public.{TABLE} t
      WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
    ),
    lead_days AS (
      SELECT (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS leads
      FROM public.{TABLE} t
      WHERE (t.lead_created_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    ),
    qual_days AS (
      SELECT (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS quals
      FROM public.{TABLE} t
      WHERE (t.qualification_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    ),
    prequal_days AS (
      SELECT (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS prequals
      FROM public.{TABLE} t
      WHERE (t.pre_qual_date AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    ),
    passport_days AS (
      SELECT (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date AS d, {REGION_EXPR} AS region, COUNT(*) AS passports
      FROM public.{TABLE} t
      WHERE (t.pasport_poluchen AT TIME ZONE 'Europe/Moscow')::date BETWEEN :d_from AND :d_to {reg_filter}
      GROUP BY 1, 2
    )
    SELECT days.d AS date, regions.region,
      COALESCE(ld.leads, 0) AS leads,
      COALESCE(qd.quals, 0) AS quals,
      COALESCE(pqd.prequals, 0) AS prequals,
      COALESCE(pd.passports, 0) AS passports
    FROM days
    CROSS JOIN regions
    LEFT JOIN lead_days ld ON ld.d = days.d AND ld.region = regions.region
    LEFT JOIN qual_days qd ON qd.d = days.d AND qd.region = regions.region
    LEFT JOIN prequal_days pqd ON pqd.d = days.d AND pqd.region = regions.region
    LEFT JOIN passport_days pd ON pd.d = days.d AND pd.region = regions.region
    ORDER BY days.d, regions.region
    """
    params = {"d_from": date_from, "d_to": date_to, **reg_params}
    return run_sql(engine, sql, params)
