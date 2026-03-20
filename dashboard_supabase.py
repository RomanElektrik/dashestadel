# -*- coding: utf-8 -*-
"""
Дашборд Эстадель на данных Supabase (чтение только из кэш-таблиц; сырьё «For dash» — для ETL/обновления кэша).
Подключение: переменная окружения SUPABASE_DB_URL (Connection string из Supabase → Database).
Запуск: streamlit run dashboard_supabase.py
"""
import os
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# Google Таблица: ФОРМА ОМ estadel stat 2026
GOOGLE_SHEET_BASE = "https://docs.google.com/spreadsheets/d/1F70-t8ORm9jmuJozGV1ATxvn3RizFBe1yU6MKzSVFDs/export?format=csv&gid="
# URL для экспорта диапазона (gviz/tq)
GOOGLE_SHEET_GVIZ = "https://docs.google.com/spreadsheets/d/1F70-t8ORm9jmuJozGV1ATxvn3RizFBe1yU6MKzSVFDs/gviz/tq?tqx=out:csv&gid={gid}&range={range}"
# Опциональный диапазон для листов Источников (если указан — gviz; иначе полный лист)
IST_FORMA_RANGES = {
    "Ист Крым Всего": "A1:CS37",
    "Ист Сочи":       "A4:BR37",
}
OM_FORMA_GIDS = {
    "ОМ Крым": "0",
    "ОМ Крым Всего": "1127804293",
    "ОМ Сочи-Крым": "1204764825",
    "ОМ Баку": "1124279371",
    "ОМ Анапа": "1147051445",
    "ОМ Монолит": "1935950491",
}
# Легенда этапов сделок (как у доната/воронки сверху: 2×3 и далее по кругу)
STAGE_LEGEND_PALETTE = [
    "#A78BFA",
    "#C4B5FD",
    "#34D399",
    "#FBBF24",
    "#EF4444",
    "#2DD4BF",
    "#6366F1",
    "#8B5CF6",
    "#06B6D4",
    "#0EA5E9",
    "#10B981",
    "#84CC16",
    "#F59E0B",
    "#F97316",
    "#EC4899",
]


OM_COLORS = {
    "ОМ Крым":       "#3B82F6",
    "ОМ Крым Всего": "#6366F1",
    "ОМ Сочи-Крым":  "#10B981",
    "ОМ Баку":       "#F59E0B",
    "ОМ Анапа":      "#EF4444",
    "ОМ Монолит":    "#8B5CF6",
}

IST_FORMA_GIDS = {
    "Ист Крым": "1413105299",
    "Ист Сочи": "129195735",
    "Ист Крым Всего": "53440241",
    "Ист Анапа": "304814993",
    "Ист Баку": "146312803",
    "Ист Монолит": "2076452012",
}
IST_COLORS = {
    "Ист Крым":       "#3B82F6",
    "Ист Сочи":       "#10B981",
    "Ист Крым Всего": "#6366F1",
    "Ист Анапа":      "#EF4444",
    "Ист Баку":       "#F59E0B",
    "Ист Монолит":    "#8B5CF6",
}
from dashboard_supabase_data import (
    get_engine,
    run_sql,
    get_responsible_id_to_name_map,
    kpi_period,
    kpi_extended,
    kpi_by_region,
    daily_series,
    daily_series_by_region,
    by_region,
    by_utm,
    by_formname,
    by_landing,
    date_bounds,
    region_list,
    funnel_data,
    funnel_by_region,
    FUNNEL_STAGES,
    DB_SCHEMA_HINT,
    top_managers,
    deal_stages,
    deal_stages_funnel,
    rejection_reasons,
    ensure_cache_table,
    cache_is_empty,
)

st.set_page_config(
    page_title="Эстадель — Аналитика",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Стиль: Glassmorphism + Dark Neomorphism, Syne/Space Grotesk, курсоры, ховеры, тултипы графиков
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
    .stApp { background: linear-gradient(180deg, #0a0a0f 0%, #0d0d14 50%, #0a0a0f 100%) !important; min-height: 100vh; }
    .main .block-container { padding: 0.5rem 2rem 1.5rem 2rem; max-width: 100%; }
    .main .block-container > div:first-child h1,
    .main .block-container > div:first-child h2,
    .main .block-container > div:first-child h3 { margin-top: 0 !important; }
    .main .block-container > div:first-child { padding-top: 0 !important; margin-top: 0 !important; }
    * { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; cursor: default; }
    /* Типографика: заголовки — Syne/Space Grotesk */
    h1 { font-family: 'Syne', 'Space Grotesk', sans-serif !important; color: #FFFFFF !important; font-weight: 700; font-size: 2rem; margin-bottom: 0.5rem !important; }
    h2 { font-family: 'Syne', 'Space Grotesk', sans-serif !important; color: #E2E8F0 !important; font-weight: 600; font-size: 1.35rem; margin-top: 1.15rem !important; margin-bottom: 0.65rem !important; }
    h3 { font-family: 'Syne', 'Space Grotesk', sans-serif !important; color: #A78BFA !important; font-weight: 600; font-size: 1.05rem; margin-top: 0.95rem !important; margin-bottom: 0.5rem !important; }
    /* Подзаголовки секций */
    .main h2, .main h3, .stMarkdown h2, .stMarkdown h3 { font-family: 'Syne', 'Space Grotesk', sans-serif !important; color: #E2E8F0 !important; }
    .main h3, .stMarkdown h3 { color: #A78BFA !important; }
    /* Подписи и метки — мелкие, приглушённый серый */
    [data-testid="stMetricLabel"], .stCaption, .dash-balance-title { color: #6b6b80 !important; font-size: 11px !important; font-family: 'Space Grotesk', sans-serif !important; }
    /* Суммы и цифры — tabular-nums, чтобы не прыгали */
    [data-testid="stMetricValue"], .dash-balance-value, .stDataFrame td { font-variant-numeric: tabular-nums !important; }
    /* Glassmorphism + dark neomorphism карточки */
    [data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4) !important;
        backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 16px;
        padding: 1.25rem 1rem;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.35);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        cursor: pointer;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-6px);
        box-shadow: 0 14px 44px rgba(0, 0, 0, 0.45);
        border-color: rgba(255, 255, 255, 0.08);
    }
    /* Значения метрик (цифры) — как на скрине: фиолетовые + лёгкий glow, без gradient-clip */
    [data-testid="stMetricValue"],
    [data-testid="stMetricValue"] * {
        font-family: 'Syne', 'Space Grotesk', sans-serif !important;
        letter-spacing: -0.01em !important;
    }
    [data-testid="stMetricValue"] {
        color: #C4B5FD !important;
        -webkit-text-fill-color: #C4B5FD !important;
        background: none !important;
        -webkit-background-clip: unset !important;
        background-clip: unset !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
        line-height: 1.15 !important;
        text-shadow:
            0 0 14px rgba(167, 139, 250, 0.28),
            0 0 4px rgba(196, 181, 253, 0.18) !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.85rem !important;
        font-variant-numeric: tabular-nums !important;
        font-family: 'Syne', 'Space Grotesk', sans-serif !important;
        color: #94A3B8 !important;
    }
    [data-testid="stSidebar"] {
        background: rgba(15, 15, 20, 0.85) !important;
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255, 255, 255, 0.06);
    }
    [data-testid="stSidebar"] label { color: #E2E8F0 !important; font-weight: 500; }
    /* Таблицы: glassmorphism, ховер строки */
    .stDataFrame {
        background: rgba(30, 41, 59, 0.4) !important;
        backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 14px;
        overflow: hidden;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.2);
    }
    .stDataFrame th { background: rgba(37, 37, 53, 0.9) !important; color: #A78BFA !important; font-weight: 600; font-size: 11px !important; text-transform: uppercase; letter-spacing: 0.5px; font-family: 'Space Grotesk', sans-serif !important; padding: 0.75rem 1rem !important; border-bottom: 1px solid rgba(255, 255, 255, 0.06) !important; }
    .stDataFrame th:first-child { border-top-left-radius: 14px !important; }
    .stDataFrame th:last-child { border-top-right-radius: 14px !important; }
    .stDataFrame td { background: rgba(15, 15, 20, 0.5) !important; color: #E2E8F0 !important; font-family: 'Space Grotesk', sans-serif !important; padding: 0.65rem 1rem !important; border-bottom: 1px solid rgba(255, 255, 255, 0.04) !important; }
    .stDataFrame tbody tr:hover td { background: rgba(255, 255, 255, 0.03) !important; }
    .stDataFrame tbody tr:last-child td { border-bottom: none !important; }
    .stDataFrame tbody tr:last-child td:first-child { border-bottom-left-radius: 14px !important; }
    .stDataFrame tbody tr:last-child td:last-child { border-bottom-right-radius: 14px !important; }
    [data-testid="stAlert"], .stAlert { border-radius: 14px !important; overflow: hidden !important; }
    /* Не оборачиваем каждый блок с графиком/таблицей в «карточку»:
       это ломает ровность сетки в колонках и даёт “скачки” отступов по странице. */
    div[data-testid="stVerticalBlock"] > div { background: transparent !important; }
    /* Убираем пустоту между колонками (баланс + KPI) */
    [data-testid="stHorizontalBlock"] { gap: 0.85rem !important; }
    /* Разделители в стиле дашборда (чуть компактнее, ровнее по секциям) */
    hr { border: none !important; border-top: 1px solid rgba(255, 255, 255, 0.06) !important; margin: 1.1rem 0 !important; }
    [data-testid="stHorizontalBlock"] > div:empty + div hr { border-color: rgba(255, 255, 255, 0.06) !important; }
    /* Курсоры: графики — crosshair; кнопки и карточки — pointer */
    .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"], .stPlotlyChart { cursor: crosshair !important; }
    a, button, [role="button"], .stSelectbox > div, [data-testid="stMetric"] { cursor: pointer !important; }

    /* Убираем кнопки полноэкранного режима (часто вызывают тяжёлый rerender и лаги) */
    .stPlotlyChart button[aria-label*="full"], .stPlotlyChart button[title*="full"],
    .stDataFrame button[aria-label*="full"], .stDataFrame button[title*="full"] {
        display: none !important;
    }
    /* Кнопки: scale + усиление glow при ховере */
    .stButton > button {
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .stButton > button:hover {
        transform: scale(1.02);
        box-shadow: 0 0 20px rgba(167, 139, 250, 0.35);
    }
    /* Детальные разбивки: табы в стиле карточек */
    .stTabs [data-baseweb="tab-list"] { background: rgba(30, 41, 59, 0.5); border-radius: 10px; padding: 0.25rem; border: 1px solid rgba(255, 255, 255, 0.06); margin-bottom: 0.75rem; }
    .stTabs [data-baseweb="tab"] { color: #6b6b80; border-radius: 8px; cursor: pointer !important; font-family: 'Space Grotesk', sans-serif !important; font-size: 12px !important; font-weight: 500; }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, #7C3AED, #A78BFA) !important; color: #FFFFFF !important; }
    /* Контент внутри таба (По региону, По UTM, По formname, По посадочной) — единая карточка */
    [data-testid="stTabs"] > div:last-child,
    .stTabs [data-baseweb="tab-panel"] {
        background: rgba(30, 41, 59, 0.2) !important;
        backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 14px;
        padding: 1.25rem;
        margin-top: 0.25rem;
    }
    /* Блок баланса: крупные цифры 36–42px */
    .dash-balance {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(52, 211, 153, 0.08) 50%, rgba(167, 139, 250, 0.1) 100%);
        backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 20px; padding: 1.45rem 1.7rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        margin-bottom: 0.45rem;
    }
    .dash-balance:hover {
        transform: translateY(-6px);
        box-shadow: 0 14px 44px rgba(0, 185, 129, 0.15);
    }
    .dash-balance-title { font-size: 11px !important; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 0.5rem; }
    .dash-balance-value { font-family: 'Syne', 'Space Grotesk', sans-serif !important; font-size: 2.35rem !important; font-weight: 800; background: linear-gradient(135deg, #34D399, #A7F3D0); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
    .dash-card {
        background: rgba(30, 41, 59, 0.35); backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 14px;
        padding: 1rem; margin-bottom: 0.75rem;
    }
    /* Тултипы Plotly — скругление */
    .hoverlayer .hovertext { border-radius: 8px !important; }
    /* Все графики — скруглённая область (столбцы/линии плавно в контейнере) */
    [data-testid="stPlotlyChart"], .stPlotlyChart, .js-plotly-plot { border-radius: 14px !important; overflow: hidden !important; }
    .js-plotly-plot .svg-container, .plotly .main-svg { border-radius: 14px !important; }
    /* Убрать кнопки fullscreen/expand/стрелочки у всех графиков и таблиц */
    [data-testid="stPlotlyChart"] button[aria-label],
    [data-testid="stDataFrame"] button[aria-label],
    button[aria-label*="full"],
    button[aria-label*="Full"],
    button[aria-label*="expand"],
    button[aria-label*="Expand"],
    button[aria-label*="zoom"],
    [data-testid="StyledFullScreenButton"],
    [data-testid="stFullScreenFrame"] > button,
    .stPlotlyChart > div > div > div > button { display: none !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "doubleClick": False,
    "showTips": False,
    "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
}

# Единый стиль тултипов: тёмная плашка, тонкая рамка (финтех-дашборд)
PLOTLY_HOVERLABEL = dict(
    bgcolor="#252535",
    bordercolor="rgba(255,255,255,0.1)",
    font=dict(size=11, color="#E2E8F0", family="'Space Grotesk', sans-serif"),
    align="left",
)
# Скругление столбцов у всех bar-графиков (Plotly 5.18+; в старых версиях игнорируется)
BAR_MARKER_ROUND = dict(cornerradius=8)


def _apply_bar_rounded(fig):
    """Применить скругление столбцов; не падать, если Plotly не поддерживает."""
    try:
        fig.update_traces(marker=BAR_MARKER_ROUND)
    except Exception:
        pass


def get_openai_client():
    if load_dotenv is not None:
        try:
            load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
        except Exception:
            pass
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    return OpenAI(api_key=api_key)


def _normalize_ai_sql(raw_sql: str) -> str:
    sql_raw = str(raw_sql or "").strip()
    if not sql_raw:
        raise ValueError("GPT не вернул SQL.")

    # Убираем markdown-кодфенсы и их маркеры (GPT часто оборачивает)
    sql = re.sub(r"```(?:sql)?", "", sql_raw, flags=re.IGNORECASE)
    sql = sql.replace("```", "").strip()

    # Если модель вернула "объект" или текст вокруг SQL — достаём первый SELECT/WITH
    m = re.search(r"(?is)\b(select|with)\b", sql)
    if not m:
        raise ValueError("Не удалось извлечь SQL из ответа GPT.")
    sql = sql[m.start() :].strip()

    # Внутри ответа могут встречаться хвостовые символы/пояснения
    # Оставим до первого ';', если он есть (иначе это уже несколько запросов)
    if ";" in sql:
        candidate = sql.split(";", 1)[0].strip()
        if not candidate:
            raise ValueError("GPT вернул некорректный SQL.")
        sql = candidate

    sql_lower = sql.lower().strip()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise ValueError("Разрешены только SELECT / WITH запросы.")

    forbidden = [
        "insert", "update", "delete", "drop", "alter", "truncate",
        "create", "grant", "revoke", "comment", "merge", "copy",
        "vacuum", "analyze", "call", "do",
    ]
    for token in forbidden:
        if re.search(rf"\b{token}\b", sql_lower):
            raise ValueError(f"Запрещённый SQL-оператор: {token.upper()}")
    if not re.search(r"\blimit\b", sql_lower):
        sql = f"{sql}\nLIMIT 10000"
    return sql


def ask_gpt_for_sql(client, user_question: str, chat_history: list) -> tuple:
    SYSTEM_PROMPT = f"""Ты — аналитик данных компании Эстадель встроенный в дашборд.

ВАЖНО: Ты УМЕЕШЬ выполнять SQL запросы к базе данных. Когда ты генерируешь SQL — система автоматически его выполняет и показывает результат пользователю. Тебе НЕ НУЖНО просить пользователя выполнять запросы самому.

ТЕКУЩИЙ ГОД: 2026. Если пользователь говорит "январь", "февраль" и т.д. без года — имеется в виду 2026 год.

АЛГОРИТМ РАБОТЫ:
1. Если запрос понятен — СРАЗУ генерируй SQL без предупреждений типа "сейчас определю", "подождите", "сейчас подготовлю запрос"
2. НЕ пиши вводные фразы перед SQL — сразу начинай с SELECT или WITH
3. НЕ говори "выполните запрос", "подождите немного", "сейчас подготовлю" — просто генерируй SQL
4. После выполнения SQL — сразу анализируй результаты и дай выводы
5. Если нужно уточнение — задай один короткий вопрос, без лишних слов

ЗАПРЕЩЕНО писать перед SQL:
- "Сейчас определю..."
- "Подождите немного"
- "Сейчас подготовлю запрос"
- "Давайте выгрузим..."
- "Отлично, вот SQL-запрос..."
- Любые вводные фразы — только чистый SQL

ПОСАДОЧНЫЕ СТРАНИЦЫ = utm_referrer (не utm_source). Всегда используй SPLIT_PART(utm_referrer, '?', 1) чтобы убрать параметры из URL.

ЭФФЕКТИВНОСТЬ ПОСАДОК считай через воронку:
- лиды: COUNT(*)
- предквалы: COUNT(*) FILTER (WHERE pre_qual_date IS NOT NULL)
- квалы: COUNT(*) FILTER (WHERE qualification_date_krym IS NOT NULL OR qualification_date_sochi IS NOT NULL OR qualification_date_anapa IS NOT NULL OR qualification_date_baku IS NOT NULL)
- показы: COUNT(*) FILTER (WHERE pokaz_proveden IS NOT NULL OR pokaz_proveden_date IS NOT NULL)
- брони: COUNT(*) FILTER (WHERE objekt_zabronirovan IS NOT NULL OR data_oplaty_broni_1 IS NOT NULL)
- конверсия лид→квал: ROUND(квалы * 100.0 / NULLIF(лиды, 0), 1) || '%'

СТИЛЬ:
- Коротко и по делу
- После выгрузки — сразу анализ топ-5 и рекомендации
- Если вопрос аналитический без выгрузки — отвечай текстом

КОГДА УТОЧНЯТЬ:
- Не указан регион → спроси один раз
- Остальное додумывай сам (год=2026, период=текущий месяц если не указан)

{DB_SCHEMA_HINT}"""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ] + chat_history + [{"role": "user", "content": user_question}]

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=2000,
        messages=messages,
    )
    text = (response.choices[0].message.content or "").strip()

    # Сначала — явное начало с SQL / fenced block
    text_upper = text.lstrip().upper()
    looks_like_sql = text_upper.startswith(
        (
            "SELECT",
            "WITH",
            "```SQL",
            "``` SELECT",
            "``` WITH",
            "```\nSELECT",
            "```\nWITH",
        )
    )

    if looks_like_sql:
        try:
            normalized = _normalize_ai_sql(text)
            return normalized, True
        except ValueError:
            looks_like_sql = False

    # GPT часто пишет вводную фразу, затем SQL — извлекаем первый SELECT/WITH
    if not looks_like_sql:
        try:
            normalized = _normalize_ai_sql(text)
            return normalized, True
        except ValueError:
            pass

    return text, False


def _run_ai_sql(sql: str) -> pd.DataFrame:
    """Выполняет AI-генерированный SQL через SQLAlchemy (text() корректно обрабатывает % в ILIKE/LIKE)."""
    from sqlalchemy import create_engine, text as sa_text
    url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not url:
        raise ValueError("Нет подключения к базе. Укажи SUPABASE_DB_URL.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if ".pooler.supabase.com:6543/" in url:
        url = url.replace(".pooler.supabase.com:6543/", ".pooler.supabase.com:5432/")
    sql_str = str(sql).strip()
    sql_str = re.sub(r"^```(?:sql)?\n?", "", sql_str, flags=re.IGNORECASE)
    sql_str = re.sub(r"\n?```$", "", sql_str)
    sql_str = sql_str.strip()
    sql_str = _normalize_ai_sql(sql_str)
    sa_engine = create_engine(
        url,
        connect_args={
            "connect_timeout": 15,
            "sslmode": "require",
            "options": "-c timezone=Europe/Moscow -c statement_timeout=60000",
        },
        pool_pre_ping=False,
        pool_size=1,
        max_overflow=0,
    )
    try:
        with sa_engine.connect() as conn:
            df = pd.read_sql(sa_text(sql_str), conn)
    finally:
        sa_engine.dispose()
    return df


def _build_ai_result_context(df: pd.DataFrame) -> str:
    row_count = len(df)
    columns = ", ".join(map(str, df.columns.tolist()[:50]))
    preview_csv = df.head(20).to_csv(index=False)
    if len(preview_csv) > 12000:
        preview_csv = preview_csv[:12000]

    numeric_summary = ""
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()[:8]
    if numeric_cols:
        try:
            numeric_summary = df[numeric_cols].describe().round(2).to_string()
        except Exception:
            numeric_summary = ""

    # Нельзя использовать \n внутри {...} в f-string (SyntaxError в Python < 3.12)
    summary_block = ""
    if numeric_summary:
        summary_block = "Сводка по числовым колонкам:\n" + numeric_summary

    return (
        f"Строк: {row_count}\n"
        f"Колонки: {columns}\n\n"
        f"Первые строки (CSV):\n{preview_csv}\n"
        f"{summary_block}"
    )


def ask_gpt_for_result_analysis(client, user_question: str, sql: str, df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Запрос выполнен, но по этим условиям данные не найдены."

    analysis_prompt = """Ты — аналитик данных Эстадель, встроенный в дашборд.

SQL уже выполнен автоматически, и результат уже у тебя перед глазами.
Никогда не проси пользователя выполнять запрос самому, присылать выгрузку или сообщать результаты.

Отвечай по-русски, коротко и по делу.
Без вводных «сейчас проанализирую», «подождите» — сразу суть.
Если это скорее выгрузка — скажи, что выгрузка готова, и добавь 1-3 коротких наблюдения.
Если это аналитический вопрос — дай краткий вывод, top-5 наблюдений и что улучшить.
Если данных мало или вывод ненадёжен — прямо скажи об ограничении.
Не вставляй SQL в ответ.
"""
    result_context = _build_ai_result_context(df)
    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1200,
        messages=[
            {"role": "system", "content": analysis_prompt},
            {
                "role": "user",
                "content": (
                    f"Вопрос пользователя:\n{user_question}\n\n"
                    f"SQL, который был выполнен:\n{sql}\n\n"
                    f"Результат запроса:\n{result_context}\n\n"
                    "Сразу дай выводы и практические рекомендации."
                ),
            },
        ],
    )
    return (response.choices[0].message.content or "").strip()


def _ai_chat_message(role: str):
    """Без avatar: в Streamlit avatar= только URL/файл изображения; emoji и часть путей дают StreamlitAPIException."""
    return st.chat_message(role)


def _render_ai_analyst_tab():
    if not os.environ.get("SUPABASE_DB_URL", "").strip():
        st.error("Нет подключения к базе. Укажи `SUPABASE_DB_URL`.")
        return

    if "ai_chat_history" not in st.session_state:
        st.session_state.ai_chat_history = []
    if "ai_last_sql" not in st.session_state:
        st.session_state.ai_last_sql = ""
    if "ai_last_result_df" not in st.session_state:
        st.session_state.ai_last_result_df = None
    if "ai_last_error" not in st.session_state:
        st.session_state.ai_last_error = ""

    openai_client = get_openai_client()
    if not openai_client:
        st.error("OPENAI_API_KEY не задан или пакет `openai` не установлен.")
        st.info("Добавь `OPENAI_API_KEY` в локальный `.env` и перезапусти приложение.")
        return

    st.markdown(
        """
<style>
/* Фон чата */
.stChatMessage {
    border-radius: 16px;
    margin-bottom: 12px;
    padding: 4px 8px;
}
/* Сообщение пользователя */
[data-testid="stChatMessageContent"] {
    font-size: 15px;
    line-height: 1.6;
}
/* Таблица результатов */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
    margin-top: 8px;
}
/* Кнопка скачать */
.stDownloadButton button {
    border-radius: 8px;
    font-size: 13px;
    padding: 4px 12px;
    background: transparent;
    border: 1px solid rgba(255,255,255,0.2);
    color: rgba(255,255,255,0.7);
}
.stDownloadButton button:hover {
    border-color: rgba(255,255,255,0.5);
    color: white;
}
/* Expander SQL */
.streamlit-expanderHeader {
    font-size: 12px;
    color: rgba(255,255,255,0.4);
    border-radius: 8px;
}
/* Кнопка очистить */
div[data-testid="column"]:last-child .stButton button {
    background: transparent;
    border: 1px solid rgba(255,255,255,0.15);
    color: rgba(255,255,255,0.5);
    border-radius: 8px;
    font-size: 13px;
}
div[data-testid="column"]:last-child .stButton button:hover {
    border-color: rgba(255,255,255,0.3);
    color: rgba(255,255,255,0.8);
}
</style>
""",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([8, 1])
    with col1:
        st.markdown("### ИИ-аналитик")
        st.markdown(
            "<span style='color:rgba(255,255,255,0.4);font-size:13px'>Спроси про данные или попроси выгрузку</span>",
            unsafe_allow_html=True,
        )
    with col2:
        if st.session_state.ai_chat_history:
            if st.button("Очистить", use_container_width=True):
                st.session_state.ai_chat_history = []
                st.session_state.ai_last_sql = ""
                st.session_state.ai_last_result_df = None
                st.session_state.ai_last_error = ""
                st.rerun()

    history = st.session_state.ai_chat_history
    if not history:
        st.markdown(
            """
    <div style='text-align:center;padding:60px 20px;color:rgba(255,255,255,0.3)'>
        <div style='font-size:32px;margin-bottom:16px'>✦</div>
        <div style='font-size:16px;margin-bottom:8px'>Спроси про данные</div>
        <div style='font-size:13px'>Например: какие посадки самые эффективные в феврале по Крыму?</div>
    </div>
    """,
            unsafe_allow_html=True,
        )

    last_sql_idx = max(
        [idx for idx, msg in enumerate(history) if msg["role"] == "assistant" and msg.get("is_sql")],
        default=-1,
    )
    for idx, msg in enumerate(history):
        if msg["role"] == "user":
            with _ai_chat_message("user"):
                st.write(msg["content"])
        else:
            with _ai_chat_message("assistant"):
                if msg.get("is_sql"):
                    with st.expander("Показать SQL запрос", expanded=False):
                        st.code(msg["content"], language="sql")
                    if idx == last_sql_idx and isinstance(st.session_state.ai_last_result_df, pd.DataFrame):
                        df = st.session_state.ai_last_result_df
                        if df is not None and not df.empty:
                            st.markdown(
                                f"<span style='color:rgba(255,255,255,0.5);font-size:12px'>Найдено строк: {len(df)} · {len(df.columns)} колонок</span>",
                                unsafe_allow_html=True,
                            )
                            _h = min(400, 50 + len(df) * 35)
                            st.dataframe(df, use_container_width=True, height=_h)
                            csv = df.to_csv(index=False).encode("utf-8-sig")
                            st.download_button(
                                "↓ Скачать CSV",
                                csv,
                                f"export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                "text/csv",
                                use_container_width=False,
                            )
                        else:
                            st.markdown(
                                "<span style='color:rgba(255,255,255,0.5);font-size:12px'>Данных по запросу нет</span>",
                                unsafe_allow_html=True,
                            )
                else:
                    st.write(msg["content"])

    user_input = st.chat_input("Например: выгрузи все квалы за 17 марта со всеми UTM и телефонами")
    if user_input:
        st.session_state.ai_last_error = ""
        with _ai_chat_message("user"):
            st.write(user_input)
        try:
            with st.spinner("GPT думает…"):
                response_text, is_sql = ask_gpt_for_sql(openai_client, user_input, st.session_state.ai_chat_history)
            st.session_state.ai_chat_history.append({"role": "user", "content": user_input})
            st.session_state.ai_chat_history.append({"role": "assistant", "content": response_text, "is_sql": is_sql})
            if is_sql:
                st.session_state.ai_last_sql = response_text
                with st.spinner("Выполняю запрос…"):
                    df = _run_ai_sql(response_text)
                    st.session_state.ai_last_result_df = df
                try:
                    with st.spinner("Анализирую результат…"):
                        analysis_text = ask_gpt_for_result_analysis(openai_client, user_input, response_text, df)
                except Exception:
                    if df is None or df.empty:
                        analysis_text = "Запрос выполнен, но данные не найдены."
                    else:
                        analysis_text = f"Запрос выполнен. Найдено строк: {len(df)}."
                st.session_state.ai_chat_history.append(
                    {"role": "assistant", "content": analysis_text, "is_sql": False}
                )
            else:
                st.session_state.ai_last_result_df = None
        except Exception as e:
            st.session_state.ai_last_result_df = None
            err = str(e)
            err_low = err.lower()
            if "429" in err_low or "insufficient_quota" in err_low:
                st.session_state.ai_last_error = (
                    "OpenAI вернул 429 (insufficient_quota). "
                    "Проверь тариф/лимиты и пополнение баланса в OpenAI, затем повтори."
                )
            else:
                st.session_state.ai_last_error = err
        st.rerun()

    if st.session_state.ai_last_error:
        st.error(f"Ошибка выполнения SQL: {st.session_state.ai_last_error}")
        st.info("Попробуй переформулировать запрос.")


def _db_error_message(e):
    """Сообщение для пользователя при ошибке подключения к БД."""
    err = str(e).lower()
    if "circuit breaker" in err or "too many authentication errors" in err:
        return (
            "**Supabase временно ограничил подключения** (слишком много неудачных попыток). "
            "Подождите **10–15 минут**, не обновляйте страницу и не меняйте даты. Потом обновите страницу. "
            "Если повторяется — проверьте пароль в настройках приложения (Secrets): пользователь должен быть `postgres.…`, порт 5432 (session mode)."
        )
    return str(e)


@st.cache_data(ttl=7200)
def _fetch_google_sheet(gid, raw=False, range_a1=None):
    """Загрузка листа из Google Sheets по gid. Кэш 2 ч. raw=True — без заголовков.
    range_a1 — опционально, напр. 'B4:CS37' (тогда используется gviz/tq).
    До 2 попыток с коротким таймаутом. При ошибке после повторов — raise (не кэшируется)."""
    if not gid or not str(gid).strip():
        return None
    import urllib.request
    import urllib.error
    import time
    gid_s = str(gid).strip()
    if range_a1:
        import urllib.parse
        url = GOOGLE_SHEET_GVIZ.format(gid=gid_s, range=urllib.parse.quote(range_a1))
    else:
        url = f"{GOOGLE_SHEET_BASE}{gid_s}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    last_err = None
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as resp:
                raw_bytes = resp.read()
            if raw_bytes[:100].strip().lower().startswith(b"<!") or b"<html" in raw_bytes[:500].lower():
                return None  # HTML вместо CSV — лист недоступен (нужен доступ по ссылке)
            from io import BytesIO
            df = pd.read_csv(BytesIO(raw_bytes), encoding="utf-8", header=None if raw else 0, on_bad_lines="skip")
            if df.empty:
                return None
            return df
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            last_err = e
            if attempt < 1:
                time.sleep(0.8)
            continue
        except Exception as e:
            last_err = e
            break
    raise RuntimeError(f"Не удалось загрузить лист (gid={gid}): {last_err}")


def _fetch_sheet_debug(gid):
    """Тестовый запрос без кэша. Возвращает (ok, status_code, msg)."""
    import urllib.request
    import urllib.error
    if not gid or not str(gid).strip():
        return False, None, "gid пустой"
    url = f"{GOOGLE_SHEET_BASE}{str(gid).strip()}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; Dashboard/1.0)"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            from io import BytesIO
            df = pd.read_csv(BytesIO(raw), encoding="utf-8", header=None, on_bad_lines="skip", nrows=20)
            rows = len(df)
            return rows > 0, resp.status, f"OK, получено ~{rows} строк" if rows else "Лист пустой (0 строк)"
    except urllib.error.HTTPError as e:
        return False, e.code, f"HTTP {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return False, None, f"Сеть: {e.reason}"
    except Exception as e:
        return False, None, str(e)


def _sheet_export_url(gid):
    """URL для проверки экспорта листа вручную."""
    return f"{GOOGLE_SHEET_BASE}{str(gid).strip()}"


def _to_num(val):
    """Парсинг числа из ячейки: '184 643' -> 184643, '14,6' -> 14.6."""
    if pd.isna(val) or val == "" or val is None:
        return None
    s = str(val).strip().replace(" ", "").replace(",", ".").replace("\u00a0", "")
    if not s or s in ("%", "-", "–"):
        return None
    if "%" in s:
        s = s.replace("%", "")
    try:
        return float(s) if "." in s else int(float(s))
    except (ValueError, TypeError):
        return None


def _fmt_num_display(val):
    """Форматирование числа для отображения: до десятых, запятая, пробел тысяч."""
    if val is None or pd.isna(val):
        return ""
    try:
        x = float(val)
        r = round(x, 1)
        if abs(x) >= 1000:
            return f"{int(r):,}".replace(",", " ")
        if abs(r - int(r)) < 0.001:
            return str(int(r))
        return f"{r:.1f}".replace(".", ",")
    except (ValueError, TypeError):
        return str(val) if val != "" else ""


def _fmt_pct_display(val):
    """Форматирование процента: 9.2 -> '9,2%', 126 -> '126%'."""
    if val is None or pd.isna(val):
        return ""
    try:
        x = float(val)
        r = round(x, 1)
        if abs(r - int(r)) < 0.001:
            return f"{int(r)}%"
        return f"{r:.1f}%".replace(".", ",")
    except (ValueError, TypeError):
        return str(val) if val != "" else ""


def _fmt_price_display(val):
    """Форматирование цены: число + ' р.'"""
    s = _fmt_num_display(val)
    return f"{s} р." if s else ""


def _parse_forma_om(df_raw, block_name=""):
    """
    Парсит ФОРМА ОМ → (html_table, chart_df, n_rows).
    Для Монолит — другая структура: Лиды, Живой, Целевой Итого/Статика/Динамика, Бюджет, Реалиация.
    """
    if df_raw is None or df_raw.empty or len(df_raw) < 5:
        return None, None, 0

    # Название региона из строки 3 листа (csv index 2)
    try:
        r2 = df_raw.iloc[2]
        region_name = str(r2.iloc[0]).strip()
        region_sub  = str(r2.iloc[1]).strip() if df_raw.shape[1] > 1 else ""
        if region_sub in ("nan", ""):
            region_sub = ""
    except Exception:
        region_name, region_sub = "", ""

    # Поиск начала данных (строка с датой в колонке 0)
    data_start = 4
    for i in range(3, min(8, len(df_raw))):
        try:
            v = str(df_raw.iloc[i].iloc[0]).strip()
            if v and re.match(r"^\d{1,2}\s+\w+", v):
                data_start = i
                break
        except Exception:
            pass
    df_data = df_raw.iloc[data_start:].copy()
    n_cols = len(df_data.columns)
    df_data.columns = range(n_cols)
    mask = df_data[0].astype(str).str.match(r"^\d{1,2}\s+\w+", na=False)
    df_data = df_data[mask].copy()
    if df_data.empty:
        return None, None, 0

    is_monolit = "монолит" in (block_name or "").lower()
    if is_monolit:
        n_cols = min(n_cols, 33)
        cats = [
            ("Лиды", [
                ("День<br>План", 1, "plan", None),
                ("День<br>Факт", 2, "fact", 1),
                ("Всего<br>План", 3, "plan", None),
                ("Всего<br>Факт", 4, "fact", 3),
            ]),
            ("Живой", [
                ("День<br>План", 5, "plan", None),
                ("День<br>Факт", 6, "fact", 5),
                ("Всего<br>План", 7, "plan", None),
                ("Всего<br>Факт", 8, "fact", 7),
            ]),
            ("Целевой Итого", [
                ("%<br>Квала", 9, "pct", None),
                ("День<br>План", 10, "plan", None),
                ("День<br>Факт", 11, "fact", 10),
                ("Всего<br>План", 12, "plan", None),
                ("Всего<br>Факт", 13, "fact", 12),
            ]),
            ("Целевой Статика", [
                ("%<br>Квала", 14, "pct", None),
                ("День<br>План", 15, "plan", None),
                ("День<br>Факт", 16, "fact", 15),
                ("Всего<br>План", 17, "plan", None),
                ("Всего<br>Факт", 18, "fact", 17),
            ]),
            ("Целевой Динамика", [
                ("%<br>Дин", 19, "pct", None),
                ("День<br>План", 20, "plan", None),
                ("День<br>Факт", 21, "fact", 20),
                ("Всего<br>План", 22, "plan", None),
                ("Всего<br>Факт", 23, "fact", 22),
            ]),
            ("Бюджет", [
                ("День<br>План", 24, "plan", None),
                ("День<br>Факт", 25, "fact", 24),
                ("Всего<br>План", 26, "plan", None),
                ("Всего<br>Факт", 27, "fact", 26),
            ]),
            ("Реалиация", [
                ("Лиды", 28, "real", None),
                ("Квалы<br>Итого", 29, "real", None),
                ("Квалы<br>Стат", 30, "real", None),
                ("Квалы<br>Дин", 31, "real", None),
                ("Бюджет", 32, "real", None),
            ]),
        ]
        kp, kf, ktp, ktf = 10, 11, 12, 13  # Целевой Итого: День и Всего
    else:
        n_cols = min(n_cols, 29)
        cats = [
        ("Лиды", [
            ("День<br>План",  1, "plan", None),
            ("День<br>Факт",  2, "fact", 1),
            ("Всего<br>План", 3, "plan", None),
            ("Всего<br>Факт", 4, "fact", 3),
        ]),
        ("Квалы Итого", [
            ("День<br>План",  5, "plan", None),
            ("День<br>Факт",  6, "fact", 5),
            ("Всего<br>План", 7, "plan", None),
            ("Всего<br>Факт", 8, "fact", 7),
            ("%<br>Квала",    9, "pct",  None),
        ]),
        ("Квалы Статика", [
            ("День<br>План",  10, "plan", None),
            ("День<br>Факт",  11, "fact", 10),
            ("Всего<br>План", 12, "plan", None),
            ("Всего<br>Факт", 13, "fact", 12),
            ("%<br>Стат",     14, "pct",  None),
        ]),
        ("Квалы Динамика", [
            ("День<br>План",  15, "plan", None),
            ("День<br>Факт",  16, "fact", 15),
            ("Всего<br>План", 17, "plan", None),
            ("Всего<br>Факт", 18, "fact", 17),
            ("%<br>Дин",      19, "pct",  None),
        ]),
        ("Бюджет", [
            ("День<br>План",  20, "plan", None),
            ("День<br>Факт",  21, "fact", 20),
            ("Всего<br>План", 22, "plan", None),
            ("Всего<br>Факт", 23, "fact", 22),
        ]),
        ("Реализация", [
            ("Лиды",          24, "real", None),
            ("Квалы<br>Итого",25, "real", None),
            ("Квалы<br>Стат", 26, "real", None),
            ("Квалы<br>Дин",  27, "real", None),
            ("Бюджет",        28, "real", None),
        ]),
    ]
        kp, kf, ktp, ktf = 5, 6, 7, 8  # для графика
    total_cols = 1 + sum(len(s) for _, s in cats)

    p = []  # html parts

    p.append("""<style>
.omtbl{border-collapse:separate;border-spacing:0;font-family:'Inter',system-ui,sans-serif;font-size:0.82rem;width:100%;min-width:max-content;color:#F1F5F9;}
.omtbl th{padding:5px 7px;border:1px solid #334155;text-align:center;font-size:0.76rem;white-space:nowrap;line-height:1.2;}
.omtbl th.cat{background:#0F172A;color:#3B82F6;font-weight:700;font-size:0.8rem;border-bottom:2px solid #334155;padding:6px 10px;}
.omtbl th.sub{background:#1E293B;color:#64748B;font-weight:500;}
.omtbl th.sub-pl{background:#1E293B;color:#94A3B8;}
.omtbl td{padding:5px 7px;border:1px solid #1E293B;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;font-size:0.82rem;}
.omtbl td.dt{background:#1E293B;text-align:left;color:#CBD5E1;padding:5px 9px;}
.omtbl td.wk{color:#F9A8D4!important;}
.omtbl td.pl{background:#475569;color:#E2E8F0;}
.omtbl td.gn{background:rgba(34,197,94,0.45);color:#F1F5F9;font-weight:600;}
.omtbl td.rd{background:rgba(239,68,68,0.45);color:#F1F5F9;font-weight:600;}
.omtbl td.pc{background:#1E3A5F;color:#93C5FD;}
.omtbl td.rl{background:#172032;color:#94A3B8;}
.omtbl td.em{background:#0F172A;color:#334155;}
.omtbl tbody tr:hover td{filter:brightness(1.18);}
.omtbl thead{background:#0F172A!important;}
.omtbl thead tr{background:#0F172A!important;}
.omtbl thead th{position:sticky;top:0;z-index:2;background:#0F172A!important;background-color:#0F172A!important;box-shadow:0 1px 0 #334155,0 0 0 9999px transparent;}
.omtbl thead tr:nth-child(1) th{top:0;height:34px;}
.omtbl thead tr:nth-child(2) th{top:34px;height:34px;background:#1E293B!important;background-color:#1E293B!important;}
.omtbl thead tr:nth-child(3) th{top:68px;height:48px;background:#1E293B!important;background-color:#1E293B!important;}
.omtbl th.cat{background:#0F172A!important;background-color:#0F172A!important;}
.omtbl th.sub, .omtbl th.sub-pl{background:#1E293B!important;background-color:#1E293B!important;}
.omtbl td.dt, .omtbl th:first-child{position:sticky;left:0;z-index:3;background:#1E293B!important;background-color:#1E293B!important;box-shadow:2px 0 0 #334155,6px 0 12px #0B1120;}
.omtbl thead th:first-child{z-index:5;}
.omtbl th.cat-sep{border-right:3px solid #475569!important;}
.omtbl td.cat-sep{border-right:3px solid #334155!important;}
</style>""")

    p.append('<div style="overflow-x:auto;overflow-y:auto;max-height:720px;border-radius:8px;border:1px solid #334155;-webkit-overflow-scrolling:touch;background:#0B1120;">')
    p.append('<table class="omtbl">')
    p.append('<thead>')

    # Строка с названием региона (sticky, чтобы при скролле оставалась сверху)
    if region_name:
        sub_txt = f' &nbsp;<span style="color:#64748B;font-weight:400;font-size:0.88rem;">{region_sub}</span>' if region_sub else ''
        p.append(f'<tr><td colspan="{total_cols}" style="position:sticky;top:0;z-index:6;background:#0F172A!important;background-color:#0F172A!important;color:#3B82F6;font-weight:700;font-size:1rem;padding:8px 12px;border-bottom:1px solid #334155;box-shadow:0 2px 0 #334155,0 6px 12px #0B1120;">{region_name}{sub_txt}</td></tr>')

    # Строка категорий (colspan), границы между секциями
    p.append('<tr>')
    p.append('<th class="cat" rowspan="2" style="min-width:82px;">Дата</th>')
    for i, (cat_label, subs) in enumerate(cats):
        sep = " cat-sep" if i < len(cats) - 1 else ""
        p.append(f'<th class="cat{sep}" colspan="{len(subs)}">{cat_label}</th>')
    p.append('</tr>')

    # Строка подзаголовков
    p.append('<tr>')
    for i, (_, subs) in enumerate(cats):
        for j, (sub_lbl, _, col_type, _) in enumerate(subs):
            cls = "sub-pl" if col_type == "plan" else "sub"
            sep = " cat-sep" if (i < len(cats) - 1 and j == len(subs) - 1) else ""
            p.append(f'<th class="{cls}{sep}">{sub_lbl}</th>')
    p.append('</tr>')
    p.append('</thead><tbody>')

    # Строки данных
    for _, row in df_data.iterrows():
        date_val = str(row[0]).strip()
        is_wk = False
        try:
            d = int("".join(c for c in date_val.split()[0] if c.isdigit()) or "0")
            if "феврал" in date_val.lower() and d > 0:
                is_wk = datetime(2026, 2, min(d, 28)).weekday() >= 5
        except Exception:
            pass

        p.append('<tr>')
        wk_cls = " wk" if is_wk else ""
        p.append(f'<td class="dt{wk_cls}">{date_val}</td>')

        for ci, (cat_label, subs) in enumerate(cats):
            for sj, (_, col_idx, col_type, plan_idx) in enumerate(subs):
                val = _to_num(row.get(col_idx))
                sep = " cat-sep" if (ci < len(cats) - 1 and sj == len(subs) - 1) else ""
                if col_type == "plan":
                    p.append(f'<td class="pl{sep}">{_fmt_num_display(val)}</td>')
                elif col_type == "fact":
                    plan_val = _to_num(row.get(plan_idx)) if plan_idx is not None else None
                    if val is not None and plan_val is not None:
                        # Бюджет: выше плана — красный (перерасход), ниже — зелёный (экономия)
                        if "бюджет" in (cat_label or "").lower():
                            css = "rd" if val > plan_val else ("gn" if val < plan_val else "em")
                        else:
                            css = "gn" if val >= plan_val else "rd"
                    else:
                        css = "em"
                    p.append(f'<td class="{css}{sep}">{_fmt_num_display(val)}</td>')
                elif col_type == "pct":
                    p.append(f'<td class="pc{sep}">{_fmt_pct_display(val)}</td>')
                else:
                    p.append(f'<td class="rl{sep}">{_fmt_pct_display(val)}</td>')
        p.append('</tr>')

    p.append('</tbody></table></div>')
    html = "".join(p)

    # Данные для графика (День и Всего)
    chart_rows = []
    for _, row in df_data.iterrows():
        chart_rows.append({
            "Дата":              str(row[0]).strip(),
            "Лиды план":         _to_num(row.get(1)),
            "Лиды факт":         _to_num(row.get(2)),
            "Квалы план":        _to_num(row.get(kp)),
            "Квалы факт":        _to_num(row.get(kf)),
            "Лиды всего план":   _to_num(row.get(3)),
            "Лиды всего факт":   _to_num(row.get(4)),
            "Квалы всего план":  _to_num(row.get(ktp)),
            "Квалы всего факт":  _to_num(row.get(ktf)),
        })
    chart_df = pd.DataFrame(chart_rows)

    return html, chart_df, len(df_data)


# Короткие названия категорий для отображения (как в Ист Крым/Баку), порядок — от длинных к коротким
_IST_CAT_DISPLAY_ORDER = (
    "Квалы Динамика", "Квалы Статика", "Квалы Итого", "Квалы динамика", "Квалы статика",
    "Реалиация", "Предквалы", "Бюджет", "Цена", "Лиды",
)


def _ist_cat_display(label):
    """Оставляем для заголовка только короткое название категории (Лиды, Квалы Итого, Бюджет и т.д.)."""
    if not label or not str(label).strip():
        return label or "—"
    s = str(label).strip()
    s_lower = s.lower()
    for cat in _IST_CAT_DISPLAY_ORDER:
        if cat.lower() in s_lower:
            return cat
    if "лид" in s_lower and "квал" not in s_lower:
        return "Лиды"
    if "квал" in s_lower and "итого" in s_lower:
        return "Квалы Итого"
    if "квал" in s_lower and "стат" in s_lower:
        return "Квалы Статика"
    if "квал" in s_lower and "дин" in s_lower:
        return "Квалы Динамика"
    if "бюджет" in s_lower:
        return "Бюджет"
    if "реализ" in s_lower or "реалиация" in s_lower:
        return "Реалиация"
    if "цена" in s_lower:
        return "Цена"
    return s


def _ist_sub_to_two_lines(sh):
    """Подзаголовок в 2 строки как в ОМ: День План, Квалы динамика, % квала и т.д."""
    s = sh.replace("\n", " ").replace("\r", " ").strip()
    # Нормализуем слитные подписи из таблицы (ДеньПлан → день план)
    s_lower_orig = s.lower()
    for fused, spaced in (
        ("деньплан", "день план"), ("деньфакт", "день факт"),
        ("всегоплан", "всего план"), ("всегофакт", "всего факт"),
    ):
        if fused in s_lower_orig:
            s_lower_orig = s_lower_orig.replace(fused, spaced)
    for known in (
        "день план", "день факт", "всего план", "всего факт",
        "% квала", "% стат квала", "% лида",
        "лиды", "квалы итого", "квалы статика", "квалы динамика",
        "бюджет", "лида", "квала", "стат квала",
    ):
        if known in s_lower_orig:
            # Берём фрагмент, содержащий known (для многословных — целиком)
            idx = s_lower_orig.find(known)
            if known in ("день план", "день факт", "всего план", "всего факт"):
                s = s[idx : idx + len(known)].strip() or s
            elif known in ("% квала", "% стат квала", "% лида"):
                s = s[idx : idx + len(known)].strip() or s
            else:
                s = s[idx : idx + len(known)].strip() or s
            break
    s_lower = s.lower()
    if re.match(r"^день\s+план$", s_lower):
        return "День<br>План"
    if re.match(r"^день\s+факт$", s_lower):
        return "День<br>Факт"
    if re.match(r"^всего\s+план$", s_lower):
        return "Всего<br>План"
    if re.match(r"^всего\s+факт$", s_lower):
        return "Всего<br>Факт"
    if re.match(r"^%?\s*квала\s*(итого)?$", s_lower) or re.match(r"^%?\s*квалы?\s*итого$", s_lower) or re.match(r"^квалы?\s+итого$", s_lower):
        return "%<br>Квала" if "%" in s else "Квалы<br>Итого"
    if re.match(r"^%?\s*стат\s*квала$", s_lower) or re.match(r"^%\s*стат", s_lower):
        return "% Стат<br>Квала"
    if re.match(r"^%?\s*квала\s*стат", s_lower) or re.match(r"^квалы?\s+стат", s_lower):
        return "Квалы<br>Статика"
    if re.match(r"^%?\s*квала\s*дин", s_lower) or re.match(r"^квалы?\s+дин", s_lower):
        return "Квалы<br>Динамика"
    if re.match(r"^%?\s*лида$", s_lower):
        return "%<br>Лида"
    if re.match(r"^лиды?$", s_lower):
        return "Лиды"
    if re.match(r"^день$", s_lower):
        return "День"
    return s.replace("\n", "<br>").replace("\r", "")



def _parse_forma_ist(df_raw, block_name=""):
    """
    Парсит ФОРМА ИСТОЧНИКИ — таблицы с 3 уровнями заголовков.
    Ист Монолит: другая раскладка — ищем строку с «Дата», sources/categories/sub выше неё.
    """
    if df_raw is None or df_raw.empty or len(df_raw) < 5:
        return None, None, 0

    n_cols = df_raw.shape[1]

    def _val(r, c):
        try:
            v = r.iloc[c]
            # всегда скаляр (избегаем "truth value of a DataFrame/Series is ambiguous")
            while isinstance(v, (pd.DataFrame, pd.Series)):
                v = v.iloc[0] if len(v) else ""
            s = str(v).strip() if pd.notna(v) else ""
            return s.replace("\n", " ").replace('"', "").strip() if s and s.lower() != "nan" else ""
        except Exception:
            return ""

    def _forward_fill(row):
        res, prev = [], ""
        for c in range(len(row)):
            v = _val(row, c)
            if v:
                prev = v
            res.append(prev if prev else f"Кол.{c+1}")
        return res

    # Поиск строки с «Дата» (подзаголовки)
    sub_row = 2
    for i in range(min(8, len(df_raw))):
        v = _val(df_raw.iloc[i], 0)
        vl = v.lower().strip()
        if vl == "дата" or vl.startswith("дата ") or " дата" in vl or vl.endswith(" дата"):
            sub_row = i
            break

    _src_hint = ("yandex", "яндекс", "директ", "direct", "другое", "vk", "вк", "telegram", "tg", "inst", "instagram", "discovery", "botto", "гцк", "моп", "авито")
    _region_in_block = {"баку": "баку", "крым": "крым", "сочи": "сочи", "анапа": "анапа", "монолит": "монолит"}

    # Реальная структура листов Источники:
    # row 0 -> крупные блоки (Лиды / Квалы / ...)
    # row 1 -> "Источник" + тех. повтор названий каналов
    # row 3 -> регион + фактические названия источников
    # row 4 -> категории
    # row 5 -> подзаголовки (Дата, ДеньПлан, ДеньФакт, ...)
    cat_row = max(0, sub_row - 1)
    src_row = max(0, sub_row - 2)
    block_l = (block_name or "").lower()
    block_region = next((region_k for block_k, region_k in _region_in_block.items() if block_k in block_l), "")

    # Предпочитаем строку вида "Баку | Инст..." / "Крым | Yandex..." / ...
    for i in range(max(0, sub_row - 1), -1, -1):
        v0 = _val(df_raw.iloc[i], 0).lower().strip()
        v1 = _val(df_raw.iloc[i], 1) if n_cols > 1 else ""
        if block_region and v0 == block_region and v1.strip():
            src_row = i
            break
    else:
        for i in range(max(0, sub_row - 1), -1, -1):
            v0 = _val(df_raw.iloc[i], 0).lower().strip()
            v1 = _val(df_raw.iloc[i], 1) if n_cols > 1 else ""
            if v1.strip() and v0 not in ("", "источник", "показатель"):
                src_row = i
                break

    r0 = df_raw.iloc[src_row]
    r1 = df_raw.iloc[cat_row]
    r2 = df_raw.iloc[sub_row]

    sources = _forward_fill(r0)
    categories = _forward_fill(r1)
    subheaders = []
    for c in range(n_cols):
        v = _val(r2, c)
        subheaders.append(v if v else _val(r1, c) if _val(r1, c) else str(c + 1))

    # Группировка столбцов: (источник, категория) → [(подзаголовок, col_idx)]
    # Столбец 0 — дата, исключаем из блоков источников (мог быть "Ист Крым" и т.п.)
    from collections import OrderedDict
    groups = OrderedDict()
    for c in range(1, n_cols):
        src = sources[c] or f"Кол.{c+1}"
        cat = categories[c] or "—"
        sub = subheaders[c] or str(c)
        key = (src, cat)
        if key not in groups:
            groups[key] = []
        groups[(src, cat)].append((sub, c))

    # Построение структуры: источник → [(категория, ...)]
    # Пропускаем скрытые/пустые столбцы (источник = placeholder "Кол.N")
    src_blocks = OrderedDict()
    for (src, cat), cols in groups.items():
        if re.match(r"^Кол\.\d+$", src):
            continue
        if src not in src_blocks:
            src_blocks[src] = []
        subs_list = [(_ist_sub_to_two_lines(sh), idx) for sh, idx in cols]
        # Определяем тип по подзаголовку; plan/fact парятся внутри категории
        typed = []
        last_plan_idx = None
        cat_l = (cat or "").lower()
        for sh, idx in subs_list:
            sh_l = sh.lower().replace("<br>", " ")
            if "план" in sh_l and "факт" not in sh_l:
                last_plan_idx = idx
                typed.append((sh, idx, "plan", None))
            elif "факт" in sh_l:
                typed.append((sh, idx, "fact", last_plan_idx))
            elif "цена" in sh_l or "цена" in cat_l or "руб" in sh_l:
                typed.append((sh, idx, "price", None))
            elif "%" in sh or "квала" in sh_l or "стат" in sh_l or "дин" in sh_l:
                typed.append((sh, idx, "pct", None))
            else:
                typed.append((sh, idx, "real", None))
        src_blocks[src].append((cat, typed))

    # Фильтр: в Баку — только Инст и Яндекс; в Монолит — все; в остальных — только Яндекс
    def _src_allowed(name):
        n = name.lower()
        yandex = "yandex" in n or "яндекс" in n
        inst = "инст" in n or "inst" in n
        botto = "botto" in n
        direct = "директ" in n or "direct" in n
        if "баку" in (block_name or "").lower():
            return yandex or inst
        if "монолит" in (block_name or "").lower():
            return True
        if "крым" in (block_name or "").lower():
            return yandex or botto
        if "сочи" in (block_name or "").lower():
            gck = "гцк" in n or "моп" in n
            return yandex or direct or botto or gck
        return yandex

    src_blocks = OrderedDict((k, v) for k, v in src_blocks.items() if _src_allowed(k))
    if not src_blocks:
        return None, None, 0

    # Данные: первая строка с датой — после sub_row
    data_start = None
    for i in range(sub_row + 1, min(sub_row + 10, len(df_raw))):
        v = str(df_raw.iloc[i].iloc[0]).strip()
        if v and re.match(r"^\d{1,2}\s+\w+", v):
            data_start = i
            break
    if data_start is None:
        for i in range(4, min(10, len(df_raw))):
            v = str(df_raw.iloc[i].iloc[0]).strip()
            if v and re.match(r"^\d{1,2}\s+\w+", v):
                data_start = i
                break
    if data_start is None:
        data_start = sub_row + 1
    df_data = df_raw.iloc[data_start:].copy()
    df_data.columns = range(n_cols)
    mask = df_data[0].astype(str).str.match(r"^\d{1,2}\s+\w+", na=False)
    df_data = df_data[mask].copy()
    if df_data.empty:
        return None, None, 0

    # HTML — структура как ОМ (категории + подзаголовки), сверху ряд «Источник»
    p = []
    p.append("""<style>
.isttbl{border-collapse:separate;border-spacing:0;font-family:'Inter',system-ui,sans-serif;font-size:0.82rem;width:100%;min-width:max-content;color:#F1F5F9;}
.isttbl th{min-width:58px;padding:5px 7px;border:1px solid #334155;text-align:center;font-size:0.76rem;white-space:nowrap;line-height:1.2;}
.isttbl th.src{background:#0F172A;color:#3B82F6;font-weight:700;font-size:0.9rem;border-bottom:2px solid #334155;padding:6px 10px;}
.isttbl th.cat{background:#0F172A;color:#3B82F6;font-weight:700;font-size:0.8rem;border-bottom:2px solid #334155;padding:6px 10px;}
.isttbl th.sub{background:#1E293B;color:#64748B;font-weight:500;}
.isttbl th.sub-pl{background:#1E293B;color:#94A3B8;}
.isttbl td{min-width:58px;padding:5px 7px;border:1px solid #1E293B;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;font-size:0.82rem;}
.isttbl td.dt{background:#1E293B;text-align:left;color:#CBD5E1;padding:5px 9px;}
.isttbl td.wk{color:#F9A8D4!important;}
.isttbl td.pl{background:#475569;color:#E2E8F0;}
.isttbl td.gn{background:rgba(34,197,94,0.45);color:#F1F5F9;font-weight:600;}
.isttbl td.rd{background:rgba(239,68,68,0.45);color:#F1F5F9;font-weight:600;}
.isttbl td.pc{background:#1E3A5F;color:#93C5FD;}
.isttbl td.rl{background:#172032;color:#94A3B8;}
.isttbl td.gray{background:#1E293B;color:#94A3B8;}
.isttbl td.em{background:#0F172A;color:#334155;}
.isttbl tbody tr:hover td{filter:brightness(1.18);}
.isttbl thead{background:#0F172A!important;}
.isttbl thead tr{background:#0F172A!important;}
.isttbl thead th{position:sticky;background-clip:padding-box;box-shadow:0 1px 0 #334155,0 0 0 9999px transparent;}
.isttbl thead tr:nth-child(1) th{top:0;z-index:5;height:34px;background:#0F172A!important;}
.isttbl thead tr:nth-child(2) th{top:34px;z-index:4;height:34px;background:#162033!important;}
.isttbl thead tr:nth-child(3) th{top:68px;z-index:3;height:48px;background:#1E293B!important;}
.isttbl th.cat{background:#162033!important;}
.isttbl th.sub, .isttbl th.sub-pl{background:#1E293B!important;}
.isttbl td.dt, .isttbl th:first-child{position:sticky;left:0;background:#1E293B!important;box-shadow:2px 0 0 #334155,6px 0 12px #0B1120;}
.isttbl td.dt{z-index:1;}
.isttbl thead th:first-child{z-index:6;}
.isttbl th.cat-sep{border-right:3px solid #475569!important;}
.isttbl td.cat-sep{border-right:3px solid #334155!important;}
</style>""")

    p.append('<div style="overflow-x:auto;overflow-y:auto;max-height:720px;border-radius:8px;border:1px solid #334155;-webkit-overflow-scrolling:touch;background:#0B1120;">')
    p.append('<table class="isttbl">')
    p.append('<thead>')

    # Строка 1: источники (colspan по каждому источнику)
    p.append('<tr>')
    p.append('<th class="src" rowspan="3" style="min-width:82px;">Дата</th>')
    for src, blocks in src_blocks.items():
        span = sum(len(s[1]) for s in blocks)
        p.append(f'<th class="src" colspan="{span}">{src}</th>')
    p.append('</tr>')

    # Строка 2: категории (короткие названия как в Ист Крым/Баку), границы между секциями
    p.append('<tr>')
    all_cats = [(src, cat, subs) for src, blocks in src_blocks.items() for cat, subs in blocks]
    for i, (src, cat_label, subs) in enumerate(all_cats):
        sep = " cat-sep" if i < len(all_cats) - 1 else ""
        p.append(f'<th class="cat{sep}" colspan="{len(subs)}">{_ist_cat_display(cat_label)}</th>')
    p.append('</tr>')

    # Строка 3: подзаголовки
    p.append('<tr>')
    for i, (src, cat_label, subs) in enumerate(all_cats):
        for sj, (sub_lbl, col_idx, col_type, plan_idx) in enumerate(subs):
            cls = "sub-pl" if col_type == "plan" else "sub"
            sep = " cat-sep" if (i < len(all_cats) - 1 and sj == len(subs) - 1) else ""
            p.append(f'<th class="{cls}{sep}">{sub_lbl}</th>')
    p.append('</tr>')
    p.append('</thead><tbody>')

    # Строки данных
    for _, row in df_data.iterrows():
        date_val = str(row[0]).strip()
        is_wk = False
        try:
            d = int("".join(c for c in date_val.split()[0] if c.isdigit()) or "0")
            if "феврал" in date_val.lower() and d > 0:
                is_wk = datetime(2026, 2, min(d, 28)).weekday() >= 5
        except Exception:
            pass
        p.append('<tr>')
        wk_cls = " wk" if is_wk else ""
        p.append(f'<td class="dt{wk_cls}">{date_val}</td>')

        for i, (src, cat_label, subs) in enumerate(all_cats):
            for sj, (sub_lbl, col_idx, col_type, plan_idx) in enumerate(subs):
                val = _to_num(row.get(col_idx))
                sep = " cat-sep" if (i < len(all_cats) - 1 and sj == len(subs) - 1) else ""
                if col_type == "plan":
                    p.append(f'<td class="pl{sep}">{_fmt_num_display(val)}</td>')
                elif col_type == "fact":
                    plan_val = _to_num(row.get(plan_idx)) if plan_idx is not None else None
                    if val is not None and plan_val is not None:
                        if "бюджет" in (cat_label or "").lower():
                            css = "rd" if val > plan_val else ("gn" if val < plan_val else "em")
                        else:
                            css = "gn" if val >= plan_val else "rd"
                    else:
                        css = "em"
                    p.append(f'<td class="{css}{sep}">{_fmt_num_display(val)}</td>')
                elif col_type == "pct":
                    cell_cls = "gray" if ("реалиация" in (cat_label or "").lower() or "цена" in (cat_label or "").lower()) else "pc"
                    p.append(f'<td class="{cell_cls}{sep}">{_fmt_pct_display(val)}</td>')
                elif col_type == "price":
                    cell_cls = "gray" if ("реалиация" in (cat_label or "").lower() or "цена" in (cat_label or "").lower()) else "rl"
                    p.append(f'<td class="{cell_cls}{sep}">{_fmt_price_display(val)}</td>')
                else:
                    cell_cls = "gray" if ("реалиация" in (cat_label or "").lower() or "цена" in (cat_label or "").lower()) else "rl"
                    p.append(f'<td class="{cell_cls}{sep}">{_fmt_pct_display(val)}</td>')
        p.append('</tr>')

    p.append('</tbody></table></div>')
    html = "".join(p)

    # График: первые Лиды факт
    chart_rows = []
    for _, row in df_data.iterrows():
        leads_fact = None
        leads_plan = None
        for src, blocks in src_blocks.items():
            for cat_label, subs in blocks:
                if "лид" in cat_label.lower():
                    for sub_lbl, col_idx, col_type, plan_idx in subs:
                        if col_type == "fact" and col_idx < len(row):
                            leads_fact = _to_num(row.get(col_idx))
                            if plan_idx is not None:
                                leads_plan = _to_num(row.get(plan_idx))
                            break
                    if leads_fact is not None:
                        break
            if leads_fact is not None:
                break
        chart_rows.append({
            "Дата": str(row[0]).strip(),
            "Лиды план": leads_plan,
            "Лиды факт": leads_fact,
        })
    chart_df = pd.DataFrame(chart_rows)

    return html, chart_df, len(df_data)


def _bounds_or_regions_engine():
    """Для обычного чтения используем только pooler: direct часто тормозит/таймаутит."""
    return _engine()


def _run_bounds_or_regions(fn, default):
    """Запрос bounds или regions с откатом на pooler при translate host / EOF / SSL."""
    engine = _bounds_or_regions_engine()
    if engine is None:
        e2 = _engine()
        return fn(e2) if e2 else default
    try:
        return fn(engine)
    except Exception as e:
        err = str(e).lower()
        if "translate host" in err or "nodename" in err or "servname" in err or "eof" in err or "ssl" in err or "syscall" in err:
            fallback = _engine()
            if fallback:
                return fn(fallback)
        raise


@st.cache_data(ttl=600, show_spinner=False)
def _cached_bounds():
    """Границы дат из БД. Кэш 10 мин — иначе каждый rerun 12–15 s."""
    return _run_bounds_or_regions(date_bounds, None)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_regions():
    """Список регионов. Кэш 10 мин. Engine берётся внутри _run_bounds_or_regions (не в аргументах)."""
    return _run_bounds_or_regions(region_list, [])

@st.cache_data(ttl=600, show_spinner=False)
def _cached_kpi(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return None
    if isinstance(region_list, (list, tuple)) and len(region_list) > 1:
        return kpi_by_region(engine, date_from_str, date_to_str, list(region_list))
    return kpi_extended(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if (isinstance(region_list, (list, tuple)) and len(region_list) == 1) else None)

@st.cache_data(ttl=600, show_spinner=False)
def _cached_funnel(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return None
    if isinstance(region_list, (list, tuple)) and len(region_list) > 1:
        return funnel_by_region(engine, date_from_str, date_to_str, list(region_list))
    return funnel_data(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if (isinstance(region_list, (list, tuple)) and len(region_list) == 1) else None)

def _details_engine():
    """Для деталей используем тот же pooler, чтобы не ловить долгие таймауты direct-хоста."""
    return _engine()


def _run_details_query(fn, *args, **kwargs):
    """Выполнить запрос деталей с откатом на pooler при ошибке резолва хоста / EOF."""
    engine = _details_engine()
    if engine is None:
        eng = _engine()
        return fn(eng, *args, **kwargs) if eng is not None else pd.DataFrame()
    try:
        return fn(engine, *args, **kwargs)
    except Exception as e:
        err = str(e).lower()
        if "translate host" in err or "nodename" in err or "servname" in err or "eof" in err or "ssl" in err or "syscall" in err:
            eng = _engine()
            return fn(eng, *args, **kwargs) if eng is not None else pd.DataFrame()
        raise


@st.cache_data(ttl=600, show_spinner=False)
def _cached_managers(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    return _run_details_query(
        top_managers, date_from_str, date_to_str,
        region=region_key if region_key and region_key != "Все" else None,
        region_list=region_list, limit=10,
    )

@st.cache_data(ttl=600, show_spinner=False)
def _cached_daily(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return None
    if isinstance(region_list, (list, tuple)) and len(region_list) > 1:
        return daily_series_by_region(engine, date_from_str, date_to_str, list(region_list))
    return daily_series(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if (isinstance(region_list, (list, tuple)) and len(region_list) == 1) else None)

@st.cache_data(ttl=600, show_spinner=False)
def _cached_by_region(date_from_str, date_to_str):
    engine = _engine()
    return by_region(engine, date_from_str, date_to_str) if engine is not None else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def _cached_by_utm(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return pd.DataFrame()
    return by_utm(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=25)

@st.cache_data(ttl=600, show_spinner=False)
def _cached_by_formname(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return pd.DataFrame()
    return by_formname(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=30)

@st.cache_data(ttl=300, show_spinner=False)
def _cached_by_landing(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    engine = _engine()
    if engine is None:
        return pd.DataFrame()
    return by_landing(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=30)

@st.cache_data(ttl=600, show_spinner=False)
def _cached_deal_stages(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    return _run_details_query(
        deal_stages, date_from_str, date_to_str,
        region=region_key if region_key and region_key != "Все" else None,
        region_list=region_list, limit=12,
    )

@st.cache_data(ttl=600, show_spinner=False)
def _cached_deal_stages_funnel(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    return _run_details_query(
        deal_stages_funnel, date_from_str, date_to_str,
        region=region_key if region_key and region_key != "Все" else None,
        region_list=region_list,
    )

@st.cache_data(ttl=600, show_spinner=False)
def _cached_reject_reasons(date_from_str, date_to_str, region_key=None, region_list=None):
    region_list = tuple(region_list) if region_list else None
    return _run_details_query(
        rejection_reasons, date_from_str, date_to_str,
        region=region_key if region_key and region_key != "Все" else None,
        region_list=region_list, limit=15,
    )

@st.cache_resource(show_spinner=False)
def _engine():
    """Один engine на процесс без принудительного прогрева соединения."""
    return get_engine()


def _render_forma_block(block_name, gid, color, idx, subtitle="", chart_prefix="om"):
    """Отрисовка одного блока: заголовок → таблица → график (для ОМ и Источников)."""
    subtitle_html = f'<span style="font-size:0.8rem;color:#94A3B8;margin-left:1rem;">{subtitle}</span>' if subtitle else ""
    st.markdown(
        f"""<div style="
            font-family: 'Nunito', sans-serif;
            border-left: 4px solid {color};
            padding: 0.6rem 1.2rem 0.4rem 1.2rem;
            background: linear-gradient(90deg,rgba(30,41,59,0.85) 0%,rgba(15,23,42,0.6) 100%);
            border-radius: 0 12px 12px 0;
            margin-bottom: 0.5rem;
        ">
            <span style="font-size:1.35rem;font-weight:700;color:{color};letter-spacing:0.5px;">{block_name}</span>
            {subtitle_html}
        </div>""",
        unsafe_allow_html=True,
    )
    is_ist = chart_prefix == "ist"
    df_raw = _fetch_google_sheet(gid, raw=True)
    if df_raw is None or df_raw.empty:
        url = _sheet_export_url(gid)
        ok, status, msg = _fetch_sheet_debug(gid)
        st.warning(
            f"⚠️ **{block_name}** (gid={gid}): нет данных.\n\n"
            f"**Диагностика:** {msg}\n\n"
            f"**Что сделать:**\n"
            f"- Откройте [ссылку экспорта]({url}) в браузере: скачивает CSV → доступ есть.\n"
            f"- 403/401 → таблица закрыта: Настройки доступа → «Все, у кого есть ссылка» → **Просматривать**.\n"
            f"- 404 / «не существует» → лист удалён или gid изменился: откройте нужный лист в таблице и скопируйте gid из URL (`#gid=…`).\n"
            f"- Нажмите **↻ Обновить** выше."
        )
        return
    if is_ist:
        html_table, chart_df, n_rows = _parse_forma_ist(df_raw, block_name=block_name)
        if html_table is None:
            df_std = _fetch_google_sheet(gid, raw=True)
            if df_std is not None and not df_std.empty:
                html_table, chart_df, n_rows = _parse_forma_om(df_std, block_name=block_name)
    else:
        html_table, chart_df, n_rows = _parse_forma_om(df_raw, block_name=block_name)
    if html_table is None:
        st.warning(f"Не удалось разобрать таблицу {block_name}.")
        return
    _draw_forma_table_and_chart(block_name, color, idx, html_table, chart_df, chart_prefix, subtitle)


def _draw_forma_table_and_chart(block_name, color, idx, html_table, chart_df, chart_prefix, subtitle=""):
    """Отрисовка уже загруженных таблицы и графика (без запросов)."""
    # --- Таблица (HTML) ---
    st.markdown(html_table, unsafe_allow_html=True)

    # --- Линейный график по дням ---
    _has_leads = False
    if chart_df is not None and not chart_df.empty and "Лиды факт" in chart_df.columns:
        _col = chart_df["Лиды факт"]
        _has_leads = bool(_col.notna().any()) if isinstance(_col, pd.Series) else bool(_col.notna().any().any())
    if _has_leads:
        leads_color = color
        quals_color = "#10B981" if color != "#10B981" else "#0EA5E9"
        chart_data = chart_df[chart_df["Лиды факт"].notna() & (chart_df["Лиды факт"] != "")].copy()

        def _rgba(hex_c, a=0.2):
            if hex_c and hex_c.startswith("#") and len(hex_c) == 7:
                return f"rgba({int(hex_c[1:3],16)},{int(hex_c[3:5],16)},{int(hex_c[5:7],16)},{a})"
            return "rgba(59,130,246,0.2)"

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=chart_data["Дата"], y=chart_data["Лиды план"], name="Лиды план", mode="lines",
            line=dict(color="#64748B", width=1.2, shape="spline", smoothing=0.5, dash="dot"),
        ))
        fig.add_trace(go.Scatter(
            x=chart_data["Дата"], y=chart_data["Лиды факт"], name="Лиды факт", mode="lines+markers",
            line=dict(color=leads_color, width=2.8, shape="spline", smoothing=0.5),
            marker=dict(size=8, color=leads_color, line=dict(width=0)),
            fill="tozeroy", fillcolor=_rgba(leads_color, 0.22),
        ))
        has_quals = "Квалы план" in chart_data.columns and "Квалы факт" in chart_data.columns
        if has_quals:
            fig.add_trace(go.Scatter(
                x=chart_data["Дата"], y=chart_data["Квалы план"], name="Квалы план", mode="lines",
                line=dict(color="#475569", width=1.2, shape="spline", smoothing=0.5, dash="dot"),
            ))
            fig.add_trace(go.Scatter(
                x=chart_data["Дата"], y=chart_data["Квалы факт"], name="Квалы факт", mode="lines+markers",
                line=dict(color=quals_color, width=2.8, shape="spline", smoothing=0.5),
                marker=dict(size=8, color=quals_color, line=dict(width=0)),
                fill="tozeroy", fillcolor=_rgba(quals_color, 0.22),
            ))

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,23,42,0.4)",
            font=dict(color="#94A3B8", family="'Inter','Segoe UI',sans-serif", size=12),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.25)", fixedrange=True, tickangle=-32,
                tickfont=dict(size=11), showline=False, zeroline=False, title=None,
            ),
            yaxis=dict(
                gridcolor="rgba(51,65,85,0.25)", fixedrange=True, showline=False,
                zeroline=False, tickfont=dict(size=11), title=None,
            ),
            legend=dict(
                bgcolor="rgba(15,23,42,0.85)", bordercolor="#334155",
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                font=dict(size=11), itemsizing="constant",
            ),
            margin=dict(t=40, b=50, l=45, r=25),
            height=320,
            dragmode=False, uirevision=f"{chart_prefix}_chart_{idx}", hovermode="x unified",
            hoverlabel=PLOTLY_HOVERLABEL,
            annotations=[],
        )
        fig.update_xaxes(showgrid=True, gridwidth=1)
        fig.update_yaxes(showgrid=True, gridwidth=1)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)


def forma_om_page():
    """Страница Форма ОМ — все таблицы один раз загружаются и кэшируются в сессии."""
    st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        .om-page-header { font-size: 1.8rem; font-weight: 800; font-family: 'Nunito', sans-serif !important;
            background: linear-gradient(90deg, #3B82F6, #10B981, #F59E0B); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.25rem; }
        [data-testid="stTab"] .stDataFrame th, [data-testid="stTab"] .stDataFrame td { font-family: 'Nunito', sans-serif !important; }
    </style>
    """, unsafe_allow_html=True)

    col_h, col_btn = st.columns([5, 1])
    with col_h:
        st.markdown('<p class="om-page-header">Регион ОМ</p>', unsafe_allow_html=True)
        st.caption("Данные из Google Таблицы · план/факт")
    with col_btn:
        if st.button("↻ Обновить", key="om_refresh_all"):
            _fetch_google_sheet.clear()
            if "forma_om_blocks" in st.session_state:
                del st.session_state["forma_om_blocks"]
            st.rerun()

    subtitle = ""
    cache_key = "forma_om_blocks"
    if cache_key not in st.session_state:
        with st.spinner("Загрузка листов ОМ…"):
            def _load_om(om_name, gid, color):
                try:
                    df_raw = _fetch_google_sheet(gid, raw=True)
                except Exception as e:
                    return {"block_name": om_name, "color": color, "html_table": None, "chart_df": None, "error": str(e), "subtitle": subtitle}
                if df_raw is None or df_raw.empty:
                    return {"block_name": om_name, "color": color, "html_table": None, "chart_df": None, "error": f"Нет данных (gid={gid})", "subtitle": subtitle}
                html_table, chart_df, _ = _parse_forma_om(df_raw, block_name=om_name)
                if html_table is None:
                    return {"block_name": om_name, "color": color, "html_table": None, "chart_df": None, "error": "Не удалось разобрать таблицу", "subtitle": subtitle}
                return {"block_name": om_name, "color": color, "html_table": html_table, "chart_df": chart_df, "error": None, "subtitle": subtitle}

            om_items = list(OM_FORMA_GIDS.items())
            tasks = []
            for idx, (om_name, gid) in enumerate(om_items):
                color = OM_COLORS.get(om_name, "#3B82F6")
                tasks.append((idx, _load_om, (om_name, gid, color), {}))
                
            results = _run_parallel_tasks_io(tasks, max_workers=6)
            blocks = []
            for i in range(len(om_items)):
                om_name, _gid = om_items[i]
                b = results.get(i)
                if b is None:
                    err_msg = results.get(f"err_{i}") or "Ошибка загрузки или разбора"
                    blocks.append(
                        {
                            "block_name": om_name,
                            "color": OM_COLORS.get(om_name, "#3B82F6"),
                            "html_table": None,
                            "chart_df": None,
                            "error": err_msg,
                            "subtitle": subtitle,
                        }
                    )
                else:
                    blocks.append(b)

            st.session_state[cache_key] = blocks

    blocks = st.session_state[cache_key]
    tab_labels = [blk["block_name"] for blk in blocks]
    tabs = st.tabs(tab_labels)
    for idx, (tab, blk) in enumerate(zip(tabs, blocks)):
        with tab:
            if blk["error"]:
                st.warning(f"⚠️ {blk['error']}. Нажмите **↻ Обновить** выше.")
            else:
                _draw_forma_table_and_chart(blk["block_name"], blk["color"], idx, blk["html_table"], blk["chart_df"], "om", blk["subtitle"])


def forma_ist_page():
    """Страница Источники — все таблицы один раз загружаются и кэшируются в сессии."""
    st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        .ist-page-header { font-size: 1.8rem; font-weight: 800; font-family: 'Nunito', sans-serif !important;
            background: linear-gradient(90deg, #3B82F6, #10B981, #F59E0B); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.25rem; }
        [data-testid="stTab"] .stDataFrame th, [data-testid="stTab"] .stDataFrame td { font-family: 'Nunito', sans-serif !important; }
    </style>
    """, unsafe_allow_html=True)

    col_h, col_btn = st.columns([5, 1])
    with col_h:
        st.markdown('<p class="ist-page-header">Ист</p>', unsafe_allow_html=True)
        st.caption("Данные из Google Таблицы · план/факт")
    with col_btn:
        if st.button("↻ Обновить", key="ist_refresh_all"):
            _fetch_google_sheet.clear()
            if "forma_ist_blocks" in st.session_state:
                del st.session_state["forma_ist_blocks"]
            st.rerun()

    subtitle = ""
    cache_key = "forma_ist_blocks"
    if cache_key not in st.session_state:
        with st.spinner("Загрузка листов Источники…"):
            def _load_ist(ist_name, gid, color, range_a1):
                try:
                    df_raw = _fetch_google_sheet(gid, raw=True, range_a1=range_a1)
                except Exception as e:
                    return {"block_name": ist_name, "color": color, "html_table": None, "chart_df": None, "error": str(e), "subtitle": subtitle}
                if df_raw is None or df_raw.empty:
                    return {"block_name": ist_name, "color": color, "html_table": None, "chart_df": None, "error": f"Нет данных (gid={gid})", "subtitle": subtitle}
                html_table, chart_df, _ = _parse_forma_ist(df_raw, block_name=ist_name)
                if html_table is None:
                    html_table, chart_df, _ = _parse_forma_om(df_raw, block_name=ist_name)
                if html_table is None:
                    return {"block_name": ist_name, "color": color, "html_table": None, "chart_df": None, "error": "Не удалось разобрать таблицу", "subtitle": subtitle}
                return {"block_name": ist_name, "color": color, "html_table": html_table, "chart_df": chart_df, "error": None, "subtitle": subtitle}

            ist_items = list(IST_FORMA_GIDS.items())
            tasks = []
            for idx, (ist_name, gid) in enumerate(ist_items):
                color = IST_COLORS.get(ist_name, "#3B82F6")
                range_a1 = IST_FORMA_RANGES.get(ist_name)
                tasks.append((idx, _load_ist, (ist_name, gid, color, range_a1), {}))
                
            results = _run_parallel_tasks_io(tasks, max_workers=6)
            blocks = []
            for i in range(len(ist_items)):
                ist_name, _gid = ist_items[i]
                b = results.get(i)
                if b is None:
                    err_msg = results.get(f"err_{i}") or "Ошибка загрузки или разбора"
                    blocks.append(
                        {
                            "block_name": ist_name,
                            "color": IST_COLORS.get(ist_name, "#3B82F6"),
                            "html_table": None,
                            "chart_df": None,
                            "error": err_msg,
                            "subtitle": subtitle,
                        }
                    )
                else:
                    blocks.append(b)

            st.session_state[cache_key] = blocks

    blocks = st.session_state[cache_key]
    tab_labels = [blk["block_name"] for blk in blocks]
    tabs = st.tabs(tab_labels)
    for idx, (tab, blk) in enumerate(zip(tabs, blocks)):
        with tab:
            if blk["error"]:
                st.warning(f"⚠️ {blk['error']}. Нажмите **↻ Обновить** выше.")
            else:
                _draw_forma_table_and_chart(blk["block_name"], blk["color"], idx, blk["html_table"], blk["chart_df"], "ist", blk["subtitle"])


def _run_parallel_tasks(tasks, max_workers):
    """
    Выполняет задачи СЕКВЕНТАЛЬНО для избежания дедлоков Streamlit.
    tasks: список кортежей (key, func, args, kwargs)
    Возвращает dict {key: result}
    """
    results = {}
    for task in tasks:
        key, func, args, kwargs = task
        try:
            results[key] = func(*args, **kwargs)
        except Exception as e:
            results[key] = None
            results[key + "_error"] = str(e)[:500]
    return results


def _run_parallel_tasks_io(tasks, max_workers=6):
    """
    Параллельно: HTTP к Google Sheets + парсинг DataFrame (без Streamlit и без общего DB engine).
    Ускоряет первый заход на страницы ОМ / Источники (раньше все листы шли по очереди).
    """
    results = {}

    def _one(task):
        key, func, args, kwargs = task
        try:
            return key, func(*args, **kwargs), None
        except Exception as e:
            return key, None, str(e)[:500]

    n = len(tasks)
    if not n:
        return results
    max_workers = max(1, min(max_workers, n))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_one, t) for t in tasks]
        for fut in as_completed(futures):
            key, val, err = fut.result()
            if err is not None:
                results[key] = None
                results[f"err_{key}"] = err  # key может быть int — не склеивать с str через +
            else:
                results[key] = val
    return results


def _run_dashboard():
    """Весь контент основного дашборда (Supabase-данные, KPI, воронка и т.д.)."""
    t0 = time.time()
    engine = _engine()
    print(f"[TIMING] после get_engine: {time.time() - t0:.2f}s")
    if engine is None:
        st.error(
            "Не задано подключение к базе. Укажи переменную окружения **SUPABASE_DB_URL** "
            "(Connection string из Supabase → Project Settings → Database)."
        )
        st.code(
            'export SUPABASE_DB_URL="postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres"',
            language="bash",
        )
        return

    if "cache_tables_ensured" not in st.session_state:
        st.session_state["cache_tables_ensured"] = True  # ставим до вызова — не повторять даже при ошибке
        try:
            ensure_cache_table(engine)
        except Exception:
            pass
    print(f"[TIMING] после ensure_cache_table: {time.time() - t0:.2f}s")

    # Одна проверка на весь запуск: от неё зависят и debug, и сообщение «напрямую из таблицы»
    cache_empty = cache_is_empty(engine)
    print(f"[TIMING] после cache_is_empty: {time.time() - t0:.2f}s")

    try:
        with st.spinner("Загрузка…"):
            bounds = _cached_bounds()
            regions = _cached_regions()
        print(f"[TIMING] после bounds + regions: {time.time() - t0:.2f}s")
        if not isinstance(regions, list):
            regions = list(regions) if regions is not None else []
        if bounds is None or bounds.empty:
            st.warning("В кэше KPI нет диапазона дат (таблица пуста или не заполнена). Обнови кэш.")
            return
        def _scalar(v):
            while isinstance(v, (pd.Series, pd.DataFrame)) and len(v):
                v = v.iloc[0]
            return v
        _md = _scalar(bounds["min_d"].iloc[0])
        if pd.isna(_md):
            st.warning("В кэше KPI нет диапазона дат (таблица пуста или не заполнена). Обнови кэш.")
            return
        min_d = _scalar(bounds["min_d"].iloc[0])
        max_d = _scalar(bounds["max_d"].iloc[0])
        # Не вызываем date_bounds() второй раз — _cached_bounds уже использует ту же логику;
        # лишний запрос на каждом rerun давал +5–20 с при медленном pooler.

        if cache_empty:
            st.warning(
                "**Кэш KPI в БД пуст** (таблица `kpi_daily_region` без строк). Дашборд читает только кэш — "
                "нужна пересборка таблицы (ETL / фоновая задача)."
            )
        if hasattr(min_d, "date"):
            min_d = min_d.date()
        if hasattr(max_d, "date"):
            max_d = max_d.date()
    except Exception as e:
        err_text = str(e).lower()
        st.error("Ошибка подключения к базе: " + _db_error_message(e))
        if "circuit breaker" in err_text or "too many authentication errors" in err_text:
            st.info("Подождите 10–15 минут, затем обновите страницу. Проверьте пароль в Secrets (Streamlit Cloud).")
        elif "255.255.255.255" in err_text or "address family" in err_text:
            st.info(
                "**Подключение по хосту db.xxx.supabase.co часто не работает.** Используй **Connection pooler**: "
                "Supabase → Project Settings → Database → URI с хостом `aws-1-eu-north-1.pooler.supabase.com` и вставь в `.env` как `SUPABASE_DB_URL`."
            )
        elif "timed out" in err_text or "timeout" in err_text:
            st.info(
                "**Таймаут соединения** — до сервера не доходят пакеты. Проверь: 1) В `.env` указан pooler (хост `aws-1-eu-north-1.pooler.supabase.com`). "
                "2) Другая сеть или отключи VPN. 3) Файрвол не блокирует исходящий порт 5432."
            )
        return

    # Сайдбар
    with st.sidebar:
        st.header("Период и фильтры")
        # По умолчанию: вчера (один день) — быстрая загрузка
        yesterday = max_d - timedelta(days=1)
        if yesterday < min_d:
            yesterday = min_d
        default_period1 = (yesterday, yesterday)
        default_period2 = (min_d, yesterday)

        # Инициализация сохранённых фильтров в сессии
        if "applied_filters" not in st.session_state:
            st.session_state["applied_filters"] = {
                "period1": default_period1,
                "use_period2": False,
                "period2": default_period2,
                "regions": [],
            }

        applied = st.session_state["applied_filters"]

        with st.form("filters_form"):
            st.markdown("**Период 1 (основной)**")
            period1 = st.date_input(
                "Диапазон дат для Периода 1",
                value=applied.get("period1", default_period1),
                min_value=min_d,
                max_value=max_d,
                key="period1_input",
            )

            st.markdown("**Период 2 (для сравнения)**")
            period2 = st.date_input(
                "Диапазон дат для Периода 2",
                value=applied.get("period2", default_period2),
                min_value=min_d,
                max_value=max_d,
                key="period2_input",
            )
            use_period2 = st.checkbox(
                "Использовать Период 2 в расчётах",
                value=applied.get("use_period2", False),
                key="use_period2_checkbox",
            )

            st.markdown("---")
            st.markdown("**Направления**")
            ALLOWED_REGIONS = ["Крым", "Сочи", "Анапа", "Баку"]
            _reg_ok = regions is None or (isinstance(regions, (list, tuple)) and len(regions) == 0)
            region_options = [r for r in ALLOWED_REGIONS if r in regions or _reg_ok]
            if not region_options:
                region_options = ALLOWED_REGIONS
            selected_regions = st.multiselect(
                "Выбери одно или несколько направлений",
                options=region_options,
                default=applied.get("regions", []),
                help="Крым, Сочи, Анапа, Баку. При нескольких выбранных графики покажут сравнение по направлениям.",
                key="regions_multiselect",
            )

            submitted = st.form_submit_button("Применить фильтры")

        # Применяем фильтры только при нажатии «Применить»
        if submitted:
            # Нормализуем периоды
            if isinstance(period1, (list, tuple)) and len(period1) == 2:
                date_from_1, date_to_1 = period1
            else:
                date_from_1 = date_to_1 = period1
            if date_from_1 > date_to_1:
                date_to_1 = date_from_1

            if use_period2:
                if isinstance(period2, (list, tuple)) and len(period2) == 2:
                    date_from_2, date_to_2 = period2
                else:
                    date_from_2 = date_to_2 = period2
                if date_from_2 > date_to_2:
                    date_to_2 = date_from_2
            else:
                date_from_2 = date_to_2 = None

            st.session_state["applied_filters"] = {
                "period1": (date_from_1, date_to_1),
                "use_period2": use_period2,
                "period2": (date_from_2, date_to_2) if (date_from_2 and date_to_2) else default_period2,
                "regions": list(selected_regions),
            }
            applied = st.session_state["applied_filters"]

        # Используем только применённые значения
        date_from_1, date_to_1 = applied["period1"]
        use_period2 = applied["use_period2"]
        period2 = applied.get("period2", (None, None))
        date_from_2, date_to_2 = period2 if use_period2 else (None, None)
        selected_regions = applied.get("regions", [])

        # Приведение диапазонов к корректным значениям
        if date_from_1 > date_to_1:
            date_to_1 = date_from_1
        if date_from_2 and date_to_2 and date_from_2 > date_to_2:
            date_to_2 = date_from_2

        # Региональный фильтр (region_list всегда tuple при нескольких регионах — стабильный ключ кэша)
        if not selected_regions:
            region_key, region_list = "Все", None
        elif len(selected_regions) == 1:
            region_key, region_list = selected_regions[0], None
        else:
            region_key, region_list = None, tuple(selected_regions)
        compare_mode = region_list is not None and len(region_list) > 1

    date_from_str = date_from_1.isoformat() if hasattr(date_from_1, "isoformat") else str(date_from_1)
    date_to_str = date_to_1.isoformat() if hasattr(date_to_1, "isoformat") else str(date_to_1)
    date_from_2_str = date_to_2_str = None
    if date_from_2 and date_to_2:
        date_from_2_str = date_from_2.isoformat() if hasattr(date_from_2, "isoformat") else str(date_from_2)
        date_to_2_str = date_to_2.isoformat() if hasattr(date_to_2, "isoformat") else str(date_to_2)

    # Двухфазная загрузка: phase1 — KPI/воронка/daily; phase2 — детали из кэш-таблиц.
    compare_mode = region_list is not None and len(region_list) > 1

    try:
        filters_key = (
            date_from_str,
            date_to_str,
            date_from_2_str,
            date_to_2_str,
            region_key,
            tuple(region_list) if region_list else None,
        )

        loaded = None
        need_phase1 = (
            "last_loaded_filters_key" not in st.session_state
            or st.session_state["last_loaded_filters_key"] != filters_key
            or "last_loaded_data" not in st.session_state
        )
        need_phase2 = st.session_state.get("need_phase2", False) and st.session_state.get("last_loaded_filters_key") == filters_key

        if need_phase2:
            # Не грузим детали сразу — сначала отрисуем верх (KPI, воронка, динамика), потом догрузим детали ниже
            loaded = dict(st.session_state.get("last_loaded_data") or {})

        if need_phase1:
            t_phase1 = time.time()
            with st.spinner("Загружаю данные…"):
                phase1_tasks = [
                    ("kpi", _cached_kpi, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
                    ("funnel", _cached_funnel, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
                    ("daily", _cached_daily, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
                    ("by_region", _cached_by_region, (date_from_str, date_to_str), {}),
                ]
                if (not compare_mode) and date_from_2_str and date_to_2_str:
                    phase1_tasks.append(
                        ("kpi2", _cached_kpi, (date_from_2_str, date_to_2_str), {"region_key": region_key, "region_list": region_list}),
                    )
                out = _run_parallel_tasks(phase1_tasks, max_workers=4)
                loaded = out
                print(f"[TIMING] phase1 параллельная загрузка (kpi/funnel/daily/by_region): {time.time() - t_phase1:.2f}s")
                st.session_state["last_loaded_filters_key"] = filters_key
                st.session_state["last_loaded_data"] = loaded
                st.session_state["need_phase2"] = True
                print(f"[TIMING] phase1 завершена (без лишнего rerun): {time.time() - t0:.2f}s")
                # Не делаем st.rerun() здесь: второй полный прогон скрипта удваивал время до первого KPI.
        else:
            loaded = st.session_state.get("last_loaded_data")
            print(f"[TIMING] данные из session (phase1 уже были): {time.time() - t0:.2f}s")
            if loaded is None:
                # сессия протухла или данные сброшены — принудительно грузим фазу 1 заново
                for key in ("last_loaded_filters_key", "last_loaded_data", "phase2_loaded_for_key"):
                    st.session_state.pop(key, None)
                st.rerun()
                return
            loaded = loaded if isinstance(loaded, dict) else {}

        kpi = loaded.get("kpi")
        funnel = loaded.get("funnel")
        daily = loaded.get("daily")
        managers = loaded.get("managers")
        df_stage = loaded.get("stages_funnel")  # 12 этапов по полям (хронология) для секции «Этапы сделок»
        df_rej = loaded.get("reject_reasons")
        kpi2 = loaded.get("kpi2")
        if daily is None or (isinstance(daily, pd.DataFrame) and daily.empty):
            daily = pd.DataFrame()
        else:
            daily = daily if isinstance(daily, pd.DataFrame) else pd.DataFrame()
        if managers is None or (isinstance(managers, pd.DataFrame) and managers.empty):
            managers = pd.DataFrame()
        else:
            managers = managers if isinstance(managers, pd.DataFrame) else pd.DataFrame()
        if df_stage is None or (isinstance(df_stage, pd.DataFrame) and df_stage.empty):
            df_stage = pd.DataFrame()
        else:
            df_stage = df_stage if isinstance(df_stage, pd.DataFrame) else pd.DataFrame()
        if df_rej is None or (isinstance(df_rej, pd.DataFrame) and df_rej.empty):
            df_rej = pd.DataFrame()
        else:
            df_rej = df_rej if isinstance(df_rej, pd.DataFrame) else pd.DataFrame()

        if kpi is None:
            err = (loaded or {}).get("kpi_error", "")
            st.error("Не удалось загрузить KPI." + (f" ({err})" if err else ""))
            return
        if not isinstance(kpi, pd.DataFrame):
            st.error("Не удалось загрузить KPI (неверный формат).")
            return
        print(f"[TIMING] после получения loaded (kpi/funnel/daily распакованы): {time.time() - t0:.2f}s")
    except Exception as e:
        st.error("Ошибка загрузки данных: " + _db_error_message(e))
        return

    if kpi.empty:
        st.warning("Нет данных за выбранный период.")
        return

    # Progressive loading: фаза 1 — KPI + графики «Динамика по дням» и «Воронка»; фаза 2 — брокеры, этапы, причины, детальные разбивки.

    CHART_HEIGHT = 520
    # Верхний донат в KPI-блоке должен быть компактнее и ближе к квадрату, чтобы круг не выглядел растянутым.
    FUNNEL_KPI_PIE_HEIGHT = 430

    if compare_mode:
        st.subheader(f"KPI за период · {date_from_str} — {date_to_str} · сравнение регионов")
        kpi_display = kpi.copy()
        # Русские подписи колонок
        rename_map = {
            "region": "Регион",
            "leads": "Лиды",
            "prequals": "Предквалы",
            "quals": "Квалы",
            "pokaz": "Показы",
            "broni": "Брони",
            "sdelki": "Сделки",
            "komissi": "Комиссии",
        }
        kpi_display = kpi_display.rename(columns={k: v for k, v in rename_map.items() if k in kpi_display.columns})
        # Числовые колонки явно приводим к float, чтобы не ловить object-тип
        if {"Лиды", "Квалы", "Сделки"}.issubset(kpi_display.columns):
            leads_num = pd.to_numeric(kpi_display["Лиды"], errors="coerce")
            quals_num = pd.to_numeric(kpi_display["Квалы"], errors="coerce")
            deals_num = pd.to_numeric(kpi_display["Сделки"], errors="coerce")

            base_leads = leads_num.replace(0, pd.NA)
            base_quals = quals_num.replace(0, pd.NA)

            # Результат деления может содержать pd.NA — не приводим к float через astype, иначе NAType даёт ошибку
            conv_lead_kval = (100 * quals_num / base_leads).replace({pd.NA: np.nan})
            conv_kval_sdelka = (100 * deals_num / base_quals).replace({pd.NA: np.nan})
            kpi_display["Конв. Лид→Квал %"] = pd.to_numeric(conv_lead_kval, errors="coerce").round(1)
            kpi_display["Конв. Квал→Сделка %"] = pd.to_numeric(conv_kval_sdelka, errors="coerce").round(1)

        st.dataframe(kpi_display, use_container_width=True, hide_index=True)
    else:
        row = kpi.iloc[0]
        leads = int(row.get("leads", 0) or 0)
        quals = int(row.get("quals", 0) or 0)
        prequals = int(row.get("prequals", 0) or 0)
        passports = int(row.get("passports", 0) or 0)
        pokaz = int(row.get("pokaz", 0) or 0)
        broni = int(row.get("broni", 0) or 0)
        sdelki = int(row.get("sdelki", 0) or 0)
        kommissii = float(row.get("komissi", 0) or 0)

        # Если выбран Период 2 — считаем дельты по нему
        if kpi2 is not None and isinstance(kpi2, pd.DataFrame) and not kpi2.empty:
            prev_row = kpi2.iloc[0]
            delta_leads = leads - int(prev_row.get("leads", 0) or 0)
            delta_quals = quals - int(prev_row.get("quals", 0) or 0)
            delta_prequals = prequals - int(prev_row.get("prequals", 0) or 0)
            delta_sdelki = sdelki - int(prev_row.get("sdelki", 0) or 0)
            delta_pokaz = pokaz - int(prev_row.get("pokaz", 0) or 0)
            delta_broni = broni - int(prev_row.get("broni", 0) or 0)
            delta_komm = kommissii - float(prev_row.get("komissi", 0) or 0)
        else:
            prev_row = None
            delta_leads = delta_quals = delta_prequals = delta_sdelki = delta_pokaz = delta_broni = delta_komm = None

        st.subheader(
            f"KPI · Период 1: {date_from_str} — {date_to_str}"
            + (f" · {region_key}" if region_key and region_key != "Все" else "")
        )
        if prev_row is not None and date_from_2_str and date_to_2_str and (delta_leads is not None or delta_quals is not None):
            st.caption(f"Сравнение с Периодом 2: {date_from_2_str} — {date_to_2_str}")
        # Верхний экран: слева воронка + комиссии, справа KPI.
        # Делаем правую часть заметно уже, чтобы композиция была пропорциональнее.
        col_left, col_right = st.columns([1.22, 0.78])
        with col_left:
            komm_str = f"{kommissii / 1_000_000:.1f}М ₽" if kommissii >= 1_000_000 else (f"{kommissii / 1_000:.0f}К ₽" if kommissii >= 1_000 else f"{kommissii:.0f} ₽")
            st.markdown(f"""
            <div class="dash-balance">
                <div class="dash-balance-title">Комиссии за период</div>
                <div class="dash-balance-value">{komm_str}</div>
            </div>
            """, unsafe_allow_html=True)
            # Воронка — в колонке под комиссиями (заполняет пустое место слева)
            st.subheader("Воронка")
            try:
                if funnel is not None and not funnel.empty:
                    f_row = funnel.iloc[0]
                    stages = ["Лиды", "Предквалы", "Квалы", "Показы", "Брони", "Сделки"]
                    cols_f = ["leads", "prequals", "quals", "pokaz", "broni", "sdelki"]
                    values = [int(f_row.get(c) or 0) for c in cols_f]
                    colors = ["#A78BFA", "#C4B5FD", "#34D399", "#FBBF24", "#EF4444", "#2DD4BF"]
                    pull = [0.025] * len(stages)
                    fig_donut_kpi = go.Figure(go.Pie(
                        labels=stages, values=values, hole=0.62, pull=pull,
                        marker=dict(colors=colors, line=dict(color="rgba(255,255,255,0.06)", width=2)),
                        textinfo="label+percent", textposition="outside",
                        textfont=dict(size=10, family="'Space Grotesk', sans-serif"),
                        hovertemplate="<b>%{label}</b><br>%{value} (%{percent})<extra></extra>",
                    ))
                    fig_donut_kpi.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#E2E8F0", size=10, family="'Space Grotesk', sans-serif"),
                        margin=dict(t=6, b=88, l=4, r=4),
                        height=FUNNEL_KPI_PIE_HEIGHT,
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="top",
                            y=-0.03,
                            x=0.5,
                            xanchor="center",
                            bgcolor="rgba(30,41,59,0.55)",
                            font=dict(size=9, color="#E2E8F0"),
                        ),
                        dragmode=False,
                        uirevision="donut_kpi",
                        hoverlabel=PLOTLY_HOVERLABEL,
                    )
                    st.plotly_chart(fig_donut_kpi, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных для воронки.")
            except Exception as e:
                st.error("Ошибка воронки: " + _db_error_message(e))
        with col_right:
            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("Лиды", f"{leads:,}", delta=delta_leads)
                st.metric("Предквалы", f"{prequals:,}", delta=delta_prequals)
            with k2:
                st.metric("Квалы", f"{quals:,}", delta=delta_quals)
                st.metric("Показы", f"{pokaz:,}", delta=delta_pokaz)
            with k3:
                st.metric("Сделки", f"{sdelki:,}", delta=delta_sdelki)
                st.metric("Брони", f"{broni:,}", delta=delta_broni)

            conv_lead_qual = round(100 * quals / leads, 1) if leads > 0 else 0
            conv_qual_sdelka = round(100 * sdelki / quals, 1) if quals > 0 else 0
            st.metric("Конв. Лид→Квал", f"{conv_lead_qual}%")
            delta_komm_str = (
                f"{delta_komm / 1_000:.0f}К ₽"
                if delta_komm is not None and 1_000 <= abs(delta_komm) < 1_000_000
                else (
                    f"{delta_komm / 1_000_000:.1f}М ₽"
                    if delta_komm is not None and abs(delta_komm) >= 1_000_000
                    else None
                )
            )
            st.metric("Δ Комиссии", komm_str, delta=delta_komm_str)

        # Небольшая сводная таблица для явного сравнения Периода 1 и Периода 2
        if prev_row is not None and date_from_2_str and date_to_2_str:
            compare_df = pd.DataFrame(
                {
                    "Метрика": ["Лиды", "Предквалы", "Квалы", "Показы", "Брони", "Сделки", "Комиссии"],
                    f"Период 1: {date_from_str} — {date_to_str}": [
                        leads,
                        prequals,
                        quals,
                        pokaz,
                        broni,
                        sdelki,
                        round(kommissii),
                    ],
                    f"Период 2: {date_from_2_str} — {date_to_2_str}": [
                        int(prev_row.get("leads", 0) or 0),
                        int(prev_row.get("prequals", 0) or 0),
                        int(prev_row.get("quals", 0) or 0),
                        int(prev_row.get("pokaz", 0) or 0),
                        int(prev_row.get("broni", 0) or 0),
                        int(prev_row.get("sdelki", 0) or 0),
                        round(float(prev_row.get("komissi", 0) or 0)),
                    ],
                }
            )
            st.dataframe(compare_df, hide_index=True, use_container_width=True)

        # Динамика по дням — под KPI/воронкой на всю ширину (после KPI-сводки, чтобы верх был «ровный»)
        st.subheader("Динамика по дням")
        if loaded.get("daily_error"):
            st.warning("Данные по дням временно недоступны")
        if daily is None:
            daily = pd.DataFrame()
        if not daily.empty:
            daily_full = daily.copy()
            daily_full["date_str"] = pd.to_datetime(daily_full["date"]).astype(str).str[:10]
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(name="Лиды", x=daily_full["date_str"], y=daily_full["leads"], marker_color="#A78BFA", marker_line=dict(width=0)))
            fig_bar.add_trace(go.Bar(name="Квалы", x=daily_full["date_str"], y=daily_full["quals"], marker_color="#34D399", marker_line=dict(width=0)))
            fig_bar.add_trace(go.Bar(name="Предквалы", x=daily_full["date_str"], y=daily_full["prequals"], marker_color="#C4B5FD", marker_line=dict(width=0)))
            _apply_bar_rounded(fig_bar)
            fig_bar.update_layout(
                barmode="group", template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.4)",
                font=dict(color="#E2E8F0", family="'Space Grotesk', sans-serif"),
                xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, bgcolor="rgba(30,41,59,0.8)"),
                margin=dict(t=30, b=20, l=20, r=20), hovermode="x unified", height=CHART_HEIGHT, dragmode=False, uirevision="revflow_full",
                hoverlabel=PLOTLY_HOVERLABEL,
            )
            st.plotly_chart(fig_bar, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("Нет данных по дням.")

    print(f"[TIMING] после блока KPI (метрики + таблица): {time.time() - t0:.2f}s")
    st.divider()

    try:
        if daily is None:
            daily = pd.DataFrame()
    except Exception:
        daily = pd.DataFrame()

    # Динамика + воронка в одной строке — только при сравнении регионов (в обычном режиме они в KPI-колонках)
    if compare_mode:
        col_flow, col_split = st.columns([6, 4])
        with col_flow:
            st.subheader("Динамика по дням")
        with col_split:
            st.subheader("Воронка")

        chart_row1_a, chart_row1_b = st.columns([6, 4])
        with chart_row1_a:
            if loaded.get("daily_error"):
                st.warning("Данные по дням временно недоступны")
            if not daily.empty:
                daily_cmp = daily.copy()
                daily_cmp["date_str"] = pd.to_datetime(daily_cmp["date"]).astype(str).str[:10]
                if "region" in daily_cmp.columns:
                    fig_bar = px.bar(daily_cmp, x="date_str", y="leads", color="region", barmode="group", color_discrete_sequence=["#A78BFA", "#34D399", "#F59E0B"])
                else:
                    fig_bar = go.Figure()
                    fig_bar.add_trace(go.Bar(name="Лиды", x=daily_cmp["date_str"], y=daily_cmp["leads"], marker_color="#A78BFA", marker_line=dict(width=0)))
                    if "quals" in daily_cmp.columns:
                        fig_bar.add_trace(go.Bar(name="Квалы", x=daily_cmp["date_str"], y=daily_cmp["quals"], marker_color="#34D399", marker_line=dict(width=0)))
                    if "prequals" in daily_cmp.columns:
                        fig_bar.add_trace(go.Bar(name="Предквалы", x=daily_cmp["date_str"], y=daily_cmp["prequals"], marker_color="#C4B5FD", marker_line=dict(width=0)))
                _apply_bar_rounded(fig_bar)
                fig_bar.update_layout(
                    template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.4)",
                    font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                    xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True),
                    legend=dict(bgcolor="rgba(30,41,59,0.8)"), margin=dict(t=30, b=20), height=CHART_HEIGHT, dragmode=False, uirevision="daily",
                    hoverlabel=PLOTLY_HOVERLABEL,
                )
                st.plotly_chart(fig_bar, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Нет данных по дням.")
        print(f"[TIMING] после графика «Динамика по дням» (compare): {time.time() - t0:.2f}s")
        with chart_row1_b:
            try:
                if funnel is not None and not funnel.empty and "region" in funnel.columns:
                    stages = ["Лиды", "Предквалы", "Квалы", "Показы", "Брони", "Сделки"]
                    cols = ["leads", "prequals", "quals", "pokaz", "broni", "sdelki"]
                    long = []
                    for _, r in funnel.iterrows():
                        reg = r["region"]
                        for s, c in zip(stages, cols):
                            long.append({"Этап": s, "Регион": reg, "Кол-во": int(r.get(c) or 0)})
                    df_long = pd.DataFrame(long)
                    fig_funnel = px.bar(df_long, x="Этап", y="Кол-во", color="Регион", barmode="group",
                        category_orders={"Этап": stages}, color_discrete_sequence=["#A78BFA", "#34D399", "#F59E0B", "#2DD4BF"])
                    _apply_bar_rounded(fig_funnel)
                    fig_funnel.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.4)", font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                        xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True), legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                        margin=dict(t=30, b=50), height=CHART_HEIGHT, dragmode=False, uirevision="funnel", hoverlabel=PLOTLY_HOVERLABEL,
                    )
                    st.plotly_chart(fig_funnel, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных для воронки.")
            except Exception as e:
                st.error("Ошибка воронки: " + _db_error_message(e))

        print(f"[TIMING] после графика «Воронка» (compare): {time.time() - t0:.2f}s")
    st.divider()

    # Фаза 2: как раньше — одним пакетом брокеры, этапы, причины и все детальные разбивки.
    _ld = st.session_state.get("last_loaded_data") or {}
    _keys_ok = st.session_state.get("last_loaded_filters_key") == filters_key
    phase2_should_run = _keys_ok and (
        (st.session_state.get("need_phase2", False) and st.session_state.get("phase2_loaded_for_key") != filters_key)
        or ("managers" not in _ld)
    )
    if phase2_should_run:
        phase2_tasks = [
            ("utm", _cached_by_utm, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
            ("landing", _cached_by_landing, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
            ("by_formname", _cached_by_formname, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
            ("managers", _cached_managers, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
            ("stages_funnel", _cached_deal_stages_funnel, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
            ("reject_reasons", _cached_reject_reasons, (date_from_str, date_to_str), {"region_key": region_key, "region_list": region_list}),
        ]
        t_phase2 = time.time()
        with st.spinner("Загружаю детали…"):
            phase2_results = _run_parallel_tasks(phase2_tasks, max_workers=6)
            base = dict(st.session_state.get("last_loaded_data") or {})
            base.update(phase2_results)
            st.session_state["last_loaded_data"] = base
            loaded = base
            print(f"[TIMING] phase2 параллельная загрузка деталей: {time.time() - t_phase2:.2f}s")
            st.session_state["need_phase2"] = False
            st.session_state["phase2_loaded_for_key"] = filters_key
        print(f"[TIMING] после phase2: {time.time() - t0:.2f}s")
        managers = loaded.get("managers")
        if managers is None or (isinstance(managers, pd.DataFrame) and managers.empty):
            managers = pd.DataFrame()
        else:
            managers = managers if isinstance(managers, pd.DataFrame) else pd.DataFrame()
        df_stage = loaded.get("stages_funnel")
        if df_stage is None or (isinstance(df_stage, pd.DataFrame) and df_stage.empty):
            df_stage = pd.DataFrame()
        else:
            df_stage = df_stage if isinstance(df_stage, pd.DataFrame) else pd.DataFrame()
        df_rej = loaded.get("reject_reasons")
        if df_rej is None or (isinstance(df_rej, pd.DataFrame) and df_rej.empty):
            df_rej = pd.DataFrame()
        else:
            df_rej = df_rej if isinstance(df_rej, pd.DataFrame) else pd.DataFrame()

    print(f"[TIMING] перед блоком Топ брокеров: {time.time() - t0:.2f}s")
    st.subheader("Топ брокеров по квалам")
    if loaded.get("managers_error"):
        st.warning("Данные временно недоступны")
    try:
        if not managers.empty:
            # Подстановка имён из update_supabase_responsibles.sql, если в БД ещё «ID 123»
            id_to_name = get_responsible_id_to_name_map()
            if id_to_name and "broker_id" in managers.columns:
                def _broker_display_name(row):
                    name = row.get("broker_name") or ""
                    if str(name).strip().startswith("ID "):
                        return id_to_name.get(row.get("broker_id"), name)
                    return name
                managers = managers.copy()
                managers["broker_name"] = managers.apply(_broker_display_name, axis=1)

            managers = managers.reset_index(drop=True)
            managers.index = managers.index + 1
            managers["№"] = managers.index.map(lambda x: str(x))

            # Переименовываем колонки для красоты; отображаем и ID из Amo (responsible_user_id)
            cols_mgr = ["№", "broker_name", "leads"]
            if "prequals" in managers.columns:
                cols_mgr.append("prequals")
            cols_mgr += ["quals", "conv_percent"]
            managers_display = managers[cols_mgr].copy()
            col_names_mgr = ["№", "Брокер", "Лиды"]
            if "prequals" in managers.columns:
                col_names_mgr.append("Предквалы")
            col_names_mgr += ["Квалы", "Конв. %"]
            managers_display.columns = col_names_mgr

            st.dataframe(managers_display, use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Нет данных по менеджерам за этот период.")
    except Exception as e:
        st.error(f"Ошибка загрузки менеджеров: {e}")
    
    st.divider()

    st.subheader("Этапы сделок (по статусу) за период")
    if loaded.get("stages_funnel_error"):
        st.warning("Данные временно недоступны")
    if not df_stage.empty:
        # Сортируем от большего к меньшему: сверху — больше, снизу — меньше
        df_stage_sorted = df_stage.sort_values("cnt", ascending=False).reset_index(drop=True)
        seen: set = set()
        stage_options: list = []
        for s in df_stage_sorted["stage"].dropna().tolist():
            if str(s).strip() and s not in seen:
                seen.add(s)
                stage_options.append(s)

        stage_color_map = {
            stage_options[i]: STAGE_LEGEND_PALETTE[i % len(STAGE_LEGEND_PALETTE)]
            for i in range(len(stage_options))
        }

        total_all = float(df_stage_sorted["cnt"].sum())
        df_stage_sorted["percent"] = (
            (df_stage_sorted["cnt"] / total_all * 100).round(1) if total_all > 0 else 0
        )

        # Порядок этапов в воронке (как в данных) — для «% от предыдущего этапа» в тултипе
        funnel_stage_order = [lbl for _, lbl in FUNNEL_STAGES]
        cnt_by_stage = df_stage_sorted.set_index("stage")["cnt"].astype(int).to_dict()

        def _pct_from_previous(stage_name: str, cnt: int):
            try:
                idx = funnel_stage_order.index(str(stage_name))
            except ValueError:
                return None, None
            if idx <= 0:
                return None, None
            prev_name = funnel_stage_order[idx - 1]
            prev_cnt = int(cnt_by_stage.get(prev_name, 0) or 0)
            if prev_cnt <= 0:
                return None, prev_name
            return round(100.0 * float(cnt) / float(prev_cnt), 1), prev_name

        df_stage_display = df_stage_sorted[["stage", "cnt", "percent"]].copy()
        df_stage_display.columns = ["Этап", "Сделок", "Доля, %"]
        df_stage_display["Доля, %"] = df_stage_display["Доля, %"].map(
            lambda v: f"{v:.1f}%" if pd.notna(v) else ""
        )
        df_stage_display["Сделок"] = df_stage_display["Сделок"].apply(
            lambda x: f"{int(x):,}".replace(",", " ")
        )

        n = len(df_stage_sorted)
        col_stage_chart, col_stage_table = st.columns([1.2, 0.8])
        with col_stage_chart:
            # Один go.Funnel trace на каждый этап → нативная Plotly-легенда,
            # клик по квадратику скрывает/показывает этот этап (как у доната «Воронка»)
            fig_stage = go.Figure()
            for i, row in df_stage_sorted.iterrows():
                stage_name = row["stage"]
                cnt_val = int(row["cnt"])
                pct_val = float(row["percent"])
                color = stage_color_map.get(stage_name, STAGE_LEGEND_PALETTE[i % len(STAGE_LEGEND_PALETTE)])
                prev_pct, prev_lbl = _pct_from_previous(stage_name, cnt_val)
                if prev_pct is not None and prev_lbl:
                    hover_prev = (
                        f"<br><span style='opacity:0.95'>От «{prev_lbl}»: <b>{prev_pct:.1f}%</b></span>"
                    )
                elif prev_lbl is not None and prev_pct is None:
                    hover_prev = f"<br><span style='opacity:0.85'>После «{prev_lbl}»: нет базы (0)</span>"
                else:
                    hover_prev = ""
                hover_tmpl = (
                    f"<b>{stage_name}</b><br>"
                    f"Сделок: {cnt_val:,}<br>"
                    f"Доля от суммы этапов: {pct_val:.1f}%"
                    f"{hover_prev}<extra></extra>"
                )
                fig_stage.add_trace(go.Funnel(
                    name=stage_name,
                    y=[stage_name],
                    x=[cnt_val],
                    text=[f"{cnt_val:,}".replace(",", "\u202f") + f" ({pct_val:.1f}%)"],
                    textinfo="text",
                    textposition="inside",
                    textfont=dict(size=12, color="rgba(255,255,255,0.97)", family="'Space Grotesk', sans-serif"),
                    marker=dict(
                        color=color,
                        line=dict(color="rgba(255,255,255,0.13)", width=2),
                    ),
                    connector=dict(line=dict(color="rgba(148,163,184,0.18)", width=2)),
                    showlegend=True,
                    hovertemplate=hover_tmpl,
                ))
            fig_stage.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(30,41,59,0.4)",
                font=dict(color="#F1F5F9", size=11, family="'Space Grotesk', sans-serif"),
                # Большой нижний отступ + легенда ниже области графика (yanchor=top), чтобы не наезжала на воронку
                margin=dict(t=16, b=210, l=18, r=18),
                height=max(470, 46 * n + 210),
                dragmode=False,
                uirevision="stages",
                funnelmode="stack",
                hoverlabel={**PLOTLY_HOVERLABEL, "font_size": 12},
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.06,
                    x=0.5,
                    xanchor="center",
                    bgcolor="rgba(30,41,59,0.55)",
                    bordercolor="rgba(255,255,255,0.06)",
                    borderwidth=1,
                    font=dict(size=10, color="#E2E8F0"),
                    traceorder="normal",
                ),
            )
            st.plotly_chart(fig_stage, use_container_width=True, config=PLOTLY_CONFIG)

        with col_stage_table:
            st.dataframe(
                df_stage_display,
                use_container_width=True,
                hide_index=True,
                height=min(900, 52 * n + 40),
            )
    else:
        st.info("Нет данных по этапам за период.")

    st.subheader("Причины отказа за период")
    if loaded.get("reject_reasons_error"):
        st.warning("Данные временно недоступны")
    if not df_rej.empty:
        total = float(df_rej["cnt"].sum())
        df_rej = df_rej.copy()
        df_rej["percent"] = (df_rej["cnt"] / total * 100).round(1) if total > 0 else 0
        fig_rej = px.bar(df_rej, x="reason", y="cnt", text="percent", labels={"reason": "Причина", "cnt": "Количество"}, color_discrete_sequence=["#EF4444"])
        fig_rej.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
        _apply_bar_rounded(fig_rej)
        fig_rej.update_layout(
            template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
            xaxis_tickangle=-35, xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True),
            margin=dict(t=30, b=70, l=20, r=20), height=CHART_HEIGHT, showlegend=False, dragmode=False, uirevision="reasons", hoverlabel=PLOTLY_HOVERLABEL,
        )
        st.plotly_chart(fig_rej, use_container_width=True, config=PLOTLY_CONFIG)
        st.dataframe(df_rej, use_container_width=True, hide_index=True, height=280)
    else:
        st.info("Нет данных по причинам отказа за период.")

    st.divider()
    st.subheader("Детальные разбивки")
    tab_region, tab_utm, tab_formname, tab_landing = st.tabs(
        ["По региону", "По UTM", "По formname", "По посадочной (UTM referrer)"]
    )
    try:
        with tab_region:
            df_region = loaded.get("by_region")
            if df_region is None or (isinstance(df_region, pd.DataFrame) and df_region.empty):
                df_region = _cached_by_region(date_from_str, date_to_str)
            if df_region is not None and not df_region.empty:
                rename_map = {"region": "Регион", "leads": "Лиды", "prequals": "Предквалы", "quals": "Квалы"}
                region_cols = [c for c in ["region", "leads", "prequals", "quals"] if c in df_region.columns]
                df_region_display = df_region[region_cols].rename(columns=rename_map)
                st.dataframe(df_region_display, use_container_width=True, hide_index=True)
                chart_metrics = [col for col in ["Лиды", "Предквалы", "Квалы"] if col in df_region_display.columns]
                fig_r = px.bar(
                    df_region_display.head(15),
                    x="Регион",
                    y=chart_metrics,
                    barmode="group",
                    color_discrete_sequence=["#3B82F6", "#8B5CF6", "#10B981"],
                    labels={"value": "Кол-во", "variable": "Метрика"},
                )
                _apply_bar_rounded(fig_r)
                fig_r.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(30,41,59,0.5)",
                    font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                    xaxis=dict(gridcolor="#334155", fixedrange=True),
                    yaxis=dict(gridcolor="#334155", fixedrange=True),
                    legend=dict(bgcolor="rgba(30,41,59,0.8)", font=dict(size=10)),
                    margin=dict(t=24, b=28, l=18, r=18),
                    dragmode=False,
                    uirevision="tab_r",
                    hoverlabel=PLOTLY_HOVERLABEL,
                )
                st.plotly_chart(fig_r, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Нет данных по регионам.")
        with tab_utm:
            df_utm = loaded.get("utm")
            if df_utm is None:
                df_utm = pd.DataFrame()
            if df_utm is not None and not df_utm.empty:
                agg_dict = {"leads": ("leads", "sum"), "quals": ("quals", "sum")}
                if "prequals" in df_utm.columns:
                    agg_dict["prequals"] = ("prequals", "sum")
                df_utm_display = (
                    df_utm.groupby("utm_source", as_index=False)
                    .agg(**agg_dict)
                    .sort_values("leads", ascending=False)
                )
            else:
                df_utm_display = pd.DataFrame(columns=["utm_source", "leads", "prequals", "quals"])
            if not df_utm_display.empty:
                mask_no_empty = ~df_utm_display["utm_source"].astype(str).str.contains("без utm", case=False, na=False)
                df_utm_display = df_utm_display[mask_no_empty].copy()
            if not df_utm_display.empty:
                utm_rename = {"utm_source": "UTM source", "leads": "Лиды", "prequals": "Предквалы", "quals": "Квалы"}
                df_utm_show = df_utm_display.rename(columns=utm_rename)
                utm_show_cols = [c for c in ["UTM source", "Лиды", "Предквалы", "Квалы"] if c in df_utm_show.columns]
                st.dataframe(df_utm_show[utm_show_cols], use_container_width=True, hide_index=True)
                df_utm_top = df_utm_show.head(12).copy()
                utm_chart_cols = [c for c in ["Лиды", "Предквалы", "Квалы"] if c in df_utm_top.columns]
                fig_u = px.bar(
                    df_utm_top,
                    x="UTM source",
                    y=utm_chart_cols,
                    barmode="group",
                    color_discrete_sequence=["#3B82F6", "#8B5CF6", "#10B981"],
                    labels={"value": "Кол-во", "variable": "Метрика"},
                )
                _apply_bar_rounded(fig_u)
                fig_u.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(30,41,59,0.5)",
                    font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                    xaxis_tickangle=-45,
                    xaxis=dict(gridcolor="#334155", fixedrange=True),
                    yaxis=dict(gridcolor="#334155", fixedrange=True),
                    legend=dict(bgcolor="rgba(30,41,59,0.8)", font=dict(size=10)),
                    margin=dict(t=24, b=36, l=18, r=18),
                    dragmode=False,
                    uirevision="tab_u",
                    hoverlabel=PLOTLY_HOVERLABEL,
                )
                st.plotly_chart(fig_u, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Нет данных по UTM.")
        with tab_formname:
            df_fn = loaded.get("by_formname")
            if df_fn is None:
                df_fn = pd.DataFrame()
            if df_fn is not None and not df_fn.empty:
                fn_rename = {
                    "formname": "Форма",
                    "leads": "Лиды",
                    "prequals": "Предквалы",
                    "quals": "Квалы",
                    "pokaz_naznachen": "Показ назначен",
                    "pokaz_proveden": "Показ проведён",
                    "passports": "Паспорта",
                    "broni": "Брони",
                }
                df_fn_display = df_fn.rename(columns=fn_rename)
                st.dataframe(df_fn_display, use_container_width=True, hide_index=True)
                fn_chart_cols = [c for c in ["Лиды", "Предквалы", "Квалы", "Показ назначен", "Показ проведён", "Паспорта", "Брони"] if c in df_fn_display.columns]
                fig_f = px.bar(
                    df_fn_display.head(15),
                    x="Форма",
                    y=fn_chart_cols,
                    barmode="group",
                    color_discrete_sequence=["#3B82F6", "#8B5CF6", "#10B981", "#F59E0B", "#14B8A6", "#6366F1", "#EC4899"],
                    labels={"value": "Кол-во", "variable": "Метрика"},
                )
                _apply_bar_rounded(fig_f)
                fig_f.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(30,41,59,0.5)",
                    font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                    xaxis_tickangle=-45,
                    xaxis=dict(gridcolor="#334155", fixedrange=True),
                    yaxis=dict(gridcolor="#334155", fixedrange=True),
                    legend=dict(bgcolor="rgba(30,41,59,0.8)", font=dict(size=10)),
                    margin=dict(t=24, b=36, l=18, r=18),
                    dragmode=False,
                    uirevision="tab_f",
                    hoverlabel=PLOTLY_HOVERLABEL,
                )
                st.plotly_chart(fig_f, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Нет данных по formname.")
        with tab_landing:
            df_land = loaded.get("landing")
            if df_land is None:
                df_land = pd.DataFrame()
            if df_land is not None and not df_land.empty:
                df_display = df_land.rename(
                    columns={
                        "landing": "Посадка",
                        "leads": "Лиды",
                        "prequals": "Предквалы",
                        "quals": "Квалы",
                        "pokaz_naznachen": "Показ назначен",
                        "pokaz_proveden": "Показ проведён",
                        "passports": "Паспорт получен",
                        "broni": "Объект забронирован",
                    }
                )
                st.dataframe(df_display, use_container_width=True, hide_index=True)
                df_chart = df_display.head(12).copy()
                df_chart["Посадка (коротко)"] = (
                    df_chart["Посадка"].astype(str).str.replace(r"^https?://", "", regex=True).str[:40]
                )
                fig_land = px.bar(
                    df_chart,
                    x="Посадка (коротко)",
                    y=[
                        "Лиды",
                        "Предквалы",
                        "Квалы",
                        "Показ назначен",
                        "Показ проведён",
                        "Паспорт получен",
                        "Объект забронирован",
                    ],
                    barmode="group",
                    color_discrete_sequence=["#3B82F6", "#8B5CF6", "#10B981", "#F59E0B", "#14B8A6", "#6366F1", "#EC4899"],
                    labels={"value": "Кол-во", "variable": "Метрика"},
                )
                _apply_bar_rounded(fig_land)
                fig_land.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(30,41,59,0.5)",
                    font=dict(color="#F1F5F9", family="'Space Grotesk', sans-serif"),
                    xaxis_tickangle=-45,
                    xaxis=dict(gridcolor="#334155", fixedrange=True),
                    yaxis=dict(gridcolor="#334155", fixedrange=True),
                    legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                    dragmode=False,
                    uirevision="tab_landing",
                    hoverlabel=PLOTLY_HOVERLABEL,
                )
                st.plotly_chart(fig_land, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Нет данных по посадочным (проверьте наличие utm_referrer/referrer в данных).")
    except Exception as e:
        st.error(str(e))

    print(f"[TIMING] ИТОГО конец отрисовки дашборда: {time.time() - t0:.2f}s")

def main():
    st.title("Эстадель — Аналитика")
    tab_dashboard, tab_ai = st.tabs(["Дашборд", "ИИ-аналитик"])
    with tab_dashboard:
        _run_dashboard()
    with tab_ai:
        _render_ai_analyst_tab()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("Ошибка дашборда")
        st.exception(e)
