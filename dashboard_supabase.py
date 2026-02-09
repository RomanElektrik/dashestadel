# -*- coding: utf-8 -*-
"""
Дашборд Эстадель на данных Supabase (таблица "For dash").
Подключение: переменная окружения SUPABASE_DB_URL (Connection string из Supabase → Database).
Запуск: streamlit run dashboard_supabase.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dashboard_supabase_data import (
    get_engine,
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
    top_managers,
    deal_stages,
    rejection_reasons,
)

st.set_page_config(
    page_title="Эстадель — Аналитика",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Тёмная тема по концепции
st.markdown("""
<style>
    .stApp { background: #0F172A; }
    .main .block-container { padding: 2rem 2.5rem; max-width: 100%; }
    * { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }
    h1 { color: #FFFFFF !important; font-weight: 700; font-size: 2rem; margin-bottom: 0.5rem !important; }
    h2 { color: #F1F5F9 !important; font-weight: 600; font-size: 1.4rem; margin-top: 2rem !important; margin-bottom: 1rem !important; }
    h3 { color: #94A3B8 !important; font-weight: 600; font-size: 1.1rem; margin-top: 1.5rem !important; margin-bottom: 0.75rem !important; }
    [data-testid="stMetric"] {
        background: #1E293B;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1.5rem 1.5rem;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
        transition: all 0.2s ease;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.6);
    }
    [data-testid="stMetricValue"] { color: #3B82F6 !important; font-size: 2.25rem !important; font-weight: 700 !important; }
    [data-testid="stMetricLabel"] { color: #94A3B8 !important; font-size: 0.875rem !important; text-transform: uppercase; letter-spacing: 0.5px; }
    [data-testid="stMetricDelta"] { font-size: 0.875rem !important; }
    [data-testid="stSidebar"] { background: #1E293B; border-right: 1px solid #334155; }
    [data-testid="stSidebar"] label { color: #F1F5F9 !important; font-weight: 500; }
    .stDataFrame { border: 1px solid #334155 !important; border-radius: 8px; }
    .stDataFrame th { background: #1E293B !important; color: #3B82F6 !important; font-weight: 600; }
    .stDataFrame td { background: #0F172A !important; color: #F1F5F9 !important; }
    div[data-testid="stVerticalBlock"] > div { background: transparent !important; }
    .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"],
    [data-testid="stPlotlyChart"] .plotly-graph-div, [data-testid="stPlotlyChart"] svg,
    [data-testid="stPlotlyChart"] .svg-container, [data-testid="stPlotlyChart"] * { cursor: default !important; }
    a, button, [role="button"], .stSelectbox > div { cursor: pointer !important; }
    .stTabs [data-baseweb="tab-list"] { background: #1E293B; border-radius: 8px; padding: 0.25rem; }
    .stTabs [data-baseweb="tab"] { color: #94A3B8; border-radius: 6px; }
    .stTabs [aria-selected="true"] { background: #3B82F6 !important; color: #FFFFFF !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "doubleClick": False,
    "showTips": False,
    "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
}


@st.cache_data(ttl=600)
def _cached_bounds():
    engine = _engine()
    return date_bounds(engine) if engine is not None else None

@st.cache_data(ttl=600)
def _cached_regions():
    engine = _engine()
    return region_list(engine) if engine is not None else []

@st.cache_data(ttl=180)
def _cached_kpi(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return None
    if region_list and len(region_list) > 1:
        return kpi_by_region(engine, date_from_str, date_to_str, list(region_list))
    return kpi_extended(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if region_list and len(region_list) == 1 else None)

@st.cache_data(ttl=180)
def _cached_funnel(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return None
    if region_list and len(region_list) > 1:
        return funnel_by_region(engine, date_from_str, date_to_str, list(region_list))
    return funnel_data(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if region_list and len(region_list) == 1 else None)

@st.cache_data(ttl=180)
def _cached_managers(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return top_managers(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=10)

@st.cache_data(ttl=180)
def _cached_daily(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return None
    if region_list and len(region_list) > 1:
        return daily_series_by_region(engine, date_from_str, date_to_str, list(region_list))
    return daily_series(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list if region_list and len(region_list) == 1 else None)

@st.cache_data(ttl=180)
def _cached_by_region(date_from_str, date_to_str):
    engine = _engine()
    return by_region(engine, date_from_str, date_to_str) if engine else pd.DataFrame()

@st.cache_data(ttl=180)
def _cached_by_utm(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return by_utm(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=25)

@st.cache_data(ttl=180)
def _cached_by_formname(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return by_formname(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=30)

@st.cache_data(ttl=180)
def _cached_by_landing(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return by_landing(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=30)

@st.cache_data(ttl=180)
def _cached_deal_stages(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return deal_stages(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=12)

@st.cache_data(ttl=180)
def _cached_reject_reasons(date_from_str, date_to_str, region_key=None, region_list=None):
    engine = _engine()
    if not engine:
        return pd.DataFrame()
    return rejection_reasons(engine, date_from_str, date_to_str, region=region_key if region_key and region_key != "Все" else None, region_list=region_list, limit=15)

@st.cache_resource
def _engine():
    # Один engine на все перерисовки приложения (ускоряет загрузку)
    return get_engine()


def main():
    st.title("Эстадель — Аналитика")
    st.caption("Данные из Supabase · таблица «For dash»")

    engine = _engine()
    if engine is None:
        st.error(
            "Не задано подключение к базе. Укажи переменную окружения **SUPABASE_DB_URL** "
            "(Connection string из Supabase → Project Settings → Database)."
        )
        st.code(
            'export SUPABASE_DB_URL="postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres"',
            language="bash",
        )
        return

    try:
        with st.spinner("Загрузка диапазона дат…"):
            bounds = _cached_bounds()
        if bounds is None or bounds.empty or pd.isna(bounds["min_d"].iloc[0]):
            st.warning("В таблице «For dash» нет данных по датам.")
            return
        min_d = bounds["min_d"].iloc[0]
        max_d = bounds["max_d"].iloc[0]
        if hasattr(min_d, "date"):
            min_d = min_d.date()
        if hasattr(max_d, "date"):
            max_d = max_d.date()
    except Exception as e:
        err_text = str(e).lower()
        st.error(f"Ошибка подключения к базе: {e}")
        if "255.255.255.255" in err_text or "address family" in err_text:
            st.info(
                "**Подключение по хосту db.xxx.supabase.co часто не работает.** Используй **Connection pooler**: "
                "Supabase → Project Settings → Database → URI с хостом `aws-1-eu-north-1.pooler.supabase.com` и вставь в `.env` как `SUPABASE_DB_URL`."
            )
        elif "timed out" in err_text or "timeout" in err_text:
            st.info(
                "**Таймаут соединения** — до сервера не доходят пакеты. Проверь: 1) В `.env` указан pooler (хост `aws-1-eu-north-1.pooler.supabase.com`). "
                "2) Попробуй в URI порт **6543** вместо 5432 (в конце строки: `:6543/postgres`). "
                "3) Другая сеть или отключи VPN. 4) Файрвол не блокирует исходящий порт 5432/6543."
            )
        return

    # Сайдбар
    with st.sidebar:
        st.header("Период и фильтры")
        default_start = max(min_d, (max_d - timedelta(days=30)) if (max_d - min_d).days > 30 else min_d)
        if hasattr(default_start, "isoformat"):
            default_start = default_start
        else:
            default_start = min_d
        date_from = st.date_input("Дата от", value=default_start, min_value=min_d, max_value=max_d)
        date_to = st.date_input("Дата до", value=max_d, min_value=min_d, max_value=max_d)
        if date_from > date_to:
            st.warning("«Дата от» должна быть не позже «Дата до».")
            date_to = date_from

        # Только направления: Крым, Сочи, Анапа, Баку
        ALLOWED_REGIONS = ["Крым", "Сочи", "Анапа", "Баку"]
        try:
            regions = _cached_regions()
        except Exception:
            regions = []
        region_options = [r for r in ALLOWED_REGIONS if r in regions or not regions]
        if not region_options:
            region_options = ALLOWED_REGIONS
        selected_regions = st.multiselect(
            "Направления (несколько — сравнение на графиках)",
            options=region_options,
            default=[],
            help="Крым, Сочи, Анапа, Баку. При нескольких выбранных графики покажут сравнение по направлениям.",
        )
        if not selected_regions:
            region_key, region_list = "Все", None
        elif len(selected_regions) == 1:
            region_key, region_list = selected_regions[0], None
        else:
            region_key, region_list = None, tuple(selected_regions)
        compare_mode = region_list is not None and len(region_list) > 1

        st.divider()
        st.caption("Разбивки: по региону, UTM, formname, посадочная")

    date_from_str = date_from.isoformat() if hasattr(date_from, "isoformat") else str(date_from)
    date_to_str = date_to.isoformat() if hasattr(date_to, "isoformat") else str(date_to)

    # KPI за период
    try:
        with st.spinner("Считаю KPI…"):
            kpi = _cached_kpi(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
        if kpi is None:
            st.error("Не удалось загрузить KPI.")
            return
    except Exception as e:
        st.error(f"Ошибка запроса KPI: {e}")
        return

    if kpi.empty:
        st.warning("Нет данных за выбранный период.")
        return

    compare_mode = region_list is not None and len(region_list) > 1
    CHART_HEIGHT = 520

    if compare_mode:
        st.subheader(f"KPI за период · {date_from_str} — {date_to_str} · сравнение регионов")
        kpi_display = kpi.copy()
        kpi_display["Конв. Лид→Квал %"] = (100 * kpi_display["quals"] / kpi_display["leads"].replace(0, pd.NA)).round(1)
        kpi_display["Конв. Квал→Сделка %"] = (100 * kpi_display["sdelki"] / kpi_display["quals"].replace(0, pd.NA)).round(1)
        st.dataframe(kpi_display, use_container_width=True, hide_index=True)
    else:
        row = kpi.iloc[0]
        leads = int(row["leads"] or 0)
        quals = int(row["quals"] or 0)
        prequals = int(row["prequals"] or 0)
        passports = int(row["passports"] or 0)
        pokaz = int(row["pokaz"] or 0)
        broni = int(row["broni"] or 0)
        sdelki = int(row["sdelki"] or 0)
        kommissii = float(row["komissi"] or 0)

        prev_date_from = prev_date_to = None
        try:
            days_diff = (date_to - date_from).days + 1
            prev_date_from = date_from - timedelta(days=days_diff)
            prev_date_to = date_from - timedelta(days=1)
            prev_kpi = _cached_kpi(prev_date_from.isoformat(), prev_date_to.isoformat(), region_key=region_key, region_list=region_list)
            if prev_kpi is not None and not prev_kpi.empty:
                prev_row = prev_kpi.iloc[0]
                delta_leads = leads - int(prev_row["leads"] or 0)
                delta_quals = quals - int(prev_row["quals"] or 0)
                delta_prequals = prequals - int(prev_row["prequals"] or 0)
                delta_sdelki = sdelki - int(prev_row["sdelki"] or 0)
                delta_pokaz = pokaz - int(prev_row["pokaz"] or 0)
                delta_broni = broni - int(prev_row["broni"] or 0)
                delta_komm = kommissii - float(prev_row["komissi"] or 0)
            else:
                delta_leads = delta_quals = delta_prequals = delta_sdelki = delta_pokaz = delta_broni = delta_komm = None
        except Exception:
            delta_leads = delta_quals = delta_prequals = delta_sdelki = delta_pokaz = delta_broni = delta_komm = None

        st.subheader(f"KPI за период · {date_from_str} — {date_to_str}" + (f" · {region_key}" if region_key and region_key != "Все" else ""))
        if prev_date_from is not None and prev_date_to is not None and (delta_leads is not None or delta_quals is not None):
            st.caption("↑/↓ — изменение к **предыдущему периоду той же длины**: " + prev_date_from.strftime("%Y-%m-%d") + " — " + prev_date_to.strftime("%Y-%m-%d"))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Лиды", f"{leads:,}", delta=delta_leads)
        c2.metric("Предквалы", f"{prequals:,}", delta=delta_prequals)
        c3.metric("Квалы", f"{quals:,}", delta=delta_quals)
        c4.metric("Сделки", f"{sdelki:,}", delta=delta_sdelki)
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Показы", f"{pokaz:,}", delta=delta_pokaz)
        c6.metric("Брони", f"{broni:,}", delta=delta_broni)
        conv_lead_qual = round(100 * quals / leads, 1) if leads > 0 else 0
        conv_qual_sdelka = round(100 * sdelki / quals, 1) if quals > 0 else 0
        c7.metric("Конверсия Лид→Квал", f"{conv_lead_qual}%")
        komm_str = f"{kommissii / 1_000_000:.1f}М ₽" if kommissii >= 1_000_000 else (f"{kommissii / 1_000:.0f}К ₽" if kommissii >= 1_000 else f"{kommissii:.0f} ₽")
        delta_komm_str = f"{delta_komm / 1_000:.0f}К ₽" if delta_komm is not None and abs(delta_komm) >= 1_000 and abs(delta_komm) < 1_000_000 else (f"{delta_komm / 1_000_000:.1f}М ₽" if delta_komm is not None and abs(delta_komm) >= 1_000_000 else None)
        c8.metric("Комиссии", komm_str, delta=delta_komm_str)

    st.divider()

    st.subheader("Воронка продаж")
    try:
        with st.spinner("Строю воронку…"):
            funnel = _cached_funnel(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
        if funnel is not None and not funnel.empty:
            if compare_mode and "region" in funnel.columns and funnel.shape[0] > 1:
                stages = ["Лиды", "Предквалы", "Квалы", "Показы", "Брони", "Сделки"]
                cols = ["leads", "prequals", "quals", "pokaz", "broni", "sdelki"]
                long = []
                for _, r in funnel.iterrows():
                    reg = r["region"]
                    for s, c in zip(stages, cols):
                        long.append({"Этап": s, "Регион": reg, "Кол-во": int(r.get(c) or 0)})
                df_long = pd.DataFrame(long)
                fig_funnel = px.bar(df_long, x="Этап", y="Кол-во", color="Регион", barmode="group",
                    category_orders={"Этап": stages}, color_discrete_sequence=px.colors.qualitative.Set2)
                fig_funnel.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9"),
                    xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True), legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                    margin=dict(t=30, b=50), height=CHART_HEIGHT, dragmode=False, uirevision="funnel")
                st.plotly_chart(fig_funnel, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                f_row = funnel.iloc[0]
                f_leads = int(f_row["leads"] or 0)
                f_prequals = int(f_row["prequals"] or 0)
                f_quals = int(f_row["quals"] or 0)
                f_pokaz = int(f_row["pokaz"] or 0)
                f_broni = int(f_row["broni"] or 0)
                f_sdelki = int(f_row["sdelki"] or 0)
                stages = ["Лиды", "Предквалы", "Квалы", "Показы", "Брони", "Сделки"]
                values = [f_leads, f_prequals, f_quals, f_pokaz, f_broni, f_sdelki]
                colors = ["#3B82F6", "#8B5CF6", "#10B981", "#F59E0B", "#EF4444", "#14B8A6"]
                fig_funnel = go.Figure(go.Funnel(y=stages, x=values, textinfo="value+percent initial", marker={"color": colors, "line": {"color": "#334155", "width": 1}}))
                fig_funnel.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9", size=11), margin=dict(t=10, b=10, l=10, r=10), height=CHART_HEIGHT, dragmode=False, uirevision="funnel2")
                st.plotly_chart(fig_funnel, use_container_width=True, config=PLOTLY_CONFIG)
                if not compare_mode and leads > 0 and quals > 0:
                    st.caption(f"Лид → Квал: **{round(100 * quals / leads, 1)}%** · Квал → Сделка: **{round(100 * sdelki / quals, 1)}%**")
        else:
            st.info("Нет данных для воронки.")
    except Exception as e:
        st.error(f"Ошибка воронки: {e}")

    st.subheader("Динамика по дням")
    try:
        with st.spinner("График по дням…"):
            daily = _cached_daily(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
        if daily is None:
            daily = pd.DataFrame()
    except Exception as e:
        daily = pd.DataFrame()
        st.warning(f"График по дням: {e}")

    if not daily.empty:
        daily["date_str"] = pd.to_datetime(daily["date"]).astype(str).str[:10]
        if compare_mode and "region" in daily.columns:
            fig = px.line(daily, x="date_str", y="leads", color="region", markers=True,
                labels={"date_str": "Дата", "leads": "Лиды", "region": "Регион"})
            fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9"),
                xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True),
                legend=dict(bgcolor="rgba(30,41,59,0.8)"), margin=dict(t=50, b=20), hovermode="x unified", height=CHART_HEIGHT, dragmode=False, uirevision="daily")
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(name="Лиды", x=daily["date_str"], y=daily["leads"], mode="lines+markers", line=dict(color="#3B82F6", width=3), marker=dict(size=6)))
            fig.add_trace(go.Scatter(name="Предквалы", x=daily["date_str"], y=daily["prequals"], mode="lines+markers", line=dict(color="#8B5CF6", width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(name="Квалы", x=daily["date_str"], y=daily["quals"], mode="lines+markers", line=dict(color="#10B981", width=3), marker=dict(size=6)))
            fig.add_trace(go.Scatter(name="Паспорта", x=daily["date_str"], y=daily["passports"], mode="lines+markers", line=dict(color="#14B8A6", width=2), marker=dict(size=5)))
            fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9"),
                xaxis=dict(gridcolor="#334155", showspikes=False, showgrid=True, fixedrange=True),
                yaxis=dict(gridcolor="#334155", showspikes=False, showgrid=True, fixedrange=True),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, bgcolor="rgba(30,41,59,0.8)"),
                margin=dict(t=50, b=20, l=20, r=20), hovermode="x unified", height=CHART_HEIGHT, dragmode=False, uirevision="daily2")
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("Нет данных по дням.")

    st.divider()

    st.subheader("Топ брокеров по квалам")
    try:
        managers = _cached_managers(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
        if not managers.empty:
            managers = managers.reset_index(drop=True)
            managers.index = managers.index + 1
            managers["№"] = managers.index.map(lambda x: str(x))
            
            # Переименовываем колонки для красоты
            managers_display = managers[["№", "broker_name", "leads", "quals", "sdelki", "conv_percent"]].copy()
            managers_display.columns = ["№", "Брокер", "Лиды", "Квалы", "Сделки", "Конв. %"]
            
            st.dataframe(managers_display, use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Нет данных по менеджерам за этот период.")
    except Exception as e:
        st.error(f"Ошибка загрузки менеджеров: {e}")
    
    st.divider()

    st.subheader("Этапы сделок (по статусу) за период")
    try:
        df_stage = _cached_deal_stages(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
    except Exception as e:
        df_stage = pd.DataFrame()
        st.error(f"Этапы сделок: {e}")
    if not df_stage.empty:
        total = float(df_stage["cnt"].sum())
        df_stage = df_stage.copy()
        df_stage["percent"] = (df_stage["cnt"] / total * 100).round(1) if total > 0 else 0
        fig_stage = go.Figure(go.Funnel(y=df_stage["stage"], x=df_stage["cnt"], textinfo="value+percent initial", marker={"color": "#3B82F6", "line": {"color": "#334155", "width": 1}}))
        fig_stage.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9", size=11), margin=dict(t=10, b=10, l=10, r=10), height=CHART_HEIGHT, dragmode=False, uirevision="stages")
        st.plotly_chart(fig_stage, use_container_width=True, config=PLOTLY_CONFIG)
        st.dataframe(df_stage, use_container_width=True, hide_index=True, height=280)
    else:
        st.info("Нет данных по этапам за период.")

    st.subheader("Причины отказа за период")
    try:
        df_rej = _cached_reject_reasons(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
    except Exception as e:
        df_rej = pd.DataFrame()
        st.error(f"Причины отказа: {e}")
    if not df_rej.empty:
        total = float(df_rej["cnt"].sum())
        df_rej = df_rej.copy()
        df_rej["percent"] = (df_rej["cnt"] / total * 100).round(1) if total > 0 else 0
        fig_rej = px.bar(df_rej, x="reason", y="cnt", text="percent", labels={"reason": "Причина", "cnt": "Количество"}, color_discrete_sequence=["#EF4444"])
        fig_rej.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
        fig_rej.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(30,41,59,0.5)", font=dict(color="#F1F5F9"),
            xaxis_tickangle=-35, xaxis=dict(gridcolor="#334155", fixedrange=True), yaxis=dict(gridcolor="#334155", fixedrange=True),
            margin=dict(t=30, b=70, l=20, r=20), height=CHART_HEIGHT, showlegend=False, dragmode=False, uirevision="reasons")
        st.plotly_chart(fig_rej, use_container_width=True, config=PLOTLY_CONFIG)
        st.dataframe(df_rej, use_container_width=True, hide_index=True, height=280)
    else:
        st.info("Нет данных по причинам отказа за период.")
    
    # Разбивки в табах
    st.subheader("Детальные разбивки")
    tab_region, tab_utm, tab_formname, tab_landing = st.tabs(["По региону", "По UTM", "По formname", "По посадочной (UTM referrer)"])

    with tab_region:
        if st.button("Загрузить разбивку", key="load_region"):
            st.session_state["show_region"] = True
        if not st.session_state.get("show_region"):
            st.info("Нажми «Загрузить разбивку», чтобы построить таблицу и график.")
        else:
            try:
                df_region = _cached_by_region(date_from_str, date_to_str)
                if not df_region.empty:
                    st.dataframe(df_region, use_container_width=True, hide_index=True)
                    fig_r = px.bar(
                        df_region.head(15),
                        x="region",
                        y=["leads", "quals", "prequals", "passports"],
                        barmode="group",
                        color_discrete_sequence=["#3B82F6", "#10B981", "#8B5CF6", "#14B8A6"],
                        labels={"value": "Кол-во", "region": "Регион", "variable": "Метрика"},
                    )
                    fig_r.update_layout(
                        template="plotly_dark", 
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(30,41,59,0.5)", 
                        font=dict(color="#F1F5F9"),
                        xaxis=dict(gridcolor="#334155", fixedrange=True),
                        yaxis=dict(gridcolor="#334155", fixedrange=True),
                        legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                        dragmode=False, uirevision="tab_r"
                    )
                    st.plotly_chart(fig_r, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных по регионам.")
            except Exception as e:
                st.error(str(e))

    with tab_utm:
        if st.button("Загрузить разбивку", key="load_utm"):
            st.session_state["show_utm"] = True
        if not st.session_state.get("show_utm"):
            st.info("Нажми «Загрузить разбивку», чтобы построить таблицу и график.")
        else:
            try:
                df_utm = _cached_by_utm(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
                if not df_utm.empty:
                    st.dataframe(df_utm, use_container_width=True, hide_index=True)
                    df_utm_top = df_utm.head(12).copy()
                    df_utm_top["utm"] = df_utm_top["utm_source"] + " / " + df_utm_top["utm_medium"] + " / " + df_utm_top["utm_campaign"]
                    fig_u = px.bar(df_utm_top, x="utm", y=["leads", "quals"], barmode="group", color_discrete_sequence=["#3B82F6", "#10B981"])
                    fig_u.update_layout(
                        template="plotly_dark", 
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(30,41,59,0.5)", 
                        font=dict(color="#F1F5F9"), 
                        xaxis_tickangle=-45,
                        xaxis=dict(gridcolor="#334155", fixedrange=True),
                        yaxis=dict(gridcolor="#334155", fixedrange=True),
                        legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                        dragmode=False, uirevision="tab_u"
                    )
                    st.plotly_chart(fig_u, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных по UTM.")
            except Exception as e:
                st.error(str(e))

    with tab_formname:
        if st.button("Загрузить разбивку", key="load_formname"):
            st.session_state["show_formname"] = True
        if not st.session_state.get("show_formname"):
            st.info("Нажми «Загрузить разбивку», чтобы построить таблицу и график.")
        else:
            try:
                df_fn = _cached_by_formname(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
                if not df_fn.empty:
                    st.dataframe(df_fn, use_container_width=True, hide_index=True)
                    fig_f = px.bar(
                        df_fn.head(15),
                        x="formname",
                        y=["leads", "quals", "prequals", "passports"],
                        barmode="group",
                        color_discrete_sequence=["#3B82F6", "#10B981", "#8B5CF6", "#14B8A6"],
                    )
                    fig_f.update_layout(
                        template="plotly_dark", 
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(30,41,59,0.5)", 
                        font=dict(color="#F1F5F9"), 
                        xaxis_tickangle=-45,
                        xaxis=dict(gridcolor="#334155", fixedrange=True),
                        yaxis=dict(gridcolor="#334155", fixedrange=True),
                        legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                        dragmode=False, uirevision="tab_f"
                    )
                    st.plotly_chart(fig_f, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных по formname.")
            except Exception as e:
                st.error(str(e))

    with tab_landing:
        if st.button("Загрузить разбивку", key="load_landing"):
            st.session_state["show_landing"] = True
        if not st.session_state.get("show_landing"):
            st.info("Нажми «Загрузить разбивку», чтобы увидеть статистику по посадочным (лиды, предквалы, квалы, показ назначен/проведён, паспорт, бронь).")
        else:
            try:
                df_land = _cached_by_landing(date_from_str, date_to_str, region_key=region_key, region_list=region_list)
                if not df_land.empty:
                    df_display = df_land.rename(columns={
                        "landing": "Посадка",
                        "leads": "Лиды",
                        "prequals": "Предквалы",
                        "quals": "Квалы",
                        "pokaz_naznachen": "Показ назначен",
                        "pokaz_proveden": "Показ проведён",
                        "passports": "Паспорт получен",
                        "broni": "Объект забронирован",
                    })
                    st.dataframe(df_display, use_container_width=True, hide_index=True)
                    df_chart = df_land.head(12).copy()
                    df_chart["Посадка (коротко)"] = df_chart["landing"].str.replace(r"^https?://", "", regex=True).str[:40]
                    df_chart["Лиды"] = df_chart["leads"]
                    df_chart["Предквалы"] = df_chart["prequals"]
                    df_chart["Квалы"] = df_chart["quals"]
                    df_chart["Показ назначен"] = df_chart["pokaz_naznachen"]
                    df_chart["Показ проведён"] = df_chart["pokaz_proveden"]
                    df_chart["Паспорт получен"] = df_chart["passports"]
                    df_chart["Объект забронирован"] = df_chart["broni"]
                    fig_land = px.bar(
                        df_chart,
                        x="Посадка (коротко)",
                        y=["Лиды", "Предквалы", "Квалы", "Показ назначен", "Показ проведён", "Паспорт получен", "Объект забронирован"],
                        barmode="group",
                        color_discrete_sequence=["#3B82F6", "#8B5CF6", "#10B981", "#F59E0B", "#14B8A6", "#6366F1", "#EC4899"],
                        labels={"value": "Кол-во"},
                    )
                    fig_land.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(30,41,59,0.5)",
                        font=dict(color="#F1F5F9"),
                        xaxis_tickangle=-45,
                        xaxis=dict(gridcolor="#334155", fixedrange=True),
                        yaxis=dict(gridcolor="#334155", fixedrange=True),
                        legend=dict(bgcolor="rgba(30,41,59,0.8)"),
                        dragmode=False,
                        uirevision="tab_landing",
                    )
                    st.plotly_chart(fig_land, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Нет данных по посадочным (проверьте наличие utm_referrer/referrer в данных).")
            except Exception as e:
                st.error(str(e))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("Ошибка дашборда")
        st.exception(e)
