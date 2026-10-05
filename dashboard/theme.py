"""Shared look and status rules, so every page classifies and colors the same way."""
from pathlib import Path

import streamlit as st
import yaml

DASHBOARD_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARD_DIR.parent

# Congestion status from avg_jam_density_index (scale 0-100). Fixed cut-offs
# rather than percentiles, so a segment's label means the same thing on every
# load; 75 sits near the 80th percentile and 50 near the median of the current
# mart.road_current_status.
JDI_MACET = 75.0
JDI_PADAT = 50.0

# Labels say what the index measures (density), not "macet/lancar": in
# traffictab23 jam_density_index is independent of average_speed (r ≈ 0, mean
# speed 60.0 km/h in every density group -- docs/keterbatasan.md), so a
# "macet" label would promise slow traffic the data does not show. The keys
# stay macet/padat/lancar so the rest of the code is unchanged.
STATUS = {
    "macet":  {"label": "Tinggi", "long": "Kepadatan tinggi", "color": "#C8322B", "bg": "#FBE9E7", "fg": "#A3241D", "weight": 6},
    "padat":  {"label": "Sedang", "long": "Kepadatan sedang", "color": "#E08A1E", "bg": "#FDF1E1", "fg": "#8A5208", "weight": 5},
    "lancar": {"label": "Rendah", "long": "Kepadatan rendah", "color": "#3B6EA8", "bg": "#E8EFF7", "fg": "#2C5A8C", "weight": 4},
}

ALERT = {  # alert_level / v2x_status values written by the pipeline
    "kritis":    ("#FBE9E7", "#A3241D"),
    "waspada":   ("#FDF1E1", "#8A5208"),
    "perhatian": ("#FFF7D6", "#6B5300"),
    "normal":    ("#EAF4EE", "#2E6B4A"),
}


def congestion_status(jdi) -> str:
    if jdi is None:
        return "lancar"
    if jdi >= JDI_MACET:
        return "macet"
    if jdi >= JDI_PADAT:
        return "padat"
    return "lancar"


@st.cache_data
def incident_types() -> dict:
    data = yaml.safe_load((PROJECT_ROOT / "config" / "incident_types.yaml").read_text(encoding="utf-8"))
    return {int(k): v for k, v in data["incident_types"].items()}


def pill(text: str, bg: str, fg: str) -> str:
    return (f'<span style="display:inline-block;padding:2px 9px;border-radius:999px;'
            f'background:{bg};color:{fg};font-size:12px;font-weight:500">{text}</span>')


def apply_style():
    css = (DASHBOARD_DIR / "style.css").read_text(encoding="utf-8")
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def fmt(x, digits=1) -> str:
    """Indonesian decimal comma."""
    return f"{x:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


BULAN = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]


def tgl(ts) -> str:
    """'31 Des 2023 20:00' -- Indonesian month names without depending on the OS locale."""
    return f"{ts.day} {BULAN[ts.month - 1]} {ts.year} {ts:%H:%M}"
