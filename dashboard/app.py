"""MetropoliaCity dashboard entry point.

Run from the project root (so .streamlit/config.toml is picked up):
    streamlit run dashboard/app.py

Each feature is its own file under dashboard/views/; adding a feature means
adding a page here, not editing the others. The folder is deliberately NOT
named pages/: Streamlit treats pages/ as its legacy multipage folder, and on a
direct link such as /polusi it then runs that file without app.py first,
so the imports and the styling below are missing.
"""
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # `import dashboard.*` from pages

st.set_page_config(page_title="MetropoliaCity", layout="wide", initial_sidebar_state="expanded")

from dashboard.theme import apply_style  # noqa: E402

apply_style()

pages = {
    "Operasional": [
        st.Page("views/peta.py", title="Peta lalu lintas", icon=":material/map:", default=True),
        st.Page("views/insiden.py", title="Insiden", icon=":material/warning:"),
        st.Page("views/streaming.py", title="Streaming langsung", icon=":material/stream:"),
    ],
    "Analisis": [
        st.Page("views/tren.py", title="Tren kepadatan", icon=":material/insights:"),
    ],
    "Lingkungan": [
        st.Page("views/polusi.py", title="Polusi udara", icon=":material/air:"),
    ],
    "Sistem": [
        st.Page("views/kualitas.py", title="Kualitas data", icon=":material/fact_check:"),
    ],
}
st.navigation(pages).run()
