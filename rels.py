import streamlit as st
import pandas as pd
import requests
import io
import re
from dateutil.parser import parse as date_parse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import math
import datetime
from docx import Document
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor
import base64

# ===========================================================
# 🔧 CONFIGURAÇÃO FIXA — ALTERE SÓ AQUI
# ===========================================================
HEAD_URL = "https://github.com/larissafeitosa24/neoenergia-auditoria-qna/relatorios.csv"
FIND_URL = "https://github.com/larissafeitosa24/neoenergia-auditoria-qna/constatacoes.csv"
LOGO_URL = "https://github.com/larissafeitosa24/neoenergia-auditoria-qna/neo_logo.png"
# ===========================================================

NEO_GREEN = "#7CC04B"
NEO_BLUE = "#0060A9"
NEO_DARK = "#014e87"

st.set_page_config(page_title="Neoenergia • Q&A Enterprise", page_icon="📗", layout="wide")

CSS = f"""
<style>
html, body, [class*="css"] {{
  font-family: Segoe UI, SegoeUI, Helvetica, Arial, sans-serif;
}}
h1, h2, h3 {{
  color: {NEO_BLUE};
}}
.stButton>button {{
  background-color: {NEO_BLUE};
  color: white;
  border-radius: 6px;
}}
.stButton>button:hover {{
  background-color: {NEO_DARK};
}}
.source {{
  border-left: 4px solid {NEO_BLUE};
  background: #f0f7ff;
  padding: 8px;
  margin: 6px 0;
}}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ===========================================================
# Funções utilitárias
# ===========================================================
def load_csv(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def load_logo(url):
    try:
        r = requests.get(url, timeout=15)
        return r.content
    except:
        return None

def to_iso(v):
    try:
        return date_parse(str(v), dayfirst=True).date().isoformat()
    except:
        return ""

def chunk(s, max_chars=1000):
    if not isinstance(s, str): return []
    if len(s) <= max_chars: return [s]
    parts = re.split(r"(?<=[.!?]) ", s)
    buf = ""
    out = []
    for p in parts:
        if len(buf) + len(p) < max_chars:
            buf += p + " "
        else:
            out.append(buf.strip())
            buf = p
    out.append(buf.strip())
    return out

def build_corpus(dfh, dff):
    rows = []

    # HEAD
    for _, r in dfh.iterrows():
        aud = str(r["aud_code"])
        title = r.get("title", "")
        objetivo = r.get("objetivo", "")
        escopo = r.get("escopo", "")
        risco = r.get("risco_processo", "")
        alc = r.get("alcance", "")
        cronoi = to_iso(r.get("cronograma_inicio", ""))
        cronof = to_iso(r.get("cronograma_final", ""))

        text = "\n".join([
            f"[{aud}] {title}",
            f"Objetivo: {objetivo}",
            f"Escopo: {escopo}",
            f"Riscos: {risco}",
            f"Alcance: {alc}",
            f"Cronograma: início {cronoi} • fim {cronof}"
        ])

        for i, ch in enumerate(chunk(text)):
            rows.append({
                "source_type": "HEAD",
                "aud_code": aud,
                "finding_id": "",
                "text": ch
            })

    # FINDINGS
    for _, r in dff.iterrows():
        aud = str(r["aud_code"])
        fid = str(r.get("finding_id", ""))
        title = r.get("finding_title", "")
        rec = r.get("recommendation", "")
        imp = r.get("impact", "")
        status = r.get("status", "")
        owner = r.get("owner", "")
        due = to_iso(r.get("due_date", ""))
        ftext = r.get("finding_text", "")

        text = f"[{aud} – {fid}] Constatação: {title} — Impacto: {imp} — Recomendação: {rec} — Status: {status} — Resp.: {owner} — Prazo: {due}\n{ftext}"

        for i, ch in enumerate(chunk(text)):
            rows.append({
                "source_type": "FIND",
                "aud_code": aud,
                "finding_id": fid,
                "text": ch
            })

    return pd.DataFrame(rows)

def search_tf(question, corpus, top_k):
    vect = TfidfVectorizer(strip_accents="unicode", ngram_range=(1,2))
    M = vect.fit_transform(corpus["text"])
    qv = vect.transform([question])
    sim = cosine_similarity(qv, M).flatten()
    corpus["score"] = sim
    return corpus.sort_values("score", ascending=False).head(top_k)

def build_answer(contexts):
    heads = contexts[contexts["source_type"]=="HEAD"]
    finds = contexts[contexts["source_type"]=="FIND"]

    out = []

    if len(heads)>0:
        h = heads.iloc[0]
        out.append(f"**Resumo do relatório {h['aud_code']}:**")
        for line in h["text"].split("\n"):
            out.append(f"- {line}")
        out.append("")

    if len(finds)>0:
        out.append("**Constatações e Recomendações (principais):**")
        for _, r in finds.head(5).iterrows():
            tag = f"[{r['aud_code']} – {r['finding_id']}]"
            out.append(f"- {r['text'][:300]}... _(Fonte: {tag})_")

    return "\n".join(out)

# ===========================================================
# LOAD CSV AUTOMÁTICO
# ===========================================================
with st.spinner("Carregando dados do GitHub..."):
    df_h = load_csv(HEAD_URL)
    df_f = load_csv(FIND_URL)

df_h["aud_code"] = df_h["aud_code"].astype(str).str.upper()
df_f["aud_code"] = df_f["aud_code"].astype(str).str.upper()

corpus = build_corpus(df_h, df_f)

# ===========================================================
# Logo Neoenergia
# ===========================================================
logo_bytes = load_logo(LOGO_URL)
if logo_bytes:
    st.image(logo_bytes, width=180)

st.title("📗 Q&A Enterprise Neoenergia (100% Automático)")

# ===========================================================
# Filtros (locais)
# ===========================================================
st.subheader("🔎 Filtros")

cols = st.columns(4)
with cols[0]:
    f_aud = st.multiselect("AUD(s)", sorted(df_h["aud_code"].unique()))
with cols[1]:
    f_class = st.multiselect("Classificação", sorted(df_h["classification"].dropna().unique()))
with cols[2]:
    f_status = st.multiselect("Status (Findings)", sorted(df_f["status"].dropna().unique()))
with cols[3]:
    f_tema = st.multiselect("Tema", sorted(df_f["tema"].dropna().unique()))

filtered_corpus = corpus.copy()
if f_aud:
    filtered_corpus = filtered_corpus[filtered_corpus["aud_code"].isin(f_aud)]
if f_status:
    tmp = df_f[df_f["status"].isin(f_status)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[filtered_corpus["finding_id"].isin(tmp)]
if f_tema:
    tmp = df_f[df_f["tema"].isin(f_tema)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[filtered_corpus["finding_id"].isin(tmp)]

# ===========================================================
# Chat
# ===========================================================
st.subheader("💬 Pergunte sobre os relatórios")

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Digite sua pergunta...")

if q:
    results = search_tf(q, filtered_corpus, top_k=12)
    answer = build_answer(results)

    st.session_state["history"].append(("user", q))
    st.session_state["history"].append(("assistant", answer, results))

# Renderização
for msg in st.session_state["history"]:
    if msg[0] == "user":
        with st.chat_message("user"):
            st.write(msg[1])
    else:
        with st.chat_message("assistant"):
            st.write(msg[1])
            st.markdown("**Trechos utilizados:**")
            for _, r in msg[2].iterrows():
                tag = f"[{r['aud_code']} – {r['finding_id']}]"
                st.markdown(f"<div class='source'><b>{tag}</b><br>{r['text'][:500]}...</div>", unsafe_allow_html=True)

# ===========================================================
# Exportação PDF / Word
# ===========================================================
st.subheader("📤 Exportar")

def export_pdf(text):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    y = H-50

    if logo_bytes:
        c.drawImage(ImageReader(io.BytesIO(logo_bytes)), 40, y-40, width=150, height=40)
        y -= 60

    c.setFont("Helvetica-Bold", 14)
    c.setFillColor(HexColor(NEO_BLUE))
    c.drawString(40, y, "Neoenergia — Q&A de Relatórios")
    y -= 20

    c.setFont("Helvetica", 10)
    for line in text.split("\n"):
        if y < 50:
            c.showPage()
            y = H-50
        line = re.sub(r"\*\*|\_", "", line)
        c.drawString(40, y, line)
        y -= 14

    c.save()
    return buf.getvalue()

def export_docx(text):
    doc = Document()
    if logo_bytes:
        img = ImageReader(io.BytesIO(logo_bytes))
    doc.add_heading("Neoenergia — Q&A de Relatórios", level=1)
    for line in text.split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|\_", "", line))
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

last_answer = None
for msg in reversed(st.session_state["history"]):
    if msg[0] == "assistant":
        last_answer = msg[1]
        break

col1, col2 = st.columns(2)
with col1:
    if st.button("⬇️ Exportar PDF", disabled=(last_answer is None)):
        pdf = export_pdf(last_answer)
        st.download_button("Baixar PDF", pdf, "neoenergia_qa.pdf", mime="application/pdf")
with col2:
    if st.button("⬇️ Exportar Word", disabled=(last_answer is None)):
        docx = export_docx(last_answer)
        st.download_button("Baixar DOCX", docx, "neoenergia_qa.docx")
