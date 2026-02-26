# -*- coding: utf-8 -*-
import io
import re
import math
import datetime
import unicodedata
import requests
import pandas as pd
import streamlit as st
from typing import Optional, List
from dateutil.parser import parse as date_parse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# OpenAI
from openai import OpenAI

# Exportações
from docx import Document
from docx.shared import Inches
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, PageBreak, Image as RLImage, Spacer
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

# ===========================================================
# 🔧 CONFIGURAÇÃO FIXA — ALTERE APENAS AQUI
# ===========================================================
HEAD_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/relatorios.csv"
FIND_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/constatacoes.csv"
LOGO_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/neo_logo.png"

NEO_GREEN = "#7CC04B"
NEO_BLUE  = "#0060A9"
NEO_DARK  = "#014e87"

# Modelo OpenAI (troque se quiser)
DEFAULT_MODEL = "gpt-4o-mini"

st.set_page_config(
    page_title="Neoenergia • Consulta Relatórios de Auditoria",
    page_icon="📗",
    layout="wide"
)

CSS = f"""
<style>
html, body, [class*="css"] {{
  font-family: Segoe UI, SegoeUI, Helvetica, Arial, sans-serif;
}}
h1, h2, h3 {{ color: {NEO_BLUE}; }}
.stButton>button {{
  background-color: {NEO_BLUE};
  color: white; border-radius: 6px;
}}
.stButton>button:hover {{ background-color: {NEO_DARK}; }}
.source {{
  border-left: 4px solid {NEO_BLUE};
  background: #f0f7ff; padding: 8px; margin: 6px 0;
}}
.small-note {{ color:#5f6b7a; font-size:12px; }}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ===========================================================
# Utilitários
# ===========================================================
@st.cache_data(show_spinner=False, ttl=300)
def load_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    data = r.content.decode("utf-8", errors="ignore")
    return pd.read_csv(io.StringIO(data))

@st.cache_data(show_spinner=False, ttl=1800)
def load_logo(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.content
    except Exception:
        return None

def to_iso(v) -> str:
    try:
        return date_parse(str(v), dayfirst=True).date().isoformat()
    except Exception:
        return ""

def chunk(s: str, max_chars=1000) -> List[str]:
    if not isinstance(s, str):
        return []
    if len(s) <= max_chars:
        return [s]
    parts = re.split(r"(?<=[.!?])\s+", s.strip())
    out, buf = [], ""
    for p in parts:
        if len(buf) + len(p) + 1 <= max_chars:
            buf = (buf + " " + p).strip()
        else:
            if buf:
                out.append(buf)
            buf = p
    if buf:
        out.append(buf)
    return out

# ----------------------- Normalização e mapeamento -----------------------
def _normalize_col(s: str) -> str:
    s = str(s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", "_", s.replace("-", "_"))
    return s.lower()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_col(c) for c in df.columns]
    return df

def ensure_col(df: pd.DataFrame, new_name: str, candidates: List[str], default="") -> pd.DataFrame:
    """Cria df[new_name] copiando a 1ª coluna existente em 'candidates'. Se não houver, cria com default."""
    df = df.copy()
    for c in candidates:
        if c in df.columns:
            df[new_name] = df[c]
            return df
    if new_name not in df.columns:
        df[new_name] = default
    return df

def enrich_impact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regras de priorização:
      1) tipo_de_constatacao / tipo_de_constatao
      2) associated_main_risk_category
      3) impact (se já existir)
    Escreve em df['impact'].
    """
    df = df.copy()
    df = ensure_col(df, "tipo_de_constatacao", ["tipo_de_constatacao", "tipo_de_constatao"], default="")
    df = ensure_col(df, "associated_main_risk_category", ["associated_main_risk_category"], default="")
    df = ensure_col(df, "impact", ["impact"], default="")

    def _pick(row):
        for c in ["tipo_de_constatacao", "associated_main_risk_category", "impact"]:
            v = str(row.get(c, "") or "").strip()
            if v and v.lower() not in {"nan", "none", "null"}:
                return v
        return ""

    df["impact"] = df.apply(_pick, axis=1)
    return df

# -------------------------------------------------------------------------
def build_corpus(dfh: pd.DataFrame, dff: pd.DataFrame) -> pd.DataFrame:
    rows = []

    # HEAD
    for _, r in dfh.iterrows():
        aud = str(r["aud_code"])
        text = "\n".join([
            f"[{aud}] {str(r.get('title',''))}",
            f"Objetivo: {str(r.get('objetivo',''))}",
            f"Escopo: {str(r.get('escopo',''))}",
            f"Riscos: {str(r.get('risco_processo',''))}",
            f"Alcance: {str(r.get('alcance',''))}",
            f"Cronograma: início {to_iso(r.get('cronograma_inicio',''))} • fim {to_iso(r.get('cronograma_final',''))}",
        ])
        for ch in chunk(text):
            rows.append({"source_type": "HEAD", "aud_code": aud, "finding_id": "", "text": ch})

    # FINDINGS
    for _, r in dff.iterrows():
        aud = str(r["aud_code"])
        fid = str(r.get("finding_id", ""))
        title = str(r.get("finding_title", ""))
        rec = str(r.get("recommendation", ""))
        imp = str(r.get("impact", ""))
        status = str(r.get("status", ""))
        owner = str(r.get("owner", ""))
        due = to_iso(r.get("due_date", ""))
        ftext = str(r.get("finding_text", ""))
        text = (
            f"[{aud} – {fid}] Constatação: {title} — Impacto: {imp} — Recomendação: {rec} — "
            f"Status: {status} — Resp.: {owner} — Prazo: {due}\n{ftext}"
        )
        for ch in chunk(text):
            rows.append({"source_type": "FIND", "aud_code": aud, "finding_id": fid, "text": ch})

    return pd.DataFrame(rows)

def search_tf(question: str, corpus: pd.DataFrame, top_k: int) -> pd.DataFrame:
    vect = TfidfVectorizer(strip_accents="unicode", ngram_range=(1, 2))
    M = vect.fit_transform(corpus["text"])
    qv = vect.transform([question])
    sim = cosine_similarity(qv, M).flatten()
    out = corpus.copy()
    out["score"] = sim
    return out.sort_values("score", ascending=False).head(top_k)

def _counts_by_aud_in_results(contexts_df: pd.DataFrame) -> pd.DataFrame:
    finds = contexts_df[contexts_df["source_type"] == "FIND"].copy()
    if finds.empty:
        return pd.DataFrame(columns=["aud_code", "qtd_findings"])
    g = finds.groupby("aud_code")["finding_id"].nunique().reset_index(name="qtd_findings")
    return g.sort_values(["qtd_findings", "aud_code"], ascending=[False, True])

# ===========================================================
# OpenAI RAG helpers
# ===========================================================
def format_context(results_df: pd.DataFrame, max_chars_total: int = 9000) -> str:
    parts = []
    total = 0
    if results_df is None or results_df.empty:
        return ""
    for _, r in results_df.iterrows():
        tag = f"[{r.get('source_type','')} | aud={r.get('aud_code','')} | finding={r.get('finding_id','')}]"
        text = str(r.get("text", "") or "").strip()
        chunk_txt = f"{tag}\n{text}\n"
        if total + len(chunk_txt) > max_chars_total:
            break
        parts.append(chunk_txt)
        total += len(chunk_txt)
    return "\n---\n".join(parts)

def openai_answer(question: str, results_df: pd.DataFrame) -> str:
    api_key = st.secrets.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return "❌ OPENAI_API_KEY não encontrada no Secrets."

    context = format_context(results_df, max_chars_total=9000)
    if not context.strip():
        return "Nenhum contexto relevante encontrado."

    client = OpenAI(api_key=api_key)

    try:
        resp = client.responses.create(
            model=DEFAULT_MODEL,
            input=[
                {"role": "system", "content": "Responda usando apenas o contexto fornecido."},
                {"role": "user", "content": f"Pergunta:\n{question}\n\nContexto:\n{context}"},
            ],
            temperature=0.2,
        )
        return resp.output_text

    except Exception as e:
        return f"🔥 ERRO OPENAI: {str(e)}"

    context = format_context(results_df, max_chars_total=9000)
    if not context.strip():
        return "Não encontrei trechos relevantes nos CSVs para responder com segurança."

    client = OpenAI(api_key=api_key)

    system = (
        "Você é um assistente de auditoria interna. Responda em PT-BR.\n"
        "Use APENAS o CONTEXTO fornecido (trechos dos CSVs do GitHub).\n"
        "Se a resposta não estiver no contexto, diga claramente que não encontrou nos arquivos.\n"
        "Sempre que possível, cite as tags [HEAD|FIND ...] que sustentam cada afirmação.\n"
        "Se pedirem números/quantidades, explique o critério de contagem com base no contexto."
    )

    user = f"PERGUNTA:\n{question}\n\nCONTEXTO:\n{context}"

    resp = client.responses.create(
        model=DEFAULT_MODEL,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        store=False,
    )

    return (resp.output_text or "").strip()

# ===========================================================
# Carregar dados
# ===========================================================
with st.spinner("Carregando dados do GitHub..."):
    df_h = load_csv(HEAD_URL)
    df_f = load_csv(FIND_URL)

df_h = normalize_columns(df_h)
df_f = normalize_columns(df_f)

# HEAD: aud_code obrigatório
if "aud_code" not in df_h.columns:
    st.error("O CSV de relatórios não contém a coluna 'aud_code'. Verifique HEAD_URL.")
    st.stop()
df_h["aud_code"] = df_h["aud_code"].astype(str).str.strip().str.upper()

# preenchimentos úteis
for c in [
    "classification", "title", "company", "objetivo", "escopo", "risco_processo", "alcance",
    "cronograma_inicio", "cronograma_draft", "cronograma_final", "emission_date", "ano", "mes"
]:
    if c in df_h.columns:
        df_h[c] = df_h[c].fillna("")

# Derivar ano_filter
if "ano" in df_h.columns:
    df_h["ano_filter"] = df_h["ano"].astype(str)
elif "emission_date" in df_h.columns:
    df_h["ano_filter"] = pd.to_datetime(df_h["emission_date"], errors="coerce").dt.year.astype("Int64").astype(str)
else:
    df_h["ano_filter"] = ""

# FINDINGS: canônicos (variações mapeadas do seu CSV)
if "aud_code" not in df_f.columns:
    df_f = ensure_col(df_f, "aud_code", ["id_do_trabalho"], default="")
df_f["aud_code"] = df_f["aud_code"].astype(str).str.strip().str.upper()

df_f = ensure_col(df_f, "finding_id", ["finding_id"], default="")
df_f = ensure_col(df_f, "finding_title", ["nome_da_constatacao", "nome_da_constatao"], default="")
df_f = ensure_col(
    df_f, "recommendation",
    ["descricao_do_plano_de_recomendacao", "descrio_do_plano_de_recomendao", "recommendation"],
    default=""
)
df_f = ensure_col(df_f, "status", ["status_da_constatacao", "estado_del_trabajo", "status"], default="")
df_f = ensure_col(
    df_f, "owner",
    [
        "proprietario_da_constatacao", "organization_of_finding_response_owner",
        "proprietario_da_resposta_descoberta", "proprietrio_da_constatao",
        "proprietrio_da_resposta__descoberta", "owner"
    ],
    default=""
)
df_f = ensure_col(
    df_f, "due_date",
    ["data_acordada_vencimento", "data_acordada__vencimento", "data_acordada_aprovada_atualmente", "end_date", "due_date"],
    default=""
)
df_f = ensure_col(df_f, "finding_text", ["constatacao", "constatao", "resposta", "finding_text"], default="")
df_f = ensure_col(df_f, "tema", ["negocio_associado", "negcio_associado", "compromissos_da_auditoria", "tema"], default="")
df_f = ensure_col(df_f, "impact", ["impact"], default="")
df_f = enrich_impact(df_f)

for c in ["status", "tema", "impact", "recommendation", "finding_title", "finding_text", "owner", "due_date"]:
    if c in df_f.columns:
        df_f[c] = df_f[c].fillna("").astype(str)

# coluna para filtro de risco (usa impact enriquecido)
df_f["risk_filter"] = df_f["impact"].astype(str).str.strip()

# Corpus para busca
corpus = build_corpus(df_h, df_f)

# ===========================================================
# Logo & Título
# ===========================================================
logo_bytes = load_logo(LOGO_URL)
if logo_bytes:
    st.image(logo_bytes, width=180)
st.title("📗 Consulta Relatórios de Auditoria")

# ===========================================================
# Filtros
# ===========================================================
st.subheader("🔎 Filtros")
cols = st.columns(4)
with cols[0]:
    f_title = st.multiselect("Título do trabalho", sorted(pd.Series(df_h["title"]).dropna().astype(str).unique()))
with cols[1]:
    f_risk = st.multiselect(
        "Risco / Impacto (constatações)",
        sorted(pd.Series(df_f["risk_filter"]).replace("", pd.NA).dropna().unique())
    )
with cols[2]:
    f_company = (
        st.multiselect("Empresa", sorted(pd.Series(df_h["company"]).dropna().astype(str).unique()))
        if "company" in df_h.columns else []
    )
with cols[3]:
    f_year = st.multiselect("Ano", sorted(pd.Series(df_h["ano_filter"]).replace("", "Sem ano").unique()))

heads_filt = df_h.copy()
if f_title:
    heads_filt = heads_filt[heads_filt["title"].isin(f_title)]
if f_company:
    heads_filt = heads_filt[heads_filt["company"].isin(f_company)]
if f_year:
    years_norm = [("" if y == "Sem ano" else y) for y in f_year]
    heads_filt = heads_filt[heads_filt["ano_filter"].astype(str).isin(years_norm)]

aud_subset = set(heads_filt["aud_code"]) if not heads_filt.empty else set()

filtered_corpus = corpus.copy()
if aud_subset:
    filtered_corpus = filtered_corpus[filtered_corpus["aud_code"].isin(aud_subset)]

if f_risk:
    valid_finds = df_f[df_f["risk_filter"].isin(f_risk)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[
        ((filtered_corpus["source_type"] == "FIND") & (filtered_corpus["finding_id"].isin(valid_finds)))
        | (filtered_corpus["source_type"] == "HEAD")
    ]

# ===========================================================
# Resumo por AUD (no filtro atual)
# ===========================================================
with st.expander("📊 Resumo de constatações por AUD (no filtro atual)"):
    filt_finds = df_f.copy()
    if aud_subset:
        filt_finds = filt_finds[filt_finds["aud_code"].isin(aud_subset)]
    if f_risk:
        filt_finds = filt_finds[filt_finds["risk_filter"].isin(f_risk)]

    cnt = (
        filt_finds
        .assign(finding_id=filt_finds["finding_id"].fillna("").astype(str))
        .query("finding_id != ''")
        .groupby("aud_code")["finding_id"].nunique()
        .reset_index(name="qtd_findings")
        .sort_values(["qtd_findings", "aud_code"], ascending=[False, True])
    )

    if cnt.empty:
        st.write("Nenhuma constatação no filtro atual.")
    else:
        st.dataframe(cnt, use_container_width=True)

# ===========================================================
# Chat (OpenAI RAG) — usa o filtered_corpus e responde via OpenAI
# ===========================================================
st.subheader("💬 Pergunte sobre os relatórios (OpenAI)")
show_sources = st.checkbox("Mostrar fontes (trechos)", value=False)

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Digite sua pergunta...")

if q:
    # 1) Recupera trechos mais relevantes do corpus filtrado
    results = search_tf(q, filtered_corpus, top_k=12)

    # 2) Modelo responde baseado nos trechos recuperados
    answer = openai_answer(q, results)

    st.session_state["history"].append(("user", q))
    st.session_state["history"].append(("assistant", answer, results))

for msg in st.session_state["history"]:
    if msg[0] == "user":
        with st.chat_message("user"):
            st.write(msg[1])
    else:
        with st.chat_message("assistant"):
            st.write(msg[1])

            if show_sources:
                st.markdown("**Trechos utilizados (contexto enviado ao modelo):**")
                for _, r in msg[2].iterrows():
                    tag = f"[{r.get('source_type','')} | {r.get('aud_code','')} – {r.get('finding_id','')}]"
                    html = f"<div class='source'><b>{tag}</b><br>{str(r.get('text',''))[:500]}...</div>"
                    st.markdown(html, unsafe_allow_html=True)

# ===========================================================
# Exportações
# ===========================================================
st.subheader("📤 Exportar")

def export_pdf(text):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    y = H - 50
    if logo_bytes:
        c.drawImage(ImageReader(io.BytesIO(logo_bytes)), 40, y - 40, width=150, height=40)
        y -= 60
    c.setFont("Helvetica-Bold", 14)
    c.setFillColor(HexColor(NEO_BLUE))
    c.drawString(40, y, "Neoenergia — Q&A de Relatórios")
    y -= 20
    c.setFont("Helvetica", 10)
    for line in text.split("\n"):
        if y < 50:
            c.showPage()
            y = H - 50
        line = re.sub(r"\*\*|_", "", line)
        c.drawString(40, y, line[:1200])
        y -= 14
    c.save()
    return buf.getvalue()

def export_pdf_detailed(df_head, df_find, results_df, logo_bytes=None):
    res_finds = results_df[results_df["source_type"] == "FIND"][["aud_code", "finding_id"]].drop_duplicates()
    if res_finds.empty:
        return export_pdf("Nenhuma constatação no resultado atual para exportar.")

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_find.set_index(["aud_code", "finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code", "status", "finding_id"], na_position="last")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=48, bottomMargin=36)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TitleNeo", parent=styles["Heading1"], textColor=colors.HexColor(NEO_BLUE))
    h2_style = ParagraphStyle("H2Neo", parent=styles["Heading2"], textColor=colors.HexColor(NEO_BLUE))
    normal = styles["BodyText"]
    story = []

    if logo_bytes:
        story.append(RLImage(io.BytesIO(logo_bytes), width=180, height=48))
        story.append(Spacer(1, 12))

    story.append(Paragraph("Neoenergia — Q&A de Relatórios (Export Detalhado)", title_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(datetime.datetime.now().strftime("%d/%m/%Y %H:%M"), normal))
    story.append(PageBreak())

    for aud, df_aud in aux.groupby("aud_code", sort=False):
        story.append(Paragraph(f"Relatório: {aud}", h2_style))
        head_row = df_head[df_head["aud_code"] == aud].head(1)
        if len(head_row):
            hr = head_row.iloc[0]
            resumo = [
                f"<b>Título:</b> {hr.get('title','')}",
                f"<b>Empresa:</b> {hr.get('company','')}",
                f"<b>Objetivo:</b> {hr.get('objetivo','')}",
                f"<b>Escopo:</b> {hr.get('escopo','')}",
                f"<b>Riscos:</b> {hr.get('risco_processo','')}",
                f"<b>Alcance:</b> {hr.get('alcance','')}",
                f"<b>Cronograma:</b> início {to_iso(hr.get('cronograma_inicio',''))} • fim {to_iso(hr.get('cronograma_final',''))}",
            ]
            for line in resumo:
                story.append(Paragraph(line, normal))
            story.append(Spacer(1, 6))

        tbl_data = [["Finding ID", "Título", "Impacto (priorizado)", "Recomendação", "Status", "Responsável", "Prazo"]]
        for _, r in df_aud.iterrows():
            tbl_data.append([
                str(r.get("finding_id", "")),
                str(r.get("finding_title", "")),
                str(r.get("impact", "")),
                str(r.get("recommendation", "")),
                str(r.get("status", "")),
                str(r.get("owner", "")),
                to_iso(r.get("due_date", "")),
            ])

        table = Table(tbl_data, repeatRows=1, colWidths=[60, 120, 110, 110, 60, 80, 60])
        table_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(NEO_BLUE)),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ])
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 8))

        for _, r in df_aud.iterrows():
            story.append(Paragraph(f"**[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}**", normal))
            ftext = str(r.get("finding_text", "") or "")
            if not ftext.strip():
                ftext = "(Sem descrição detalhada.)"
            story.append(Paragraph(ftext.replace("\n", "<br/>"), normal))
            story.append(Spacer(1, 6))

        story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()

def export_docx(text):
    doc = Document()
    if logo_bytes:
        try:
            doc.add_picture(io.BytesIO(logo_bytes), width=Inches(2.2))
        except Exception:
            pass
    doc.add_heading("Neoenergia — Q&A de Relatórios", level=1)
    for line in text.split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|_", "", line))
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

def export_docx_detailed(df_head, df_find, results_df, logo_bytes=None):
    res_finds = results_df[results_df["source_type"] == "FIND"][["aud_code", "finding_id"]].drop_duplicates()
    if res_finds.empty:
        d = Document()
        d.add_heading("Neoenergia — Q&A (Export Detalhado)", level=1)
        d.add_paragraph("Nenhuma constatação no resultado atual para exportar.")
        out = io.BytesIO()
        d.save(out)
        return out.getvalue()

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_find.set_index(["aud_code", "finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code", "status", "finding_id"], na_position="last")

    d = Document()
    if logo_bytes:
        try:
            d.add_picture(io.BytesIO(logo_bytes), width=Inches(2.2))
        except Exception:
            pass
    d.add_heading("Neoenergia — Q&A de Relatórios (Export Detalhado)", level=1)
    d.add_paragraph(datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))

    first_aud = True
    for aud, df_aud in aux.groupby("aud_code", sort=False):
        if not first_aud:
            d.add_page_break()
        first_aud = False

        d.add_heading(f"Relatório: {aud}", level=2)
        head_row = df_head[df_head["aud_code"] == aud].head(1)
        if len(head_row):
            hr = head_row.iloc[0]
            d.add_paragraph(f"Título: {hr.get('title','')}")
            d.add_paragraph(f"Empresa: {hr.get('company','')}")
            d.add_paragraph(f"Objetivo: {hr.get('objetivo','')}")
            d.add_paragraph(f"Escopo: {hr.get('escopo','')}")
            d.add_paragraph(f"Riscos: {hr.get('risco_processo','')}")
            d.add_paragraph(f"Alcance: {hr.get('alcance','')}")
            d.add_paragraph(
                f"Cronograma: início {to_iso(hr.get('cronograma_inicio',''))} • fim {to_iso(hr.get('cronograma_final',''))}"
            )

        table = d.add_table(rows=1, cols=7)
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text = "Finding ID"
        hdr_cells[1].text = "Título"
        hdr_cells[2].text = "Impacto (priorizado)"
        hdr_cells[3].text = "Recomendação"
        hdr_cells[4].text = "Status"
        hdr_cells[5].text = "Responsável"
        hdr_cells[6].text = "Prazo"

        for _, r in df_aud.iterrows():
            row = table.add_row().cells
            row[0].text = str(r.get("finding_id", ""))
            row[1].text = str(r.get("finding_title", ""))
            row[2].text = str(r.get("impact", ""))
            row[3].text = str(r.get("recommendation", ""))
            row[4].text = str(r.get("status", ""))
            row[5].text = str(r.get("owner", ""))
            row[6].text = to_iso(r.get("due_date", ""))

        for _, r in df_aud.iterrows():
            d.add_paragraph(f"[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}", style="List Bullet")
            ftext = str(r.get("finding_text", "") or "")
            if not ftext.strip():
                ftext = "(Sem descrição detalhada.)"
            d.add_paragraph(ftext)

    out = io.BytesIO()
    d.save(out)
    return out.getvalue()

# Últimos resultados para export
last_answer, last_results = None, None
for msg in reversed(st.session_state.get("history", [])):
    if msg[0] == "assistant":
        last_answer = msg[1]
        last_results = msg[2]
        break

col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("⬇️ Exportar PDF (simples)", disabled=(last_answer is None)):
        pdf = export_pdf(last_answer or "")
        st.download_button("Baixar PDF", pdf, "neoenergia_qa.pdf", mime="application/pdf")
with col2:
    if st.button("⬇️ Exportar Word (simples)", disabled=(last_answer is None)):
        docx = export_docx(last_answer or "")
        st.download_button("Baixar DOCX", docx, "neoenergia_qa.docx")
with col3:
    if st.button("⬇️ Exportar PDF detalhado", disabled=(last_results is None)):
        pdfd = export_pdf_detailed(df_h, df_f, last_results, logo_bytes=logo_bytes)
        st.download_button("Baixar PDF detalhado", pdfd, "neoenergia_qa_detalhado.pdf", mime="application/pdf")
with col4:
    if st.button("⬇️ Exportar Word detalhado", disabled=(last_results is None)):
        docxd = export_docx_detailed(df_h, df_f, last_results, logo_bytes=logo_bytes)
        st.download_button("Baixar DOCX detalhado", docxd, "neoenergia_qa_detalhado.docx")


