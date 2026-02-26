# -*- coding: utf-8 -*-
import io
import re
import datetime
import unicodedata
import requests
import pandas as pd
import streamlit as st
from typing import Optional, List, Tuple

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

DEFAULT_MODEL = "gpt-4o-mini"  # bom custo/benefício
TODAY = datetime.date.today()

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
    df = df.copy()
    for c in candidates:
        if c in df.columns:
            df[new_name] = df[c]
            return df
    if new_name not in df.columns:
        df[new_name] = default
    return df

def enrich_impact(df: pd.DataFrame) -> pd.DataFrame:
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

def build_corpus(dfh: pd.DataFrame, dff: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, r in dfh.iterrows():
        aud = str(r.get("aud_code",""))
        ano = str(r.get("ano",""))
        text = "\n".join([
            f"[{aud}] {str(r.get('title',''))}",
            f"Ano: {ano}",
            f"Empresa: {str(r.get('company',''))}",
            f"Tipo: {str(r.get('report_type',''))}",
            f"Objetivo: {str(r.get('objetivo',''))}",
            f"Escopo: {str(r.get('escopo',''))}",
            f"Riscos: {str(r.get('risco_processo',''))}",
            f"Alcance: {str(r.get('alcance',''))}",
            f"Cronograma: início {to_iso(r.get('cronograma_inicio',''))} • fim {to_iso(r.get('cronograma_final',''))}",
        ])
        for ch in chunk(text):
            rows.append({"source_type":"HEAD","aud_code":aud,"finding_id":"","text":ch})

    for _, r in dff.iterrows():
        aud  = str(r.get("aud_code",""))
        fid  = str(r.get("finding_id",""))
        title= str(r.get("finding_title",""))
        rec  = str(r.get("recommendation",""))
        imp  = str(r.get("impact",""))
        status = str(r.get("status",""))
        owner  = str(r.get("owner",""))
        due    = to_iso(r.get("due_date",""))
        ftext  = str(r.get("finding_text",""))
        text = (
            f"[{aud} – {fid}] Constatação: {title} — Impacto: {imp} — "
            f"Recomendação: {rec} — Status: {status} — Resp.: {owner} — Prazo: {due}\n{ftext}"
        )
        for ch in chunk(text):
            rows.append({"source_type":"FIND","aud_code":aud,"finding_id":fid,"text":ch})

    return pd.DataFrame(rows)

def search_tf(question: str, corpus: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if corpus is None or corpus.empty:
        return pd.DataFrame(columns=["source_type","aud_code","finding_id","text","score"])
    vect = TfidfVectorizer(strip_accents="unicode", ngram_range=(1,2))
    M = vect.fit_transform(corpus["text"].astype(str))
    qv = vect.transform([str(question)])
    sim = cosine_similarity(qv, M).flatten()
    out = corpus.copy()
    out["score"] = sim
    return out.sort_values("score", ascending=False).head(top_k)

# ===========================================================
# OpenAI (narração/explicação)
# ===========================================================
def format_context(results_df: pd.DataFrame, max_chars_total: int = 7000) -> str:
    if results_df is None or results_df.empty:
        return ""
    parts, total = [], 0
    for _, r in results_df.iterrows():
        tag = f"[{r.get('source_type','')} | aud={r.get('aud_code','')} | finding={r.get('finding_id','')}]"
        text = str(r.get("text","") or "").strip()
        chunk_txt = f"{tag}\n{text}\n"
        if total + len(chunk_txt) > max_chars_total:
            break
        parts.append(chunk_txt)
        total += len(chunk_txt)
    return "\n---\n".join(parts)

def openai_chat(question: str, context_df: pd.DataFrame, analytics_text: str = "", analytics_table: Optional[pd.DataFrame] = None) -> str:
    api_key = st.secrets.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return "❌ OPENAI_API_KEY não encontrada no Secrets do Streamlit."

    client = OpenAI(api_key=api_key)

    rag_context = format_context(context_df, max_chars_total=7000)
    table_csv = ""
    if analytics_table is not None and isinstance(analytics_table, pd.DataFrame) and not analytics_table.empty:
        table_csv = analytics_table.head(30).to_csv(index=False)

    system = (
        "Você é um assistente executivo de Auditoria Interna (PT-BR). "
        "Regra: NÃO invente números. Se houver um 'Resultado Analítico', use ele como verdade. "
        "Se precisar de algo que não está no contexto/tabela, diga o que falta. "
        "Responda de forma objetiva e estratégica."
    )

    user = (
        f"PERGUNTA:\n{question}\n\n"
        f"RESULTADO ANALÍTICO (se houver):\n{analytics_text}\n\n"
        f"TABELA (csv, se houver):\n{table_csv}\n\n"
        f"CONTEXTO (trechos dos CSVs, se houver):\n{rag_context}"
    )

    try:
        resp = client.responses.create(
            model=DEFAULT_MODEL,
            input=[
                {"role":"system","content":system},
                {"role":"user","content":user},
            ],
            temperature=0.2,
            store=False,
        )
        return (resp.output_text or "").strip()
    except Exception as e:
        return f"🔥 ERRO OPENAI: {str(e)}"

# ===========================================================
# Analytics (respostas exatas para superintendência)
# ===========================================================
def _extract_year(q: str) -> Optional[str]:
    m = re.search(r"\b(20\d{2})\b", q)
    return m.group(1) if m else None

def _extract_days(q: str) -> Optional[int]:
    m = re.search(r"\b(30|60|90)\b", q)
    return int(m.group(1)) if m else None

def _is_analytics(q: str) -> bool:
    q = (q or "").lower()
    keys = [
        "quantos","quantidade","total","top","ranking","maior","menor","mais","menos",
        "começa","comeca","inicia","início","inicio","termina","fim","cronograma",
        "próximos","proximos","dias","mês","mes","trimestre",
        "empresa","risco","impacto","status","atrasad","vencid","prazo","respons"
    ]
    return any(k in q for k in keys)

def analytics_answer(question: str, df_h: pd.DataFrame, df_f: pd.DataFrame) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    q = (question or "").strip().lower()
    year = _extract_year(q)

    h = df_h.copy()
    f = df_f.copy()

    # ano DO RELATÓRIO é a regra (como você pediu)
    if "ano" in h.columns:
        h["ano"] = h["ano"].astype(str).str.strip()
    if year and "ano" in h.columns:
        h = h[h["ano"] == str(year)]

    # datas
    h["dt_inicio"] = pd.to_datetime(h.get("cronograma_inicio",""), errors="coerce").dt.date
    h["dt_fim"]    = pd.to_datetime(h.get("cronograma_final",""), errors="coerce").dt.date

    # mapeia constatações para aud_code do ano filtrado
    if year and "aud_code" in h.columns:
        auds_year = set(h["aud_code"].astype(str))
        f = f[f["aud_code"].astype(str).isin(auds_year)]

    # 1) quantos aud_code em 2025?
    if ("quantos" in q or "quantidade" in q or "total" in q) and ("aud" in q or "aud_code" in q):
        total = h["aud_code"].nunique()
        return (f"Total de **{total}** aud_code" + (f" em **{year}**." if year else "."), None)

    # 2) qual trabalho teve mais recomendações/constatações em 2025?
    if ("mais" in q or "maior" in q or "top" in q) and ("recomend" in q or "constat" in q or "achad" in q):
        base = f[f.get("finding_id","").astype(str).str.strip() != ""].copy()
        if base.empty:
            return ("Não encontrei constatações para calcular o ranking no recorte atual.", None)

        g = (base.groupby("aud_code")["finding_id"].nunique()
             .sort_values(ascending=False).reset_index(name="qtd_constatacoes"))

        g = g.merge(h[["aud_code","title","company","ano","dt_inicio","dt_fim"]], on="aud_code", how="left")
        top = g.iloc[0]
        text = (
            f"O trabalho com mais constatações é **{top['aud_code']} — {top.get('title','(sem título)')}**, "
            f"com **{int(top['qtd_constatacoes'])}** constatação(ões)"
            + (f" em **{year}**." if year else ".")
        )
        return (text, g.head(10))

    # 3) próximos 30/60/90 dias (agenda)
    if "próxim" in q or "proxim" in q:
        days = _extract_days(q) or 30
        start = TODAY
        end = TODAY + datetime.timedelta(days=days)
        upcoming = h[(h["dt_inicio"].notna()) & (h["dt_inicio"] >= start) & (h["dt_inicio"] <= end)].copy()
        upcoming = upcoming.sort_values("dt_inicio")
        if upcoming.empty:
            return (f"Não encontrei trabalhos com início nos próximos **{days} dias**" + (f" em {year}." if year else "."), None)

        out = upcoming[["aud_code","title","company","ano","dt_inicio","dt_fim"]].copy()
        out.rename(columns={"dt_inicio":"inicio","dt_fim":"fim"}, inplace=True)
        return (f"Encontrei **{len(out)}** trabalho(s) com início nos próximos **{days} dias**.", out.head(50))

    # 4) trabalhos em execução hoje
    if "em execução" in q or "em execucao" in q or "rodando" in q:
        running = h[(h["dt_inicio"].notna()) & (h["dt_fim"].notna()) & (h["dt_inicio"] <= TODAY) & (h["dt_fim"] >= TODAY)].copy()
        if running.empty:
            return ("Não encontrei trabalhos em execução (hoje) no recorte atual.", None)
        out = running[["aud_code","title","company","ano","dt_inicio","dt_fim"]].copy()
        out.rename(columns={"dt_inicio":"inicio","dt_fim":"fim"}, inplace=True)
        return (f"Trabalhos em execução hoje: **{len(out)}**.", out.head(50))

    # 5) recomendações atrasadas (com responsáveis)
    if "atrasad" in q or "vencid" in q:
        f["due_dt"] = pd.to_datetime(f.get("due_date",""), errors="coerce").dt.date
        status = f.get("status","").astype(str).str.lower()
        open_mask = ~status.str.contains("encerrad|fechad|implementad|conclu", regex=True, na=False)

        overdue = f[(f["due_dt"].notna()) & (f["due_dt"] < TODAY) & open_mask].copy()
        if overdue.empty:
            return ("Não encontrei recomendações atrasadas no recorte atual.", None)

        out = overdue[["aud_code","finding_id","finding_title","owner","status","due_dt","impact"]].copy()
        out.rename(columns={"due_dt":"prazo"}, inplace=True)
        out = out.sort_values(["prazo","aud_code"]).head(50)
        return (f"Encontrei **{len(out)}** recomendação(ões) atrasada(s).", out)

    # 6) empresa com mais constatações
    if "empresa" in q and ("mais" in q or "top" in q) and ("constat" in q or "achad" in q):
        base = f[f.get("finding_id","").astype(str).str.strip() != ""].merge(h[["aud_code","company"]], on="aud_code", how="left")
        g = (base.groupby("company")["finding_id"].nunique()
             .sort_values(ascending=False).reset_index(name="qtd_constatacoes"))
        if g.empty:
            return ("Não encontrei dados suficientes para ranking por empresa.", None)
        top = g.iloc[0]
        return (f"Empresa com mais constatações: **{top['company']}** (**{int(top['qtd_constatacoes'])}**).", g.head(10))

    # não reconheceu intent analytics
    return (None, None)

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

# HEAD: ano (seu critério)
if "ano" in df_h.columns:
    df_h["ano"] = df_h["ano"].fillna("").astype(str).str.strip()

# preenchimentos úteis
for c in [
    "title","report_type","company","emission_date","objetivo","risco_processo","escopo","alcance",
    "cronograma_inicio","cronograma_draft","cronograma_final","classification","source_s3_uri","ingestion_ts","ano","mes"
]:
    if c in df_h.columns:
        df_h[c] = df_h[c].fillna("")

# FINDINGS: canônicos (variações)
if "aud_code" not in df_f.columns:
    df_f = ensure_col(df_f, "aud_code", ["id_do_trabalho"], default="")
df_f["aud_code"] = df_f["aud_code"].astype(str).str.strip().str.upper()

df_f = ensure_col(df_f, "finding_id", ["finding_id"], default="")
df_f = ensure_col(df_f, "finding_title", ["nome_da_constatacao","nome_da_constatao","finding_title"], default="")
df_f = ensure_col(df_f, "recommendation",
                  ["descricao_do_plano_de_recomendacao","descrio_do_plano_de_recomendao","recommendation"], default="")
df_f = ensure_col(df_f, "status", ["status_da_constatacao","estado_del_trabajo","status"], default="")
df_f = ensure_col(df_f, "owner", [
    "proprietario_da_constatacao","organization_of_finding_response_owner",
    "proprietario_da_resposta_descoberta","proprietrio_da_constatao",
    "proprietrio_da_resposta__descoberta","owner"
], default="")
df_f = ensure_col(df_f, "due_date", [
    "data_acordada_vencimento","data_acordada__vencimento","data_acordada_aprovada_atualmente","end_date","due_date"
], default="")
df_f = ensure_col(df_f, "finding_text", ["constatacao","constatao","resposta","finding_text"], default="")
df_f = ensure_col(df_f, "impact", ["impact"], default="")
df_f = enrich_impact(df_f)

for c in ["status","impact","recommendation","finding_title","finding_text","owner","due_date"]:
    if c in df_f.columns:
        df_f[c] = df_f[c].fillna("").astype(str)

df_f["risk_filter"] = df_f["impact"].astype(str).str.strip()

# Corpus para RAG (só para perguntas descritivas)
corpus = build_corpus(df_h, df_f)

# ===========================================================
# Logo & Título
# ===========================================================
logo_bytes = load_logo(LOGO_URL)
if logo_bytes:
    st.image(logo_bytes, width=180)

st.title("📗 Neoenergia — Q&A Estratégico de Relatórios")
st.caption("Visão executiva + perguntas analíticas (cálculo exato) + explicação via OpenAI.")

# ===========================================================
# Filtros (mantém sua lógica, mas agora 'Ano' usa df_h['ano'])
# ===========================================================
st.subheader("🔎 Filtros")
cols = st.columns(4)
with cols[0]:
    f_title = st.multiselect("Título do trabalho", sorted(pd.Series(df_h["title"]).dropna().astype(str).unique()))
with cols[1]:
    f_risk = st.multiselect("Risco / Impacto (constatações)", sorted(pd.Series(df_f["risk_filter"]).replace("", pd.NA).dropna().unique()))
with cols[2]:
    f_company = st.multiselect("Empresa", sorted(pd.Series(df_h["company"]).dropna().astype(str).unique())) if "company" in df_h.columns else []
with cols[3]:
    # usa ANO do CSV (seu critério)
    f_year = st.multiselect("Ano (campo 'ano')", sorted(pd.Series(df_h.get("ano","")).replace("", "Sem ano").unique()))

heads_filt = df_h.copy()
if f_title:
    heads_filt = heads_filt[heads_filt["title"].isin(f_title)]
if f_company:
    heads_filt = heads_filt[heads_filt["company"].isin(f_company)]
if f_year:
    years_norm = [("" if y == "Sem ano" else str(y)) for y in f_year]
    heads_filt = heads_filt[heads_filt["ano"].astype(str).isin(years_norm)]

aud_subset = set(heads_filt["aud_code"]) if not heads_filt.empty else set()

filtered_corpus = corpus.copy()
if aud_subset:
    filtered_corpus = filtered_corpus[filtered_corpus["aud_code"].isin(aud_subset)]

# filtra findings por risco, mas mantém HEAD
if f_risk:
    valid_finds = df_f[df_f["risk_filter"].isin(f_risk)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[
        ((filtered_corpus["source_type"] == "FIND") & (filtered_corpus["finding_id"].isin(valid_finds))) |
        (filtered_corpus["source_type"] == "HEAD")
    ]

# ===========================================================
# 📌 Visão Executiva (recorte atual)
# ===========================================================
st.subheader("📌 Visão Executiva (recorte dos filtros)")

# aplica recorte também em df_h / df_f
df_h_view = heads_filt.copy()
df_f_view = df_f.copy()
if aud_subset:
    df_f_view = df_f_view[df_f_view["aud_code"].isin(aud_subset)]
if f_risk:
    df_f_view = df_f_view[df_f_view["risk_filter"].isin(f_risk)]

df_h_view["dt_inicio"] = pd.to_datetime(df_h_view.get("cronograma_inicio",""), errors="coerce").dt.date
df_h_view["dt_fim"]    = pd.to_datetime(df_h_view.get("cronograma_final",""), errors="coerce").dt.date

colA, colB, colC, colD = st.columns(4)

up30 = df_h_view[(df_h_view["dt_inicio"].notna()) &
                 (df_h_view["dt_inicio"] >= TODAY) &
                 (df_h_view["dt_inicio"] <= TODAY + datetime.timedelta(days=30))]
colA.metric("Inícios (30 dias)", int(up30["aud_code"].nunique()) if not up30.empty else 0)

running = df_h_view[(df_h_view["dt_inicio"].notna()) & (df_h_view["dt_fim"].notna()) &
                    (df_h_view["dt_inicio"] <= TODAY) & (df_h_view["dt_fim"] >= TODAY)]
colB.metric("Em execução (hoje)", int(running["aud_code"].nunique()) if not running.empty else 0)

tmp_f = df_f_view.copy()
tmp_f["due_dt"] = pd.to_datetime(tmp_f.get("due_date",""), errors="coerce").dt.date
status = tmp_f.get("status","").astype(str).str.lower()
open_mask = ~status.str.contains("encerrad|fechad|implementad|conclu", regex=True, na=False)
overdue = tmp_f[(tmp_f["due_dt"].notna()) & (tmp_f["due_dt"] < TODAY) & open_mask]
colC.metric("Recom. atrasadas", int(len(overdue)) if not overdue.empty else 0)

has_find = df_f_view[df_f_view["finding_id"].astype(str).str.strip() != ""]
colD.metric("Trabalhos com achados", int(has_find["aud_code"].nunique()) if not has_find.empty else 0)

with st.expander("📅 Próximos 30 dias — lista"):
    if up30.empty:
        st.write("Sem inícios nos próximos 30 dias.")
    else:
        show = up30.sort_values("dt_inicio")[["aud_code","title","company","ano","dt_inicio","dt_fim"]].head(50)
        show.rename(columns={"dt_inicio":"inicio","dt_fim":"fim"}, inplace=True)
        st.dataframe(show, use_container_width=True)

with st.expander("🔥 Top 10 trabalhos mais críticos (por nº de constatações)"):
    if has_find.empty:
        st.write("Sem constatações para ranking.")
    else:
        g = (has_find.groupby("aud_code")["finding_id"].nunique()
             .sort_values(ascending=False).head(10).reset_index(name="qtd_constatacoes"))
        g = g.merge(df_h_view[["aud_code","title","company","ano"]], on="aud_code", how="left")
        st.dataframe(g, use_container_width=True)

# ===========================================================
# 💬 Chat Estratégico
# ===========================================================
st.subheader("💬 Perguntas (superintendência + auditor)")
show_sources = st.checkbox("Mostrar fontes (trechos)", value=False)

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Ex.: Qual trabalho teve mais recomendações em 2025? / Quais começam nos próximos 60 dias?")

if q:
    if _is_analytics(q):
        a_text, a_df = analytics_answer(q, df_h_view, df_f_view)
        if a_text is not None:
            retrieved = search_tf(q, filtered_corpus, top_k=12)
            final = openai_chat(q, retrieved, analytics_text=a_text, analytics_table=a_df)
            st.session_state["history"].append(("user", q))
            st.session_state["history"].append(("assistant", final, a_df, retrieved))
        else:
            retrieved = search_tf(q, filtered_corpus, top_k=12)
            final = openai_chat(q, retrieved, analytics_text="Não identifiquei um cálculo específico. Vou responder pelo contexto disponível.", analytics_table=None)
            st.session_state["history"].append(("user", q))
            st.session_state["history"].append(("assistant", final, None, retrieved))
    else:
        retrieved = search_tf(q, filtered_corpus, top_k=12)
        final = openai_chat(q, retrieved, analytics_text="", analytics_table=None)
        st.session_state["history"].append(("user", q))
        st.session_state["history"].append(("assistant", final, None, retrieved))

for msg in st.session_state["history"]:
    if msg[0] == "user":
        with st.chat_message("user"):
            st.write(msg[1])
    else:
        _, text, df_out, retrieved = msg
        with st.chat_message("assistant"):
            st.write(text)
            if df_out is not None and isinstance(df_out, pd.DataFrame) and not df_out.empty:
                st.dataframe(df_out, use_container_width=True)

            if show_sources and retrieved is not None and not retrieved.empty:
                st.markdown("**Trechos usados (RAG):**")
                for _, r in retrieved.head(8).iterrows():
                    tag = f"[{r.get('source_type','')} | {r.get('aud_code','')} – {r.get('finding_id','')}]"
                    html = f"<div class='source'><b>{tag}</b><br>{str(r.get('text',''))[:500]}...</div>"
                    st.markdown(html, unsafe_allow_html=True)

# ===========================================================
# Exportações (mantidas)
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
    c.setFont("Helvetica-Bold", 14); c.setFillColor(HexColor(NEO_BLUE))
    c.drawString(40, y, "Neoenergia — Q&A de Relatórios"); y -= 20
    c.setFont("Helvetica", 10)
    for line in (text or "").split("\n"):
        if y < 50:
            c.showPage(); y = H-50
        line = re.sub(r"\*\*|_", "", line)
        c.drawString(40, y, line[:1200]); y -= 14
    c.save()
    return buf.getvalue()

def export_docx(text):
    doc = Document()
    if logo_bytes:
        try:
            doc.add_picture(io.BytesIO(logo_bytes), width=Inches(2.2))
        except Exception:
            pass
    doc.add_heading("Neoenergia — Q&A de Relatórios", level=1)
    for line in (text or "").split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|_", "", line))
    out = io.BytesIO(); doc.save(out); return out.getvalue()

last_answer = None
for msg in reversed(st.session_state.get("history", [])):
    if msg[0] == "assistant":
        last_answer = msg[1]
        break

col1, col2 = st.columns(2)
with col1:
    if st.button("⬇️ Exportar PDF", disabled=(last_answer is None)):
        pdf = export_pdf(last_answer or "")
        st.download_button("Baixar PDF", pdf, "neoenergia_qa.pdf", mime="application/pdf")
with col2:
    if st.button("⬇️ Exportar Word", disabled=(last_answer is None)):
        docx = export_docx(last_answer or "")
        st.download_button("Baixar DOCX", docx, "neoenergia_qa.docx")
