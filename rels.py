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

# Exportações (simples)
from docx import Document
from docx.shared import Inches
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor

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

# ===========================================================
# Streamlit layout
# ===========================================================
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
.kpi {{
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 12px;
  padding: 14px;
  background: rgba(255,255,255,.03);
}}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ===========================================================
# Utilitários
# ===========================================================
@st.cache_data(show_spinner=False, ttl=300)
def load_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.content.decode("utf-8", errors="ignore")
    return pd.read_csv(io.StringIO(data))

@st.cache_data(show_spinner=False, ttl=1800)
def load_logo(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.content
    except Exception:
        return None

def to_iso(v) -> str:
    try:
        return date_parse(str(v), dayfirst=True).date().isoformat()
    except Exception:
        return ""

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

def safe_dt(series: pd.Series) -> pd.Series:
    # tenta parsear datas (inclui dd/mm/yyyy e yyyy-mm-dd)
    return pd.to_datetime(series.astype(str), errors="coerce", dayfirst=True)

def norm_text(s: str) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.lower().strip()

def chunk(s: str, max_chars=900) -> List[str]:
    if not isinstance(s, str): return []
    s = s.strip()
    if len(s) <= max_chars:
        return [s]
    parts = re.split(r"(?<=[.!?])\s+", s)
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

# ===========================================================
# Mapeamentos específicos do seu dataset
# ===========================================================
def choose_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_status_value(s: str) -> str:
    s = norm_text(s)
    if not s: return ""
    if any(k in s for k in ["encerrad", "fechad", "closed", "concluid", "implementad", "done", "aprovad"]):
        return "encerrado"
    if any(k in s for k in ["atras", "vencid", "overdue"]):
        return "atrasado"
    if any(k in s for k in ["andament", "in progress", "em_execucao", "em execucao"]):
        return "em_andamento"
    if any(k in s for k in ["abert", "open", "pendente"]):
        return "aberto"
    return s

def normalize_classification_value(s: str) -> str:
    s = norm_text(s)
    if not s:
        return ""
    if "alt" in s or "high" in s:
        return "Alta"
    if "med" in s or "medium" in s:
        return "Média"
    if "baix" in s or "low" in s:
        return "Baixa"
    return s.title()

# ===========================================================
# Corpus / busca (pra parte narrativa)
# ===========================================================
def build_corpus(dfh: pd.DataFrame, dff: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, r in dfh.iterrows():
        aud = str(r.get("aud_code", "")).strip()
        title = str(r.get("title", "")).strip()
        ano = str(r.get("ano", "")).strip()
        ci = to_iso(r.get("cronograma_inicio", ""))
        cf = to_iso(r.get("cronograma_final", ""))
        text = "\n".join([
            f"[{aud}] {title}",
            f"Ano: {ano}",
            f"Objetivo: {str(r.get('objetivo',''))}",
            f"Escopo: {str(r.get('escopo',''))}",
            f"Riscos: {str(r.get('risco_processo',''))}",
            f"Alcance: {str(r.get('alcance',''))}",
            f"Cronograma: início {ci} • fim {cf}",
        ])
        for ch in chunk(text):
            rows.append({"source_type": "HEAD", "aud_code": aud, "finding_id": "", "text": ch})

    # Findings
    for _, r in dff.iterrows():
        aud = str(r.get("aud_code", "")).strip()
        fid = str(r.get("finding_id", "")).strip()
        title = str(r.get("finding_title", "")).strip()
        rec = str(r.get("recommendation", "")).strip()
        status = str(r.get("status", "")).strip()
        owner = str(r.get("owner", "")).strip()
        due = to_iso(r.get("due_date", ""))
        clas = str(r.get("classification", "")).strip()
        ftext = str(r.get("finding_text", "")).strip()
        text = (
            f"[{aud} – {fid}] {title}\n"
            f"Classificação: {clas} | Status: {status} | Responsável: {owner} | Prazo: {due}\n"
            f"Recomendação: {rec}\n"
            f"Detalhe: {ftext}"
        )
        for ch in chunk(text):
            rows.append({"source_type": "FIND", "aud_code": aud, "finding_id": fid, "text": ch})

    return pd.DataFrame(rows)

def search_tf(question: str, corpus: pd.DataFrame, top_k: int) -> pd.DataFrame:
    vect = TfidfVectorizer(strip_accents="unicode", ngram_range=(1, 2))
    M = vect.fit_transform(corpus["text"].astype(str))
    qv = vect.transform([question])
    sim = cosine_similarity(qv, M).flatten()
    out = corpus.copy()
    out["score"] = sim
    return out.sort_values("score", ascending=False).head(top_k)

# ===========================================================
# OpenAI (só pra narrativa)
# ===========================================================
def format_context(results_df: pd.DataFrame, max_chars_total: int = 9000) -> str:
    if results_df is None or results_df.empty:
        return ""
    parts, total = [], 0
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
        return "❌ OPENAI_API_KEY não encontrada no Secrets do Streamlit."

    context = format_context(results_df, max_chars_total=9000)
    if not context.strip():
        return "Não encontrei trechos relevantes nos CSVs para responder com segurança."

    client = OpenAI(api_key=api_key)

    system = (
        "Você é um assistente de auditoria interna. Responda em PT-BR.\n"
        "Use APENAS o CONTEXTO fornecido.\n"
        "Se a resposta não estiver no contexto, diga claramente: 'Não encontrei nos arquivos'.\n"
        "Se citar algo, inclua a tag [HEAD|FIND | aud=...].\n"
        "Não invente números."
    )
    user = f"PERGUNTA:\n{question}\n\nCONTEXTO:\n{context}"

    try:
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
    except Exception as e:
        return f"🔥 ERRO OPENAI: {str(e)}"

# ===========================================================
# Camada “Executiva” (pandas) — aqui fica confiável
# ===========================================================
def compute_exec_tables(df_h: pd.DataFrame, df_f: pd.DataFrame) -> dict:
    out = {}

    # recomendação = finding_id único
    f = df_f.copy()
    f["finding_id"] = f.get("finding_id", "").astype(str).fillna("").str.strip()
    f = f[f["finding_id"] != ""].copy()

    # classificação (alta/média/baixa)
    class_col = choose_first_existing(f, ["classification", "classificacao", "criticidade", "prioridade", "severity"])
    if class_col is None:
        f["__class__"] = ""
    else:
        f["__class__"] = f[class_col].astype(str).fillna("").apply(normalize_classification_value)

    # status / atraso
    status_col = choose_first_existing(f, ["status", "status_da_constatacao", "estado_del_trabajo"])
    if status_col is None:
        f["__status__"] = ""
    else:
        f["__status__"] = f[status_col].astype(str).fillna("").apply(normalize_status_value)

    due_col = choose_first_existing(f, ["due_date", "end_date", "data_acordada_vencimento", "data_acordada__vencimento"])
    if due_col is None:
        f["__due__"] = pd.NaT
    else:
        f["__due__"] = safe_dt(f[due_col])

    today = pd.Timestamp(datetime.date.today())
    f["__overdue__"] = f["__due__"].notna() & (f["__due__"] < today) & (~f["__status__"].isin(["encerrado"]))

    # ===== KPIs =====
    qtd_trabalhos = int(df_h["aud_code"].nunique())
    qtd_recs = int(f["finding_id"].nunique())
    qtd_overdue = int(f.loc[f["__overdue__"], "finding_id"].nunique())

    dist_class = (f.groupby("__class__")["finding_id"].nunique()
                  .reset_index(name="qtd")
                  .sort_values("qtd", ascending=False))
    out["kpis"] = {
        "qtd_trabalhos": qtd_trabalhos,
        "qtd_recomendacoes": qtd_recs,
        "qtd_overdue": qtd_overdue,
        "dist_class": dist_class
    }

    # ===== Top 10 trabalhos críticos (por nº recomendações) =====
    # junta com HEAD pra pegar título e ano
    head_min = df_h[["aud_code", "title", "ano"]].copy()
    head_min["aud_code"] = head_min["aud_code"].astype(str).str.strip()

    by_aud = (f.groupby("aud_code")["finding_id"].nunique()
              .reset_index(name="qtd_recomendacoes")
              .merge(head_min, on="aud_code", how="left"))

    by_aud["ano"] = by_aud.get("ano", "").astype(str).fillna("")
    by_aud["title"] = by_aud.get("title", "").astype(str).fillna("")
    by_aud = by_aud.sort_values(["qtd_recomendacoes", "aud_code"], ascending=[False, True]).head(10)

    out["top10_trabalhos"] = by_aud[["aud_code", "title", "qtd_recomendacoes", "ano"]].rename(
        columns={"title": "descricao"}
    )

    # ===== Top 3 áreas (definição: company do HEAD, se existir; senão alcance; senão escopo) =====
    area_col = choose_first_existing(df_h, ["area_constatacao"])
    if area_col:
        aud_area = df_h[["aud_code", area_col]].copy()
        aud_area.columns = ["aud_code", "area"]
        aud_area["area"] = aud_area["area"].astype(str).fillna("").str.strip()
        tmp = f.merge(aud_area, on="aud_code", how="left")
        top3_areas = (tmp[tmp["area"] != ""]
                      .groupby("area")["finding_id"].nunique()
                      .reset_index(name="qtd_recomendacoes")
                      .sort_values("qtd_recomendacoes", ascending=False).head(3))
    else:
        top3_areas = pd.DataFrame(columns=["area", "qtd_recomendacoes"])
    out["top3_areas"] = top3_areas

    # ===== Top 3 auditores (definição: owner do FIND) =====
    owner_col = choose_first_existing(df_f, ["owner", "proprietario_da_constatacao", "organization_of_finding_response_owner"])
    if owner_col:
        tmp = f.copy()
        tmp["auditor"] = df_f.loc[f.index, owner_col].astype(str).fillna("").str.strip()
        top3_aud = (tmp[tmp["auditor"] != ""]
                    .groupby("auditor")["finding_id"].nunique()
                    .reset_index(name="qtd_recomendacoes")
                    .sort_values("qtd_recomendacoes", ascending=False).head(3))
    else:
        top3_aud = pd.DataFrame(columns=["auditor", "qtd_recomendacoes"])
    out["top3_auditores"] = top3_aud

    # ===== Overdue detalhado (opcional, pra drill down) =====
    out["overdue_table"] = f.loc[f["__overdue__"], ["aud_code", "finding_id", "__class__", "__status__", "__due__"]].copy()
    out["overdue_table"].rename(columns={"__class__": "classificacao", "__status__": "status", "__due__": "prazo"}, inplace=True)

    return out

# ===========================================================
# Router analítico: responder sem LLM quando der
# ===========================================================
def answer_analytic_question(q: str, df_h: pd.DataFrame, df_f: pd.DataFrame, exec_pack: dict) -> Optional[str]:
    qn = norm_text(q)

    # anos explícitos
    years = re.findall(r"\b(20\d{2})\b", qn)

    if ("quantos" in qn or "qtd" in qn) and ("trabalh" in qn or "aud_code" in qn or "aud code" in qn):
        df = df_h.copy()
        if years and "ano" in df.columns:
            df = df[df["ano"].astype(str).isin(years)]
        n = int(df["aud_code"].nunique())
        if years:
            return f"Quantidade de trabalhos (aud_code únicos) em {', '.join(years)}: **{n}**."
        return f"Quantidade total de trabalhos (aud_code únicos): **{n}**."

    if ("quantas" in qn or "qtd" in qn) and ("recomend" in qn or "constat" in qn or "finding" in qn):
        f = df_f.copy()
        f["finding_id"] = f.get("finding_id", "").astype(str).fillna("").str.strip()
        f = f[f["finding_id"] != ""].copy()
        if years and "ano" in df_h.columns:
            auds = df_h[df_h["ano"].astype(str).isin(years)]["aud_code"].astype(str).unique().tolist()
            f = f[f["aud_code"].astype(str).isin(auds)]
        n = int(f["finding_id"].nunique())
        if years:
            return f"Quantidade de recomendações/constatações (finding_id únicos) em {', '.join(years)}: **{n}**."
        return f"Quantidade total de recomendações/constatações (finding_id únicos): **{n}**."

    if "mais critico" in qn or "mais crítico" in qn or ("top" in qn and "trabalh" in qn):
        top10 = exec_pack["top10_trabalhos"]
        lines = ["Top trabalhos críticos (por nº de recomendações):"]
        for _, r in top10.iterrows():
            lines.append(f"- **{r['aud_code']}** — {r['descricao']} | **{int(r['qtd_recomendacoes'])}** recs | ano **{r['ano']}**")
        return "\n".join(lines)

    if "atras" in qn or "vencid" in qn or "overdue" in qn:
        n = exec_pack["kpis"]["qtd_overdue"]
        return f"Recomendações em atraso (prazo vencido e não encerradas): **{n}**."

    return None

# ===========================================================
# Carregar dados
# ===========================================================
with st.spinner("Carregando dados do GitHub..."):
    df_h = load_csv(HEAD_URL)
    df_f = load_csv(FIND_URL)

df_h = normalize_columns(df_h)
df_f = normalize_columns(df_f)

# HEAD colunas esperadas
if "aud_code" not in df_h.columns:
    st.error("O CSV de relatórios não contém a coluna 'aud_code'. Verifique HEAD_URL.")
    st.stop()
df_h["aud_code"] = df_h["aud_code"].astype(str).str.strip().str.upper()

# Você disse: considerar o campo 'ano' como referência principal
if "ano" not in df_h.columns:
    df_h["ano"] = ""

# FINDINGS canônicos
if "aud_code" not in df_f.columns:
    df_f = ensure_col(df_f, "aud_code", ["id_do_trabalho"], default="")
df_f["aud_code"] = df_f["aud_code"].astype(str).str.strip().str.upper()

df_f = ensure_col(df_f, "finding_id", ["finding_id"], default="")
df_f = ensure_col(df_f, "finding_title", ["nome_da_constatacao", "nome_da_constatao", "finding_title"], default="")
df_f = ensure_col(df_f, "recommendation",
                  ["descricao_do_plano_de_recomendacao", "descrio_do_plano_de_recomendao", "recommendation"], default="")
df_f = ensure_col(df_f, "status", ["status_da_constatacao", "estado_del_trabajo", "status"], default="")
df_f = ensure_col(df_f, "owner", [
    "proprietario_da_constatacao", "organization_of_finding_response_owner",
    "proprietario_da_resposta_descoberta", "proprietrio_da_constatao",
    "proprietrio_da_resposta__descoberta", "owner"
], default="")
df_f = ensure_col(df_f, "due_date", [
    "data_acordada_vencimento", "data_acordada__vencimento", "data_acordada_aprovada_atualmente", "end_date", "due_date"
], default="")
df_f = ensure_col(df_f, "finding_text", ["constatacao", "constatao", "resposta", "finding_text"], default="")

# classificação (alta/média/baixa) — mantém se existir, senão vazio
df_f = ensure_col(df_f, "classification", ["classification", "classificacao", "criticidade", "prioridade", "severity"], default="")

# limpeza
for c in ["finding_id", "finding_title", "recommendation", "status", "owner", "due_date", "finding_text", "classification"]:
    df_f[c] = df_f[c].fillna("").astype(str)

# ===========================================================
# Logo + Título
# ===========================================================
logo_bytes = load_logo(LOGO_URL)
if logo_bytes:
    st.image(logo_bytes, width=180)
st.title("📗 Consulta Relatórios de Auditoria — Visão Executiva")

# ===========================================================
# Filtros (Ano / Empresa / Título)
# ===========================================================
st.subheader("🔎 Filtros")
c1, c2, c3 = st.columns(3)

with c1:
    years_all = sorted(pd.Series(df_h["ano"]).replace("", pd.NA).dropna().astype(str).unique())
    f_year = st.multiselect("Ano (campo 'ano')", years_all, default=years_all[-1:] if years_all else [])

with c2:
    if "company" in df_h.columns:
        companies_all = sorted(pd.Series(df_h["company"]).replace("", pd.NA).dropna().astype(str).unique())
        f_company = st.multiselect("Empresa/Área (company)", companies_all)
    else:
        f_company = []

with c3:
    titles_all = sorted(pd.Series(df_h.get("title", "")).replace("", pd.NA).dropna().astype(str).unique())
    f_title = st.multiselect("Título do trabalho", titles_all)

heads_filt = df_h.copy()
if f_year:
    heads_filt = heads_filt[heads_filt["ano"].astype(str).isin([str(y) for y in f_year])]
if f_company and "company" in heads_filt.columns:
    heads_filt = heads_filt[heads_filt["company"].astype(str).isin([str(x) for x in f_company])]
if f_title and "title" in heads_filt.columns:
    heads_filt = heads_filt[heads_filt["title"].astype(str).isin([str(x) for x in f_title])]

aud_subset = set(heads_filt["aud_code"].astype(str).unique().tolist())

finds_filt = df_f.copy()
if aud_subset:
    finds_filt = finds_filt[finds_filt["aud_code"].astype(str).isin(aud_subset)]

# ===========================================================
# Resumo Executivo (pandas)
# ===========================================================
exec_pack = compute_exec_tables(heads_filt, finds_filt)

st.subheader("📌 Resumo Executivo")

k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Qtd de trabalhos (aud_code)", exec_pack["kpis"]["qtd_trabalhos"])
with k2:
    st.metric("Qtd de recomendações (finding_id únicos)", exec_pack["kpis"]["qtd_recomendacoes"])
with k3:
    st.metric("Recomendações em atraso", exec_pack["kpis"]["qtd_overdue"])

# Recs por classificação (Alta/Média/Baixa)
dist = exec_pack["kpis"]["dist_class"].copy()
dist = dist[dist["__class__"].astype(str).str.strip() != ""]
if dist.empty:
    st.info("Não encontrei coluna de classificação (Alta/Média/Baixa) nas constatações — ou está vazia.")
else:
    # garante ordem Alta, Média, Baixa se existir
    order = ["Alta", "Média", "Baixa"]
    dist["__ord__"] = dist["__class__"].apply(lambda x: order.index(x) if x in order else 999)
    dist = dist.sort_values(["__ord__", "qtd"], ascending=[True, False]).drop(columns=["__ord__"])
    st.write("**Recomendações por classificação**")
    st.dataframe(dist.rename(columns={"__class__": "classificacao"}), use_container_width=True)

# ===========================================================
# Rankings executivos
# ===========================================================
st.subheader("🔥 Top 10 trabalhos mais críticos (por nº de recomendações)")
st.dataframe(exec_pack["top10_trabalhos"], use_container_width=True)

cA, cB = st.columns(2)

with cA:
    st.subheader("🏢 Top 3 áreas com mais recomendações")
    st.dataframe(exec_pack["top3_areas"], use_container_width=True)

with cB:
    st.subheader("🧑‍💼 Top 3 auditores com mais recomendações")
    st.dataframe(exec_pack["top3_auditores"], use_container_width=True)

with st.expander("⏰ Ver lista de recomendações em atraso (drill down)"):
    st.dataframe(exec_pack["overdue_table"], use_container_width=True)

# ===========================================================
# Corpus para perguntas narrativas (RAG)
# ===========================================================
corpus = build_corpus(heads_filt, finds_filt)

# ===========================================================
# Chat
# ===========================================================
st.subheader("💬 Pergunte (visão executiva + OpenAI)")

show_sources = st.checkbox("Mostrar fontes (trechos enviados ao modelo)", value=False)

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Ex.: Quais trabalhos começam nos próximos 60 dias? / Quais os mais críticos em 2025?")

if q:
    # 1) tenta responder por pandas (confiável)
    analytic = answer_analytic_question(q, heads_filt, finds_filt, exec_pack)
    if analytic is not None:
        answer = analytic
        results = pd.DataFrame()
    else:
        # 2) narrativa via OpenAI com RAG
        results = search_tf(q, corpus, top_k=18)
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

            if show_sources and isinstance(msg[2], pd.DataFrame) and (not msg[2].empty):
                st.markdown("**Trechos enviados ao modelo:**")
                for _, r in msg[2].iterrows():
                    tag = f"[{r.get('source_type','')} | aud={r.get('aud_code','')} | finding={r.get('finding_id','')}]"
                    html = f"<div class='source'><b>{tag}</b><br>{str(r.get('text',''))[:520]}...</div>"
                    st.markdown(html, unsafe_allow_html=True)

# ===========================================================
# Exportações (última resposta)
# ===========================================================
st.subheader("📤 Exportar (última resposta do chat)")

def export_pdf(text: str, logo: Optional[bytes]) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    y = H - 50

    if logo:
        try:
            c.drawImage(ImageReader(io.BytesIO(logo)), 40, y - 40, width=150, height=40)
            y -= 60
        except Exception:
            pass

    c.setFont("Helvetica-Bold", 14)
    c.setFillColor(HexColor(NEO_BLUE))
    c.drawString(40, y, "Neoenergia — Resposta (Q&A)")
    y -= 20

    c.setFont("Helvetica", 10)
    c.setFillColor(HexColor("#FFFFFF"))
    for line in str(text or "").split("\n"):
        if y < 50:
            c.showPage()
            y = H - 50
        line = re.sub(r"\*\*|_", "", line)
        c.drawString(40, y, line[:160])
        y -= 14

    c.save()
    return buf.getvalue()

def export_docx(text: str, logo: Optional[bytes]) -> bytes:
    doc = Document()
    if logo:
        try:
            doc.add_picture(io.BytesIO(logo), width=Inches(2.2))
        except Exception:
            pass
    doc.add_heading("Neoenergia — Resposta (Q&A)", level=1)
    for line in str(text or "").split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|_", "", line))
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

last_answer = None
for msg in reversed(st.session_state.get("history", [])):
    if msg[0] == "assistant":
        last_answer = msg[1]
        break

b1, b2 = st.columns(2)
with b1:
    if st.button("⬇️ Exportar PDF", disabled=(last_answer is None)):
        pdf = export_pdf(last_answer or "", logo_bytes)
        st.download_button("Baixar PDF", pdf, "neoenergia_resposta.pdf", mime="application/pdf")
with b2:
    if st.button("⬇️ Exportar Word", disabled=(last_answer is None)):
        docx = export_docx(last_answer or "", logo_bytes)
        st.download_button("Baixar DOCX", docx, "neoenergia_resposta.docx")
