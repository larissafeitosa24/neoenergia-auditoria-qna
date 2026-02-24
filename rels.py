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

st.set_page_config(page_title="Neoenergia • Consulta Relatórios de Auditoria", page_icon="📗", layout="wide")

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
    if not isinstance(s, str): return []
    if len(s) <= max_chars: return [s]
    parts = re.split(r"(?<=[.!?])\s+", s.strip())
    out, buf = [], ""
    for p in parts:
        if len(buf) + len(p) + 1 <= max_chars:
            buf = (buf + " " + p).strip()
        else:
            if buf: out.append(buf)
            buf = p
    if buf: out.append(buf)
    return out

# ----------------------- Normalização e mapeamento -----------------------
def _normalize_col(s: str) -> str:
    s = str(s or "").strip()
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    s = re.sub(r'\s+', '_', s.replace('-', '_'))
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
            rows.append({"source_type":"HEAD","aud_code":aud,"finding_id":"","text":ch})

    # FINDINGS
    for _, r in dff.iterrows():
        aud  = str(r["aud_code"])
        fid  = str(r.get("finding_id",""))
        title= str(r.get("finding_title",""))
        rec  = str(r.get("recommendation",""))
        imp  = str(r.get("impact",""))
        status = str(r.get("status",""))
        owner  = str(r.get("owner",""))
        due    = to_iso(r.get("due_date",""))
        ftext  = str(r.get("finding_text",""))
        text = f"[{aud} – {fid}] Constatação: {title} — Impacto: {imp} — Recomendação: {rec} — Status: {status} — Resp.: {owner} — Prazo: {due}\n{ftext}"
        for ch in chunk(text):
            rows.append({"source_type":"FIND","aud_code":aud,"finding_id":fid,"text":ch})

    return pd.DataFrame(rows)

def search_tf(question: str, corpus: pd.DataFrame, top_k: int) -> pd.DataFrame:
    vect = TfidfVectorizer(strip_accents="unicode", ngram_range=(1,2))
    M = vect.fit_transform(corpus["text"])
    qv = vect.transform([question])
    sim = cosine_similarity(qv, M).flatten()
    out = corpus.copy()
    out["score"] = sim
    return out.sort_values("score", ascending=False).head(top_k)

def _counts_by_aud_in_results(contexts_df: pd.DataFrame) -> pd.DataFrame:
    finds = contexts_df[contexts_df["source_type"]=="FIND"].copy()
    if finds.empty:
        return pd.DataFrame(columns=["aud_code","qtd_findings"])
    g = finds.groupby("aud_code")["finding_id"].nunique().reset_index(name="qtd_findings")
    return g.sort_values(["qtd_findings","aud_code"], ascending=[False, True])

def build_answer(contexts: pd.DataFrame) -> str:
    heads = contexts[contexts["source_type"]=="HEAD"]
    finds = contexts[contexts["source_type"]=="FIND"]

    out = []
    if len(heads) > 0:
        h = heads.iloc[0]
        out.append(f"**Resumo do relatório {h['aud_code']}:**")
        for line in h["text"].split("\n"):
            out.append(f"- {line}")
        out.append("")

    if len(finds) > 0:
        out.append("**Constatações e Recomendações (principais):**")
        for _, r in finds.head(5).iterrows():
            tag = f"[{r['aud_code']} – {r['finding_id']}]"
            out.append(f"- {r['text'][:300]}... _(Fonte: {tag})_")
        out.append("")
        out.append("**Contagem de constatações por relatório (no resultado retornado):**")
        cnt_df = _counts_by_aud_in_results(contexts)
        for _, rr in cnt_df.iterrows():
            out.append(f"- **{rr['aud_code']}**: {int(rr['qtd_findings'])} constatação(ões)")
    return "\n".join(out)

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
for c in ["classification","title","company","objetivo","escopo","risco_processo","alcance",
          "cronograma_inicio","cronograma_draft","cronograma_final","emission_date","ano","mes"]:
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
# aud_code derivado de id_do_trabalho, se necessário
if "aud_code" not in df_f.columns:
    df_f = ensure_col(df_f, "aud_code", ["id_do_trabalho"], default="")
df_f["aud_code"] = df_f["aud_code"].astype(str).str.strip().str.upper()

df_f = ensure_col(df_f, "finding_id", ["finding_id"], default="")
df_f = ensure_col(df_f, "finding_title", ["nome_da_constatacao","nome_da_constatao"], default="")
df_f = ensure_col(df_f, "recommendation",
                  ["descricao_do_plano_de_recomendacao", "descrio_do_plano_de_recomendao", "recommendation"], default="")
df_f = ensure_col(df_f, "status", ["status_da_constatacao","estado_del_trabajo","status"], default="")
df_f = ensure_col(df_f, "owner", [
    "proprietario_da_constatacao","organization_of_finding_response_owner",
    "proprietario_da_resposta_descoberta","proprietrio_da_constatao","proprietrio_da_resposta__descoberta","owner"
], default="")
df_f = ensure_col(df_f, "due_date", [
    "data_acordada_vencimento","data_acordada__vencimento","data_acordada_aprovada_atualmente","end_date","due_date"
], default="")
df_f = ensure_col(df_f, "finding_text", ["constatacao","constatao","resposta","finding_text"], default="")
df_f = ensure_col(df_f, "tema", ["negocio_associado","negcio_associado","compromissos_da_auditoria","tema"], default="")
df_f = ensure_col(df_f, "impact", ["impact"], default="")
df_f = enrich_impact(df_f)

for c in ["status","tema","impact","recommendation","finding_title","finding_text","owner","due_date"]:
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
    f_title   = st.multiselect("Título do trabalho", sorted(pd.Series(df_h["title"]).dropna().astype(str).unique()))
with cols[1]:
    f_risk    = st.multiselect("Risco / Impacto (constatações)", sorted(pd.Series(df_f["risk_filter"]).replace("", pd.NA).dropna().unique()))
with cols[2]:
    f_company = st.multiselect("Empresa", sorted(pd.Series(df_h["company"]).dropna().astype(str).unique())) if "company" in df_h.columns else []
with cols[3]:
    f_year    = st.multiselect("Ano", sorted(pd.Series(df_h["ano_filter"]).replace("","Sem ano").unique()))

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
        ((filtered_corpus["source_type"] == "FIND") & (filtered_corpus["finding_id"].isin(valid_finds))) |
        (filtered_corpus["source_type"] == "HEAD")
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
    cnt = (filt_finds
           .assign(finding_id=filt_finds["finding_id"].fillna("").astype(str))
           .query("finding_id != ''")
           .groupby("aud_code")["finding_id"].nunique()
           .reset_index(name="qtd_findings")
           .sort_values(["qtd_findings","aud_code"], ascending=[False, True]))
    if cnt.empty:
        st.write("Nenhuma constatação no filtro atual.")
    else:
        st.dataframe(cnt, use_container_width=True)

# ===========================================================
# NLQ — Intents & helpers
# ===========================================================
def _norm_text(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    return s.lower().strip()

_PT_MONTHS = {
    "janeiro":1, "fevereiro":2, "marco":3, "março":3, "abril":4, "maio":5, "junho":6,
    "julho":7, "agosto":8, "setembro":9, "outubro":10, "novembro":11, "dezembro":12
}
def _extract_years(q: str) -> List[str]:  return sorted(set(re.findall(r"\b(20[0-3]\d)\b", q)))
def _extract_months(q: str) -> List[int]:
    months = []
    for name, num in _PT_MONTHS.items():
        if re.search(rf"\b{name}\b", q):
            months.append(num)
    return sorted(set(months))

def _extract_relative_time(q: str):
    this_month = bool(re.search(r"\b(este|neste|nesse)\s+mes\b", q))
    this_year  = bool(re.search(r"\b(este|neste|nesse)\s+ano\b", q))
    return this_month, this_year

def _normalize_status_value(s: str):
    s = _norm_text(s)
    if not s: return ""
    if any(k in s for k in ["encerrad","fechad","closed","concluid","implementad","done","aprovad"]): return "encerrado"
    if "atras" in s or "vencid" in s: return "atrasado"
    if "andament" in s or "in progress" in s: return "em_andamento"
    if "abert" in s or "open" in s or "pendente" in s: return "aberto"
    return s

def _extract_companies(q: str, df_head: pd.DataFrame) -> List[str]:
    qn = _norm_text(q)
    if "company" not in df_head.columns: return []
    detected = []
    for comp in sorted(set(df_head["company"].dropna().astype(str))):
        if comp and _norm_text(comp) in qn:
            detected.append(comp)
    return detected

def _extract_risks(q: str, df_find: pd.DataFrame) -> List[str]:
    qn = _norm_text(q)
    detected = []
    uniq = sorted(set(df_find["risk_filter"].dropna().astype(str)))
    for r in uniq:
        if r and _norm_text(r) in qn:
            detected.append(r)
    return detected

def _find_related_auds(question: str, df_head: pd.DataFrame, df_find: pd.DataFrame, _corpus: pd.DataFrame,
                       top_k=10, companies: Optional[List[str]]=None) -> List[str]:
    head_corpus = _corpus[_corpus["source_type"]=="HEAD"]
    if not head_corpus.empty:
        res = search_tf(question, head_corpus, top_k=top_k)
        auds = res["aud_code"].tolist()
        seen, ordered = set(), []
        for a in auds:
            if a not in seen:
                ordered.append(a); seen.add(a)
        if ordered:
            if companies and "company" in df_head.columns:
                valids = set(df_head[df_head["company"].isin(companies)]["aud_code"])
                ordered = [a for a in ordered if a in valids]
            return ordered
    # fallback: achar pelos textos de findings
    tmp = df_find.copy()
    tmp["__text__"] = (
        tmp.get("finding_title","").astype(str) + " " +
        tmp.get("finding_text","").astype(str)  + " " +
        tmp.get("tema","").astype(str)         + " " +
        tmp.get("impact","").astype(str)
    )
    tmp_corpus = pd.DataFrame({"text": tmp["__text__"], "aud_code": tmp["aud_code"], "finding_id": tmp["finding_id"]})
    if tmp_corpus["text"].str.strip().astype(bool).any():
        res2 = search_tf(question, tmp_corpus, top_k=top_k)
        auds2 = res2["aud_code"].tolist()
        seen, ordered2 = set(), []
        for a in auds2:
            if a not in seen:
                ordered2.append(a); seen.add(a)
        if companies and "company" in df_head.columns:
            valids = set(df_head[df_head["company"].isin(companies)]["aud_code"])
            ordered2 = [a for a in ordered2 if a in valids]
        return ordered2
    return []

def _apply_year_month_filters(df_head: pd.DataFrame, years: List[str], months: List[int],
                              this_month=False, this_year=False) -> pd.DataFrame:
    out = df_head.copy()
    today = datetime.date.today()
    if this_year and "ano_filter" in out.columns:
        out = out[out["ano_filter"].astype(str) == str(today.year)]
    elif this_year and "emission_date" in out.columns:
        ed = pd.to_datetime(out["emission_date"], errors="coerce")
        out = out[ed.dt.year == today.year]

    if this_month and "emission_date" in out.columns:
        ed = pd.to_datetime(out["emission_date"], errors="coerce")
        out = out[(ed.dt.year == today.year) & (ed.dt.month == today.month)]

    if years:
        if "ano_filter" in out.columns:
            out = out[out["ano_filter"].astype(str).isin(years)]
        elif "emission_date" in out.columns:
            ed = pd.to_datetime(out["emission_date"], errors="coerce")
            out = out[ed.dt.year.astype("Int64").astype(str).isin(years)]
    if months and "emission_date" in out.columns:
        ed = pd.to_datetime(out["emission_date"], errors="coerce")
        out = out[ed.dt.month.isin(months)]
    return out

# ===========================================================
# Respostas EXECUTIVAS (templates + intents novos)
# ===========================================================
def exec_response(title: str, resumo: str,
                  detalhes: Optional[List[str]] = None,
                  fonte: Optional[str] = None) -> str:
    txt = f"### {title}\n\n**{resumo}**\n\n"
    if detalhes:
        txt += "#### 📌 Detalhes\n"
        for d in detalhes:
            txt += f"- {d}\n"
        txt += "\n"
    if fonte:
        txt += f"#### 📎 Fonte\n{fonte}\n"
    return txt

# 1) relatórios por ano
def _exec_count_reports_by_year(q: str, df_head: pd.DataFrame):
    anos = re.findall(r"(20\d{2})", q)
    if not anos:
        return None
    ano = anos[0]
    if "ano_filter" in df_head.columns:
        count = int((df_head["ano_filter"].astype(str) == ano).sum())
    else:
        ed = pd.to_datetime(df_head["emission_date"], errors="coerce")
        count = int((ed.dt.year.astype(str) == ano).sum())
    return exec_response(
        "📊 Quantidade de relatórios",
        f"Foram emitidos **{count} relatórios** no ano de **{ano}**.",
        ["Este total considera todos os tipos existentes no CSV."],
        "relatorios.csv → ano/ano_filter, emission_date"
    ), None

# 2) recomendações por nome do relatório
def _exec_count_recs_by_report_name(q: str, df_head: pd.DataFrame, df_find: pd.DataFrame):
    m = re.search(r"(?:trabalho|relat[oó]rio)\s+(.+)", _norm_text(q))
    if not m:
        return None
    name = m.group(1).strip()
    match = df_head[df_head["title"].str.lower().str.contains(name)]
    if match.empty:
        return "Não encontrei relatório com nome semelhante a '{}'.".format(name), None
    auds = match["aud_code"].tolist()
    rec_count = int(df_find[df_find["aud_code"].isin(auds)]["finding_id"].nunique())
    detalhes = ["AUD(s): {}{}".format(", ".join(auds[:8]), "…" if len(auds)>8 else "")]
    return exec_response(
        "📌 Contagem de recomendações",
        f"O relatório **{match.iloc[0]['title']}** possui **{rec_count} recomendações/constatações**.",
        detalhes,
        "constatacoes.csv → finding_id • relatorios.csv → title"
    ), None

# 3) relatório com mais constatações
def _exec_report_with_most_findings(df_find: pd.DataFrame, df_head: pd.DataFrame):
    counts = (df_find[df_find["finding_id"]!=""]
              .groupby("aud_code")["finding_id"].nunique()
              .reset_index(name="qtd").sort_values("qtd", ascending=False))
    if counts.empty:
        return "Não há constatações registradas.", None
    top = counts.iloc[0]
    aud = top["aud_code"]
    qtd = int(top["qtd"])
    title = df_head[df_head["aud_code"]==aud]["title"].astype(str).head(1).fillna("(sem título)").iloc[0]
    return exec_response(
        "🏆 Relatório com mais constatações",
        f"O relatório **{aud} — {title}** teve **{qtd} pontos**.",
        ["Ranking por finding_id único."],
        "constatacoes.csv → grouping por aud_code"
    ), None

# 4) repetição de trabalhos por ano
def _exec_repeated_reports(df_head: pd.DataFrame):
    g = df_head.groupby("title")["ano_filter"].nunique().reset_index()
    rep = g[g["ano_filter"] > 1]
    if rep.empty:
        return "Não houve repetição de trabalhos entre os anos analisados.", None
    linhas = []
    for _, r in rep.iterrows():
        anos = df_head[df_head["title"]==r["title"]]["ano_filter"].astype(str).unique().tolist()
        anos = ", ".join(sorted([x for x in anos if x and x!='<NA>']))
        linhas.append(f"**{r['title']}** repetido nos anos: {anos}")
    return exec_response(
        "🔁 Trabalhos repetidos",
        f"Foram identificados **{len(rep)} trabalhos** com repetição anual.",
        linhas[:10],
        "relatorios.csv → title x ano_filter"
    ), None

# 5) empresa com mais constatações
def _exec_company_with_most_findings(df_head: pd.DataFrame, df_find: pd.DataFrame):
    merged = df_find[df_find["finding_id"]!=""].merge(df_head[["aud_code","company"]], on="aud_code", how="left")
    g = merged.groupby("company")["finding_id"].nunique().reset_index(name="qtd").sort_values("qtd", ascending=False)
    if g.empty:
        return "Não há constatações registradas.", None
    top = g.iloc[0]
    det = [", ".join(["{} ({})".format(r.company, int(r.qtd)) for r in g.head(5).itertuples(index=False)])]
    return exec_response(
        "🏢 Empresa mais impactada",
        f"A empresa com mais constatações é **{top['company']}** com **{int(top['qtd'])} achados**.",
        det,
        "constatacoes.csv + relatorios.csv → company"
    ), None

# 6) riscos mais recorrentes
def _exec_top_risks(df_find: pd.DataFrame):
    g = (df_find[df_find["risk_filter"]!=""]
         .groupby("risk_filter")["finding_id"].nunique()
         .reset_index(name="qtd").sort_values("qtd", ascending=False))
    if g.empty:
        return "Não há riscos recorrentes mapeados.", None
    top = g.iloc[0]
    det = [", ".join(["{} ({})".format(r.risk_filter, int(r.qtd)) for r in g.head(5).itertuples(index=False)])]
    return exec_response(
        "⚠️ Riscos mais recorrentes",
        f"O risco mais recorrente é **{top['risk_filter']}**, com **{int(top['qtd'])} constatações**.",
        det,
        "constatacoes.csv → risk_filter"
    ), None

# 7) status geral das recomendações
def _exec_global_status(df_find: pd.DataFrame):
    df = df_find.copy()
    df["__status_norm__"] = df.get("status","").astype(str).apply(_normalize_status_value)
    dist = df["__status_norm__"].value_counts().to_dict()
    if not dist:
        return "Não há recomendações mapeadas no CSV.", None
    detalhes = [
        f"{dist.get('encerrado',0)} encerradas",
        f"{dist.get('em_andamento',0)} em andamento",
        f"{dist.get('aberto',0)} em aberto",
        f"{dist.get('atrasado',0)} atrasadas",
    ]
    return exec_response(
        "📊 Status geral das recomendações",
        "Distribuição consolidada das recomendações por status.",
        detalhes,
        "constatacoes.csv → status"
    ), None

# 8) tempo médio de execução dos trabalhos
def _exec_average_report_duration(df_head: pd.DataFrame):
    if not {"cronograma_inicio","cronograma_final"} <= set(df_head.columns):
        return None
    df = df_head.copy()
    df["dt_i"] = pd.to_datetime(df["cronograma_inicio"], errors="coerce")
    df["dt_f"] = pd.to_datetime(df["cronograma_final"], errors="coerce")
    df["dur"]  = (df["dt_f"] - df["dt_i"]).dt.days
    avg = df["dur"].dropna().mean()
    if not pd.notnull(avg):
        return None
    det = [f"Menor duração: {int(df['dur'].min())} dias", f"Maior duração: {int(df['dur'].max())} dias"]
    return exec_response(
        "⏱️ Tempo médio de execução",
        f"O tempo médio de duração dos trabalhos é de **{avg:.1f} dias**.",
        det,
        "relatorios.csv → cronograma_inicio/cronograma_final"
    ), None

# 9) top relatórios críticos (maior volume de constatações)
def _exec_top_critical_reports(df_find: pd.DataFrame, df_head: pd.DataFrame):
    g = (df_find[df_find["finding_id"]!=""]
         .groupby("aud_code")["finding_id"].nunique()
         .reset_index(name="qtd").sort_values("qtd", ascending=False).head(5))
    if g.empty:
        return "Não há dados para ranking de relatórios críticos.", None
    detalhes = []
    for r in g.itertuples(index=False):
        title = df_head[df_head["aud_code"]==r.aud_code]["title"].astype(str).head(1).fillna("(sem título)").iloc[0]
        detalhes.append(f"{r.aud_code} — {title}: {int(r.qtd)} constatações")
    return exec_response(
        "🔥 Relatórios mais críticos",
        "Top relatórios por volume de constatações.",
        detalhes,
        "constatacoes.csv + relatorios.csv"
    ), None

# ------------------- Intents anteriores (operacionais) -------------------
def _answer_count_recommendations(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)
    risks = _extract_risks(q, df_ff)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=12, companies=companies)
    if not auds:
        return "Não encontrei trabalhos relacionados ao tema da sua pergunta.", None
    head = _apply_year_month_filters(df_hh[df_hh["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    df_find = df_ff[df_ff["aud_code"].isin(auds_final)].copy()
    if risks:
        df_find = df_find[df_find["risk_filter"].isin(risks)]
    df_find["recommendation"] = df_find["recommendation"].fillna("").astype(str).str.strip()
    total = int((df_find["recommendation"]!="").sum())

    t_trab = "trabalho" if len(auds_final)==1 else "trabalhos"
    t_rec  = "recomendação" if total==1 else "recomendações"
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    ans = f"Foram identificados {len(auds_final)} {t_trab}{anos_txt}, com {total} {t_rec} no total."
    res = search_tf(q, _corpus, top_k=12)
    return ans, res

def _answer_overdue(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)
    risks = _extract_risks(q, df_ff)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para verificar atrasos.", None
    head = _apply_year_month_filters(df_hh[df_hh["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    today = datetime.date.today()
    dd = pd.to_datetime(df_ff["due_date"], errors="coerce").dt.date if "due_date" in df_ff.columns else None
    status_norm = df_ff.get("status","").astype(str).apply(_normalize_status_value)

    mask_base = df_ff["aud_code"].isin(auds_final)
    mask_due  = dd.notna() & (dd < today) if dd is not None else False
    mask_open = ~status_norm.isin(["encerrado"])
    mask_risk = df_ff["risk_filter"].isin(risks) if risks else True

    overdue = df_ff[mask_base & mask_due & mask_open & mask_risk]
    n = len(overdue)
    t = "recomendação" if n==1 else "recomendações"
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    ans = f"Há {n} {t} em atraso para os trabalhos identificados{anos_txt}."
    res = search_tf(q, _corpus, top_k=12)
    return ans, res

def _answer_status_breakdown(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)
    risks = _extract_risks(q, df_ff)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para consolidar status.", None
    head = _apply_year_month_filters(df_hh[df_hh["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    df_find = df_ff[df_ff["aud_code"].isin(auds_final)].copy()
    if risks:
        df_find = df_find[df_find["risk_filter"].isin(risks)]
    df_find["__status_norm__"] = df_find.get("status","").astype(str).apply(_normalize_status_value)
    dist = df_find["__status_norm__"].value_counts().to_dict()
    if not dist:
        return "Não há recomendações registradas para esses trabalhos.", None
    parts = []
    label_map = {"encerrado":"encerradas", "aberto":"em aberto", "em_andamento":"em andamento", "atrasado":"atrasadas"}
    for k in ["encerrado","em_andamento","aberto","atrasado"]:
        if k in dist:
            parts.append(f"{dist[k]} {label_map.get(k,k)}")
    ans = "Distribuição de status: " + ", ".join(parts) + "."
    res = search_tf(q, _corpus, top_k=12)
    return ans, res

def _answer_responsibles(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)
    risks = _extract_risks(q, df_ff)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para listar responsáveis.", None
    head = _apply_year_month_filters(df_hh[df_hh["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    df_find = df_ff[df_ff["aud_code"].isin(auds_final)].copy()
    if risks:
        df_find = df_find[df_find["risk_filter"].isin(risks)]
    df_find["owner"] = df_find.get("owner","").fillna("").astype(str).str.strip()

    top = (df_find.query("owner != ''")
                 .groupby("owner")["finding_id"].nunique()
                 .reset_index(name="qtd")
                 .sort_values(["qtd","owner"], ascending=[False, True]).head(5))
    if top.empty:
        return "Não há responsáveis mapeados para esses trabalhos.", None
    itens = "; ".join([f"{r.owner}: {int(r.qtd)}" for r in top.itertuples(index=False)])
    ans = f"Principais responsáveis (por nº de recomendações): {itens}."
    res = search_tf(q, _corpus, top_k=12)
    return ans, res

def _answer_count_trabalhos(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=12, companies=companies)
    if not auds:
        return "Não encontrei trabalhos relacionados ao tema da sua pergunta.", None
    head = _apply_year_month_filters(df_hh[df_hh["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    n = head["aud_code"].nunique()
    if n == 0:
        return "Não encontrei trabalhos para o período solicitado.", None
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    t = "trabalho" if n==1 else "trabalhos"
    ans = f"Foram identificados {n} {t}{anos_txt}."
    res = search_tf(q, _corpus, top_k=12)
    return ans, res

def _answer_list_trabalhos(q, df_hh, df_ff, _corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_hh)

    auds = _find_related_auds(q, df_hh, df_ff, _corpus, top_k=15, companies=companies)
    if not auds:
        return "Não encontrei trabalhos relacionados ao tema da sua pergunta.", None

    head = df_hh[df_hh["aud_code"].isin(auds)].copy()
    head = _apply_year_month_filters(head, yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    head["__ord__"] = head["aud_code"].apply(lambda x: auds.index(x) if x in auds else 9999)
    head = head.sort_values(["__ord__","aud_code"]).drop(columns=["__ord__"], errors="ignore")

    linhas = []
    for _, r in head.head(12).iterrows():
        aud = r["aud_code"]
        title = str(r.get("title","")).strip() or "(sem título)"
        company = str(r.get("company","")).strip()
        ano = str(r.get("ano_filter","")).strip()
        meta = []
        if company: meta.append(company)
        if ano: meta.append(ano)
        meta_txt = f" ({', '.join(meta)})" if meta else ""
        linhas.append(f"- **{aud}** — {title}{meta_txt}")

    ans = "Trabalhos identificados:\n" + "\n".join(linhas)
    results = search_tf(q, _corpus, top_k=12)
    return ans, results

# ---------- Router principal (EXEC + operacionais) ----------
def try_natural_answer(question: str, df_head: pd.DataFrame, df_find: pd.DataFrame, _corpus: pd.DataFrame):
    q = _norm_text(question)

    # -------- INTENTS EXEC --------
    ans = _exec_count_reports_by_year(q, df_head)
    if ans:
        return ans

    ans = _exec_count_recs_by_report_name(q, df_head, df_find)
    if ans:
        return ans

    if "mais pontos" in q or "mais constatacao" in q or "mais achado" in q:
        return _exec_report_with_most_findings(df_find, df_head)

    if "repeticao" in q or "repetição" in q or "repetidos" in q:
        return _exec_repeated_reports(df_head)

    if "empresa" in q and "mais" in q and ("constat" in q or "achad" in q):
        return _exec_company_with_most_findings(df_head, df_find)

    if "riscos mais" in q or ("mais" in q and "risco" in q):
        return _exec_top_risks(df_find)

    if "status geral" in q or ("distribuicao" in q and "status" in q):
        return _exec_global_status(df_find)

    if "tempo medio" in q or "duracao media" in q:
        return _exec_average_report_duration(df_head)

    if "relatorios criticos" in q or ("mais criticos" in q):
        return _exec_top_critical_reports(df_find, df_head)

    # -------- INTENTS OPERACIONAIS --------
    ans, res = _answer_count_recommendations(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    ans, res = _answer_overdue(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    ans, res = _answer_status_breakdown(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    ans, res = _answer_responsibles(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    ans, res = _answer_count_trabalhos(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    ans, res = _answer_list_trabalhos(question, df_head, df_find, _corpus)
    if ans:
        return ans, res

    # -------- FALLBACK SEMÂNTICO --------
    head_corpus = _corpus[_corpus["source_type"]=="HEAD"]
    if not head_corpus.empty:
        res = search_tf(question, head_corpus, top_k=1)
        if len(res):
            aud = res.iloc[0]["aud_code"]
            hr = df_head[df_head["aud_code"]==aud].head(1)
            if len(hr):
                r0 = hr.iloc[0]
                cronoi = to_iso(r0.get("cronograma_inicio",""))
                cronof = to_iso(r0.get("cronograma_final",""))
                ans = (f"O trabalho {aud} trata de {r0.get('title','(sem título)')}. "
                       f"Objetivo: {r0.get('objetivo','')}. Escopo: {r0.get('escopo','')}. "
                       f"Cronograma: início {cronoi} • fim {cronof}.")
                results = search_tf(question, _corpus, top_k=12)
                return ans, results
    return None, None

# ===========================================================
# Chat
# ===========================================================
st.subheader("💬 Pergunte sobre os relatórios")
show_sources = st.checkbox("Mostrar fontes (trechos)", value=False)

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Digite sua pergunta...")

if q:
    nat_ans, nat_results = try_natural_answer(q, df_h, df_f, filtered_corpus)
    if nat_ans is not None:
        answer  = nat_ans
        results = nat_results if nat_results is not None else search_tf(q, filtered_corpus, top_k=8)
    else:
        results = search_tf(q, filtered_corpus, top_k=12)
        answer  = build_answer(results)
    st.session_state["history"].append(("user", q))
    st.session_state["history"].append(("assistant", answer, results))

for msg in st.session_state["history"]:
    if msg[0] == "user":
        with st.chat_message("user"): st.write(msg[1])
    else:
        with st.chat_message("assistant"):
            st.write(msg[1])
            if show_sources:
                st.markdown("**Trechos utilizados:**")
                for _, r in msg[2].iterrows():
                    tag = f"[{r['aud_code']} – {r['finding_id']}]"
                    html = f"<div class='source'><b>{tag}</b><br>{r['text'][:500]}...</div>"
                    st.markdown(html, unsafe_allow_html=True)

# ===========================================================
# Exportações
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
    for line in text.split("\n"):
        if y < 50:
            c.showPage(); y = H-50
        line = re.sub(r"\*\*|_", "", line)
        c.drawString(40, y, line[:1200]); y -= 14
    c.save()
    return buf.getvalue()

def export_pdf_detailed(df_head, df_find, results_df, logo_bytes=None):
    res_finds = results_df[results_df["source_type"]=="FIND"][["aud_code","finding_id"]].drop_duplicates()
    if res_finds.empty:
        return export_pdf("Nenhuma constatação no resultado atual para exportar.")

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_find.set_index(["aud_code","finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code","status","finding_id"], na_position="last")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=48, bottomMargin=36)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('TitleNeo', parent=styles['Heading1'], textColor=colors.HexColor(NEO_BLUE))
    h2_style    = ParagraphStyle('H2Neo',    parent=styles['Heading2'], textColor=colors.HexColor(NEO_BLUE))
    normal      = styles['BodyText']
    story = []

    if logo_bytes:
        story.append(RLImage(io.BytesIO(logo_bytes), width=180, height=48)); story.append(Spacer(1, 12))
    story.append(Paragraph("Neoenergia — Q&A de Relatórios (Export Detalhado)", title_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(datetime.datetime.now().strftime("%d/%m/%Y %H:%M"), normal))
    story.append(PageBreak())

    for aud, df_aud in aux.groupby("aud_code", sort=False):
        story.append(Paragraph(f"Relatório: {aud}", h2_style))
        head_row = df_head[df_head["aud_code"]==aud].head(1)
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
            for line in resumo: story.append(Paragraph(line, normal))
            story.append(Spacer(1, 6))

        tbl_data = [["Finding ID","Título","Impacto (priorizado)","Recomendação","Status","Responsável","Prazo"]]
        for _, r in df_aud.iterrows():
            tbl_data.append([
                str(r.get("finding_id","")),
                str(r.get("finding_title","")),
                str(r.get("impact","")),
                str(r.get("recommendation","")),
                str(r.get("status","")),
                str(r.get("owner","")),
                to_iso(r.get("due_date",""))
            ])
        table = Table(tbl_data, repeatRows=1, colWidths=[60,120,110,110,60,80,60])
        table_style = TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor(NEO_BLUE)),
            ('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('ALIGN',(0,0),(-1,0),'CENTER'),
            ('GRID',(0,0),(-1,-1),0.5,colors.grey),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
        ])
        table.setStyle(table_style)
        story.append(table); story.append(Spacer(1,8))

        for _, r in df_aud.iterrows():
            story.append(Paragraph(f"**[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}**", normal))
            ftext = str(r.get("finding_text","") or "")
            if not ftext.strip(): ftext = "(Sem descrição detalhada.)"
            story.append(Paragraph(ftext.replace("\n","<br/>"), normal))
            story.append(Spacer(1,6))
        story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()

def export_docx(text):
    doc = Document()
    if logo_bytes:
        try: doc.add_picture(io.BytesIO(logo_bytes), width=Inches(2.2))
        except Exception: pass
    doc.add_heading("Neoenergia — Q&A de Relatórios", level=1)
    for line in text.split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|_", "", line))
    out = io.BytesIO(); doc.save(out); return out.getvalue()

def export_docx_detailed(df_head, df_find, results_df, logo_bytes=None):
    res_finds = results_df[results_df["source_type"]=="FIND"][["aud_code","finding_id"]].drop_duplicates()
    if res_finds.empty:
        d = Document(); d.add_heading("Neoenergia — Q&A (Export Detalhado)", level=1)
        d.add_paragraph("Nenhuma constatação no resultado atual para exportar.")
        out = io.BytesIO(); d.save(out); return out.getvalue()

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_find.set_index(["aud_code","finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code","status","finding_id"], na_position="last")

    d = Document()
    if logo_bytes:
        try: d.add_picture(io.BytesIO(logo_bytes), width=Inches(2.2))
        except Exception: pass
    d.add_heading("Neoenergia — Q&A de Relatórios (Export Detalhado)", level=1)
    d.add_paragraph(datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))

    first_aud = True
    for aud, df_aud in aux.groupby("aud_code", sort=False):
        if not first_aud: d.add_page_break()
        first_aud = False

        d.add_heading(f"Relatório: {aud}", level=2)
        head_row = df_head[df_head["aud_code"]==aud].head(1)
        if len(head_row):
            hr = head_row.iloc[0]
            d.add_paragraph(f"Título: {hr.get('title','')}")
            d.add_paragraph(f"Empresa: {hr.get('company','')}")
            d.add_paragraph(f"Objetivo: {hr.get('objetivo','')}")
            d.add_paragraph(f"Escopo: {hr.get('escopo','')}")
            d.add_paragraph(f"Riscos: {hr.get('risco_processo','')}")
            d.add_paragraph(f"Alcance: {hr.get('alcance','')}")
            d.add_paragraph(f"Cronograma: início {to_iso(hr.get('cronograma_inicio',''))} • fim {to_iso(hr.get('cronograma_final',''))}")

        table = d.add_table(rows=1, cols=7)
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text="Finding ID"; hdr_cells[1].text="Título"; hdr_cells[2].text="Impacto (priorizado)"
        hdr_cells[3].text="Recomendação"; hdr_cells[4].text="Status"; hdr_cells[5].text="Responsável"; hdr_cells[6].text="Prazo"

        for _, r in df_aud.iterrows():
            row = table.add_row().cells
            row[0].text = str(r.get("finding_id",""))
            row[1].text = str(r.get("finding_title",""))
            row[2].text = str(r.get("impact",""))
            row[3].text = str(r.get("recommendation",""))
            row[4].text = str(r.get("status",""))
            row[5].text = str(r.get("owner",""))
            row[6].text = to_iso(r.get("due_date",""))

        for _, r in df_aud.iterrows():
            d.add_paragraph(f"[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}", style="List Bullet")
            ftext = str(r.get("finding_text","") or "")
            if not ftext.strip(): ftext = "(Sem descrição detalhada.)"
            d.add_paragraph(ftext)

    out = io.BytesIO(); d.save(out); return out.getvalue()

# Últimos resultados para export
last_answer, last_results = None, None
for msg in reversed(st.session_state["history"]):
    if msg[0] == "assistant":
        last_answer  = msg[1]
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
