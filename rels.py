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
from docx.shared import Inches
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, PageBreak, Image as RLImage, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
import base64
import unicodedata
import re as _re

# ===========================================================
# 🔧 CONFIGURAÇÃO FIXA — ALTERE SÓ AQUI
# ===========================================================
HEAD_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/relatorios.csv"
FIND_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/constatacoes.csv"
LOGO_URL = "https://raw.githubusercontent.com/larissafeitosa24/neoenergia-auditoria-qna/main/neo_logo.png"
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
    buf, out = "", []
    for p in parts:
        if len(buf) + len(p) < max_chars:
            buf += p + " "
        else:
            out.append(buf.strip())
            buf = p
    out.append(buf.strip())
    return out

# ----------------------- Normalização e mapeamento -----------------------
def _normalize_col(s: str) -> str:
    s = str(s).strip()
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    s = _re.sub(r'\s+', '_', s.replace('-', '_'))
    return s.lower()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_col(c) for c in df.columns]
    return df

def ensure_col(df: pd.DataFrame, new_name: str, candidates: list, default="") -> pd.DataFrame:
    """Cria df[new_name] a partir do primeiro candidato existente; senão, default."""
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
      1) tipo_de_constatacao (ou tipo_de_constatao)
      2) associated_main_risk_category
      3) impact (se já existir)
    Resultado final é escrito em df['impact'].
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
    corpus = corpus.copy()
    corpus["score"] = sim
    return corpus.sort_values("score", ascending=False).head(top_k)

def _counts_by_aud_in_results(contexts_df: pd.DataFrame) -> pd.DataFrame:
    finds = contexts_df[contexts_df["source_type"] == "FIND"].copy()
    if finds.empty:
        return pd.DataFrame(columns=["aud_code", "qtd_findings"])
    g = finds.groupby("aud_code")["finding_id"].nunique().reset_index(name="qtd_findings")
    return g.sort_values(["qtd_findings", "aud_code"], ascending=[False, True])

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
        out.append("")
        out.append("**Contagem de constatações por relatório (no resultado retornado):**")
        cnt_df = _counts_by_aud_in_results(contexts)
        for _, rr in cnt_df.iterrows():
            out.append(f"- **{rr['aud_code']}**: {int(rr['qtd_findings'])} constatação(ões)")

    return "\n".join(out)

# ===========================================================
# LOAD CSV AUTOMÁTICO
# ===========================================================
with st.spinner("Carregando dados do GitHub..."):
    df_h = load_csv(HEAD_URL)
    df_f = load_csv(FIND_URL)

# ----------------------- Sanitize/Mapear colunas -----------------------
df_h = normalize_columns(df_h)
df_f = normalize_columns(df_f)

# RELATÓRIO
if "aud_code" not in df_h.columns:
    st.error("O CSV de relatórios não contém a coluna 'aud_code'. Verifique HEAD_URL.")
else:
    df_h["aud_code"] = df_h["aud_code"].astype(str).str.strip().str.upper()
for c in ["classification", "title", "company"]:
    if c in df_h.columns:
        df_h[c] = df_h[c].fillna("")

# Deriva 'ano_filter' (de 'ano' se existir, senão de emission_date)
if "ano" in df_h.columns:
    df_h["ano_filter"] = df_h["ano"].astype(str)
else:
    if "emission_date" in df_h.columns:
        df_h["ano_filter"] = pd.to_datetime(df_h["emission_date"], errors="coerce").dt.year.astype("Int64").astype(str)
    else:
        df_h["ano_filter"] = ""

# CONSTATAÇÕES: criar/garantir campos esperados
if "aud_code" not in df_f.columns:
    if "id_do_trabalho" in df_f.columns:
        df_f["aud_code"] = df_f["id_do_trabalho"].astype(str)
    else:
        st.error("O CSV de constatações não contém 'id_do_trabalho' nem 'aud_code'.")
        df_f["aud_code"] = ""
df_f["aud_code"] = df_f["aud_code"].astype(str).str.strip().str.upper()

df_f = ensure_col(df_f, "finding_id", ["finding_id"], default="")
df_f = ensure_col(df_f, "finding_title", ["nome_da_constatacao", "nome_da_constatao"], default="")
df_f = ensure_col(df_f, "recommendation", ["descricao_do_plano_de_recomendacao", "descrio_do_plano_de_recomendao"], default="")
df_f = ensure_col(df_f, "status", ["status_da_constatacao", "estado_del_trabajo"], default="")
df_f = ensure_col(df_f, "owner", [
    "proprietario_da_constatacao",
    "organization_of_finding_response_owner",
    "proprietario_da_resposta_descoberta",
    "proprietrio_da_constatao",
    "proprietrio_da_resposta__descoberta"
], default="")
df_f = ensure_col(df_f, "due_date", [
    "data_acordada_vencimento",
    "data_acordada__vencimento",
    "data_acordada_aprovada_atualmente",
    "end_date"
], default="")
df_f = ensure_col(df_f, "finding_text", ["constatacao", "constatao", "resposta"], default="")
df_f = ensure_col(df_f, "tema", ["negocio_associado", "negcio_associado", "compromissos_da_auditoria"], default="")

# Impacto enriquecido (priorização)
df_f = ensure_col(df_f, "impact", ["impact"], default="")
df_f = enrich_impact(df_f)

# Coluna para filtro de risco (usa impact enriquecido)
df_f["risk_filter"] = df_f.get("impact", "").fillna("").astype(str).str.strip()

# Evita NaN
for c in ["status", "tema", "impact", "risk_filter"]:
    if c in df_f.columns:
        df_f[c] = df_f[c].fillna("")

# ---------------------------------------------------------------------------
corpus = build_corpus(df_h, df_f)

# ===========================================================
# Logo Neoenergia
# ===========================================================
logo_bytes = load_logo(LOGO_URL)
if logo_bytes:
    st.image(logo_bytes, width=180)

st.title("📗 Q&A Enterprise Neoenergia (100% Automático)")

# ===========================================================
# Filtros (substituídos)
# ===========================================================
st.subheader("🔎 Filtros")

cols = st.columns(4)
with cols[0]:
    f_title = st.multiselect("Título do trabalho", sorted(pd.Series(df_h["title"]).dropna().astype(str).unique()))
with cols[1]:
    f_risk = st.multiselect("Risco da recomendação", sorted(pd.Series(df_f["risk_filter"]).replace("", pd.NA).dropna().unique()))
with cols[2]:
    f_company = st.multiselect("Empresa", sorted(pd.Series(df_h["company"]).dropna().astype(str).unique())) if "company" in df_h.columns else []
with cols[3]:
    f_year = st.multiselect("Ano", sorted(pd.Series(df_h["ano_filter"]).replace("","Sem ano").unique()))

# Aplica filtros sobre HEAD e encontra aud_codes
heads_filt = df_h.copy()
if f_title:
    heads_filt = heads_filt[heads_filt["title"].isin(f_title)]
if f_company:
    heads_filt = heads_filt[heads_filt["company"].isin(f_company)]
if f_year:
    # Tratar "Sem ano"
    years_norm = [("" if y == "Sem ano" else y) for y in f_year]
    heads_filt = heads_filt[heads_filt["ano_filter"].astype(str).isin(years_norm)]

aud_subset = set(heads_filt["aud_code"]) if not heads_filt.empty else set()

filtered_corpus = corpus.copy()
if aud_subset:
    filtered_corpus = filtered_corpus[filtered_corpus["aud_code"].isin(aud_subset)]

# Filtro de risco atua apenas sobre linhas FIND; mantém HEADs dos auds selecionados
if f_risk:
    valid_finds = df_f[df_f["risk_filter"].isin(f_risk)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[
        ((filtered_corpus["source_type"] == "FIND") & (filtered_corpus["finding_id"].isin(valid_finds))) |
        ((filtered_corpus["source_type"] == "HEAD"))
    ]

# ===========================================================
# Resumo por AUD (com base no filtro atual)
# ===========================================================
with st.expander("📊 Resumo de constatações por AUD (filtro atual)"):
    filt_finds = df_f.copy()
    if aud_subset:
        filt_finds = filt_finds[filt_finds["aud_code"].isin(aud_subset)]
    if f_risk:
        filt_finds = filt_finds[filt_finds["risk_filter"].isin(f_risk)]
    cnt = (filt_finds.assign(finding_id=filt_finds["finding_id"].fillna("").astype(str))
           .query("finding_id != ''")
           .groupby("aud_code")["finding_id"].nunique()
           .reset_index(name="qtd_findings")
           .sort_values(["qtd_findings","aud_code"], ascending=[False, True]))
    if cnt.empty:
        st.write("Nenhuma constatação no filtro atual.")
    else:
        st.dataframe(cnt, use_container_width=True)

# ===========================================================
# NLQ (inteligência mínima) — Intents e helpers
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

def _extract_years(q: str):
    return sorted(set(re.findall(r"\b(20[0-3]\d)\b", q)))

def _extract_months(q: str):
    months = []
    for name, num in _PT_MONTHS.items():
        if re.search(rf"\b{name}\b", q):
            months.append(num)
    return sorted(set(months))

def _extract_relative_time(q: str):
    this_month = bool(re.search(r"\b(este|neste|nesse)\s+mes\b", q))
    this_year = bool(re.search(r"\b(este|neste|nesse)\s+ano\b", q))
    return this_month, this_year

def _normalize_status_value(s: str):
    s = _norm_text(s)
    if not s: return ""
    if any(k in s for k in ["encerrad","fechad","closed","concluid","implementad","done","aprovad"]): return "encerrado"
    if "atras" in s or "vencid" in s: return "atrasado"
    if "andament" in s or "in progress" in s: return "em_andamento"
    if "abert" in s or "open" in s or "pendente" in s: return "aberto"
    return s

def _extract_companies(q: str, df_h: pd.DataFrame):
    qn = _norm_text(q)
    if "company" not in df_h.columns: return []
    detected = []
    for comp in sorted(set(df_h["company"].dropna().astype(str))):
        cn = _norm_text(comp)
        if cn and cn in qn:
            detected.append(comp)
    return detected

def _extract_risks(q: str, df_f: pd.DataFrame):
    qn = _norm_text(q)
    detected = []
    uniq = sorted(set(df_f["risk_filter"].dropna().astype(str)))
    for r in uniq:
        rn = _norm_text(r)
        if rn and rn in qn:
            detected.append(r)
    return detected

def _find_related_auds(question: str, df_h: pd.DataFrame, df_f: pd.DataFrame, corpus: pd.DataFrame, top_k=10, companies=None):
    # 1) HEADs
    head_corpus = corpus[corpus["source_type"]=="HEAD"]
    if not head_corpus.empty:
        res = search_tf(question, head_corpus, top_k=top_k)
        auds = res["aud_code"].tolist()
        seen, ordered = set(), []
        for a in auds:
            if a not in seen:
                ordered.append(a); seen.add(a)
        if ordered:
            # Restringe por empresa, se informada
            if companies and "company" in df_h.columns:
                valids = set(df_h[df_h["company"].isin(companies)]["aud_code"])
                ordered = [a for a in ordered if a in valids]
            return ordered

    # 2) Fallback: constatações
    tmp = df_f.copy()
    tmp["__text__"] = (
        tmp.get("finding_title","").astype(str) + " " +
        tmp.get("finding_text","").astype(str) + " " +
        tmp.get("tema","").astype(str) + " " +
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
        if companies and "company" in df_h.columns:
            valids = set(df_h[df_h["company"].isin(companies)]["aud_code"])
            ordered2 = [a for a in ordered2 if a in valids]
        return ordered2
    return []

def _apply_year_month_filters(df_head: pd.DataFrame, years: list, months: list, this_month=False, this_year=False):
    out = df_head.copy()
    # relativos
    today = datetime.date.today()
    if this_year and "ano_filter" in out.columns:
        out = out[out["ano_filter"].astype(str) == str(today.year)]
    elif this_year and "emission_date" in out.columns:
        ed = pd.to_datetime(out["emission_date"], errors="coerce")
        out = out[ed.dt.year == today.year]

    if this_month and "emission_date" in out.columns:
        ed = pd.to_datetime(out["emission_date"], errors="coerce")
        out = out[(ed.dt.year == today.year) & (ed.dt.month == today.month)]

    # absolutos
    if years:
        if "ano_filter" in out.columns:
            out = out[out["ano_filter"].astype(str).isin(years)]
        elif "emission_date" in out.columns:
            ed = pd.to_datetime(out["emission_date"], errors="coerce")
            out = out[ed.dt.year.astype("Int64").astype(str).isin(years)]
    if months:
        if "emission_date" in out.columns:
            ed = pd.to_datetime(out["emission_date"], errors="coerce")
            out = out[ed.dt.month.isin(months)]
    return out

# -------- Intents (respostas curtas e naturais) --------
def _answer_count_recommendations(q, df_h, df_f, corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_h)
    risks = _extract_risks(q, df_f)

    auds = _find_related_auds(q, df_h, df_f, corpus, top_k=12, companies=companies)
    if not auds:
        return "Não encontrei trabalhos relacionados ao tema da sua pergunta.", None
    head = _apply_year_month_filters(df_h[df_h["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    # Reco = constatações com recommendation não vazia
    df_find = df_f[df_f["aud_code"].isin(auds_final)].copy()
    if risks:
        df_find = df_find[df_find["risk_filter"].isin(risks)]
    df_find["recommendation"] = df_find["recommendation"].fillna("").astype(str).str.strip()
    total = int((df_find["recommendation"]!="").sum())

    t_trab = "trabalho" if len(auds_final)==1 else "trabalhos"
    t_rec = "recomendação" if total==1 else "recomendações"
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    ans = f"Foram identificados {len(auds_final)} {t_trab}{anos_txt}, com {total} {t_rec} no total."
    res = search_tf(q, corpus, top_k=12)
    return ans, res

def _answer_overdue(q, df_h, df_f, corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_h)
    risks = _extract_risks(q, df_f)

    auds = _find_related_auds(q, df_h, df_f, corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para verificar atrasos.", None
    head = _apply_year_month_filters(df_h[df_h["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()

    today = datetime.date.today()
    dd = pd.to_datetime(df_f["due_date"], errors="coerce").dt.date if "due_date" in df_f.columns else None
    status_norm = df_f.get("status","").astype(str).apply(_normalize_status_value)
    mask_base = df_f["aud_code"].isin(auds_final)
    mask_due = dd.notna() & (dd < today) if dd is not None else False
    mask_open = ~status_norm.isin(["encerrado"])
    mask_risk = df_f["risk_filter"].isin(risks) if risks else True
    overdue = df_f[mask_base & mask_due & mask_open & mask_risk]
    n = len(overdue)
    t = "recomendação" if n==1 else "recomendações"
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    ans = f"Há {n} {t} em atraso para os trabalhos identificados{anos_txt}."
    res = search_tf(q, corpus, top_k=12)
    return ans, res

def _answer_status_breakdown(q, df_h, df_f, corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_h)
    risks = _extract_risks(q, df_f)

    auds = _find_related_auds(q, df_h, df_f, corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para consolidar status.", None
    head = _apply_year_month_filters(df_h[df_h["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    df_find = df_f[df_f["aud_code"].isin(auds_final)].copy()
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
    res = search_tf(q, corpus, top_k=12)
    return ans, res

def _answer_responsibles(q, df_h, df_f, corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_h)
    risks = _extract_risks(q, df_f)

    auds = _find_related_auds(q, df_h, df_f, corpus, top_k=12, companies=companies)
    if not auds:
        return "Não identifiquei trabalhos relacionados ao tema para listar responsáveis.", None
    head = _apply_year_month_filters(df_h[df_h["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    if head.empty:
        return "Não encontrei trabalhos para o período solicitado.", None

    auds_final = head["aud_code"].unique().tolist()
    df_find = df_f[df_f["aud_code"].isin(auds_final)].copy()
    if risks:
        df_find = df_find[df_find["risk_filter"].isin(risks)]

    df_find["owner"] = df_find.get("owner","").fillna("").astype(str).str.strip()
    top = (df_find.query("owner != ''")
                    .groupby("owner")["finding_id"].nunique()
                    .reset_index(name="qtd")
                    .sort_values(["qtd","owner"], ascending=[False, True])
                    .head(5))
    if top.empty:
        return "Não há responsáveis mapeados para esses trabalhos.", None
    itens = "; ".join([f"{r.owner}: {int(r.qtd)}" for r in top.itertuples(index=False)])
    ans = f"Principais responsáveis (por nº de recomendações): {itens}."
    res = search_tf(q, corpus, top_k=12)
    return ans, res

def _answer_count_trabalhos(q, df_h, df_f, corpus):
    qn = _norm_text(q)
    yrs = _extract_years(qn); months = _extract_months(qn)
    this_m, this_y = _extract_relative_time(qn)
    companies = _extract_companies(q, df_h)

    auds = _find_related_auds(q, df_h, df_f, corpus, top_k=12, companies=companies)
    if not auds:
        return "Não encontrei trabalhos relacionados ao tema da sua pergunta.", None
    head = _apply_year_month_filters(df_h[df_h["aud_code"].isin(auds)], yrs, months, this_month=this_m, this_year=this_y)
    if companies and "company" in head.columns:
        head = head[head["company"].isin(companies)]
    n = head["aud_code"].nunique()
    if n == 0: return "Não encontrei trabalhos para o período solicitado.", None
    anos = sorted(set(head["ano_filter"].dropna().astype(str).tolist()))
    anos_txt = f" (anos: {', '.join(anos)})" if anos else ""
    t = "trabalho" if n==1 else "trabalhos"
    ans = f"Foram identificados {n} {t}{anos_txt}."
    res = search_tf(q, corpus, top_k=12)
    return ans, res

def try_natural_answer(question: str, df_h: pd.DataFrame, df_f: pd.DataFrame, corpus: pd.DataFrame):
    """
    Router de intents com interpretação mínima + filtros implícitos:
    - empresa mencionada
    - risco/categoria mencionado
    - 'este mês' / 'este ano'
    """
    qn = _norm_text(question)

    # Intent: recomendações (quantas)
    if re.search(r"\bquant(as|idade)\b.*recomendac", qn) or "numero de recomendac" in qn:
        return _answer_count_recommendations(question, df_h, df_f, corpus)

    # Intent: vencidas/atrasadas
    if re.search(r"\batrasad|vencid", qn):
        return _answer_overdue(question, df_h, df_f, corpus)

    # Intent: distribuição por status
    if re.search(r"\bstatus\b.*\b(distribu|por)\b|\bdistribuicao de status\b", qn):
        return _answer_status_breakdown(question, df_h, df_f, corpus)

    # Intent: responsáveis
    if re.search(r"respons[aá]vel", qn) or "owner" in qn or "proprietario" in qn:
        return _answer_responsibles(question, df_h, df_f, corpus)

    # Intent: quantos trabalhos
    if re.search(r"\bquant(os|idade)\b.*(trabalh|relat[oó]ri|aud)\b", qn):
        return _answer_count_trabalhos(question, df_h, df_f, corpus)

    # Fallback: resumo curto do relatório mais relevante
    head_corpus = corpus[corpus["source_type"]=="HEAD"]
    if not head_corpus.empty:
        res = search_tf(question, head_corpus, top_k=1)
        if len(res):
            aud = res.iloc[0]["aud_code"]
            hr = df_h[df_h["aud_code"]==aud].head(1)
            if len(hr):
                r0 = hr.iloc[0]
                cronoi = to_iso(r0.get("cronograma_inicio",""))
                cronof = to_iso(r0.get("cronograma_final",""))
                ans = f"O trabalho {aud} trata de {r0.get('title','(sem título)')}. Objetivo: {r0.get('objetivo','')}. Escopo: {r0.get('escopo','')}. Cronograma: início {cronoi} • fim {cronof}."
                results = search_tf(question, corpus, top_k=12)
                return ans, results

    return None, None

# ===========================================================
# Chat
# ===========================================================
st.subheader("💬 Pergunte sobre os relatórios")

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Digite sua pergunta...")

if q:
    # Resposta natural (intents) + fallback
    nat_ans, nat_results = try_natural_answer(q, df_h, df_f, filtered_corpus)
    if nat_ans is not None:
        answer = nat_ans
        results = nat_results if nat_results is not None else search_tf(q, filtered_corpus, top_k=8)
    else:
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
            st.write(msg[1])  # resposta curta e natural sempre que possível
            st.markdown("**Trechos utilizados:**")
            for _, r in msg[2].iterrows():
                tag = f"[{r['aud_code']} – {r['finding_id']}]"
                st.markdown(f"<div class='source'><b>{tag}</b><br>{r['text'][:500]}...</div>", unsafe_allow_html=True)

# ===========================================================
# Exportação (Simples e Detalhada)
# ===========================================================
st.subheader("📤 Exportar")

def export_pdf(text):
    """Export simples (texto corrido)"""
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

def export_pdf_detailed(df_h, df_f, results_df, logo_bytes=None):
    """Export detalhado com tabelas e quebras por AUD (baseado nos resultados mais recentes)."""
    res_finds = results_df[results_df["source_type"]=="FIND"][["aud_code","finding_id"]].drop_duplicates()
    if res_finds.empty:
        return export_pdf("Nenhuma constatação no resultado atual para exportar.")

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_f.set_index(["aud_code","finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code","status","finding_id"], na_position="last")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=36, rightMargin=36, topMargin=48, bottomMargin=36
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('TitleNeo', parent=styles['Heading1'], textColor=colors.HexColor(NEO_BLUE))
    h2_style = ParagraphStyle('H2Neo', parent=styles['Heading2'], textColor=colors.HexColor(NEO_BLUE))
    normal = styles['BodyText']

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
        head_row = df_h[df_h["aud_code"]==aud].head(1)
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

        tbl_data = [
            ["Finding ID", "Título", "Impacto (priorizado)", "Recomendação", "Status", "Responsável", "Prazo"]
        ]
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

        table = Table(tbl_data, repeatRows=1, colWidths=[60, 120, 110, 110, 60, 80, 60])
        table_style = TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor(NEO_BLUE)),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('ALIGN', (0,0), (-1,0), 'CENTER'),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ])
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 8))

        for _, r in df_aud.iterrows():
            story.append(Paragraph(f"<b>[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}</b>", normal))
            ftext = str(r.get("finding_text","") or "")
            if not ftext.strip():
                ftext = "(Sem descrição detalhada.)"
            story.append(Paragraph(ftext.replace("\n","<br/>"), normal))
            story.append(Spacer(1, 6))

        story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()

def export_docx(text):
    """Export simples (texto corrido)"""
    doc = Document()
    if logo_bytes:
        img_stream = io.BytesIO(logo_bytes)
        try:
            doc.add_picture(img_stream, width=Inches(2.2))
        except Exception:
            pass
    doc.add_heading("Neoenergia — Q&A de Relatórios", level=1)
    for line in text.split("\n"):
        doc.add_paragraph(re.sub(r"\*\*|\_", "", line))
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

def export_docx_detailed(df_h, df_f, results_df, logo_bytes=None):
    """Export detalhado com tabelas e quebras por AUD (baseado nos resultados mais recentes)."""
    res_finds = results_df[results_df["source_type"]=="FIND"][["aud_code","finding_id"]].drop_duplicates()
    if res_finds.empty:
        d = Document()
        d.add_heading("Neoenergia — Q&A (Export Detalhado)", level=1)
        d.add_paragraph("Nenhuma constatação no resultado atual para exportar.")
        out = io.BytesIO()
        d.save(out)
        return out.getvalue()

    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_f.set_index(["aud_code","finding_id"]).loc[key].reset_index()
    aux = aux.sort_values(["aud_code","status","finding_id"], na_position="last")

    d = Document()
    if logo_bytes:
        try:
            img_stream = io.BytesIO(logo_bytes)
            d.add_picture(img_stream, width=Inches(2.2))
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

        head_row = df_h[df_h["aud_code"]==aud].head(1)
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
        hdr_cells[0].text = "Finding ID"
        hdr_cells[1].text = "Título"
        hdr_cells[2].text = "Impacto (priorizado)"
        hdr_cells[3].text = "Recomendação"
        hdr_cells[4].text = "Status"
        hdr_cells[5].text = "Responsável"
        hdr_cells[6].text = "Prazo"

        for _, r in df_aud.iterrows():
            row_cells = table.add_row().cells
            row_cells[0].text = str(r.get("finding_id",""))
            row_cells[1].text = str(r.get("finding_title",""))
            row_cells[2].text = str(r.get("impact",""))
            row_cells[3].text = str(r.get("recommendation",""))
            row_cells[4].text = str(r.get("status",""))
            row_cells[5].text = str(r.get("owner",""))
            row_cells[6].text = to_iso(r.get("due_date",""))

        for _, r in df_aud.iterrows():
            d.add_paragraph(f"[{aud} – {r.get('finding_id','')}] {r.get('finding_title','')}", style="List Bullet")
            ftext = str(r.get("finding_text","") or "")
            if not ftext.strip():
                ftext = "(Sem descrição detalhada.)"
            d.add_paragraph(ftext)

    out = io.BytesIO()
    d.save(out)
    return out.getvalue()

# Identifica última resposta/resultado
last_answer, last_results = None, None
for msg in reversed(st.session_state["history"]):
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
