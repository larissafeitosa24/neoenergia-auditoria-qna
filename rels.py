import streamlit as st
import pandas as pd
import requests
import io
import re
from dateutil.parser import parse as date_parse

# ==============================
# Aparência Neoenergia + layout
# ==============================
st.set_page_config(page_title="Neoenergia • Q&A de Relatórios", page_icon="📗", layout="wide")

NEO_GREEN = "#7CC04B"  # cor aproximada
NEO_BLUE = "#0060A9"

CUSTOM_CSS = f"""
<style>
html, body, [class*="css"]  {{
  font-family: Segoe UI, SegoeUI, Helvetica, Arial, sans-serif;
}}
h1, h2, h3 {{
  color: {NEO_BLUE};
}}
.stButton>button {{
  background-color: {NEO_BLUE};
  color: white;
  border: 0px;
  border-radius: 6px;
}}
.stButton>button:hover {{
  background-color: #014e87;
}}
.small-hint {{
  color:#666; font-size: 0.85rem;
}}
.tag {{
  display:inline-block; padding:2px 8px; margin-right:6px;
  background:{NEO_GREEN}; color:#143d00; border-radius:10px; font-size:0.75rem;
}}
.badge {{
  background:#e8f4ff; color:{NEO_BLUE}; padding:2px 8px; border-radius:10px; font-size:0.75rem;
}}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

st.title("📗 Neoenergia — Q&A de Relatórios (UI + Prompt Builder)")
st.caption("Carregue os CSVs (head + findings), filtre os relatórios e gere um prompt executável para o Copilot responder com linguagem executiva.")

# ==============================
# Funções utilitárias
# ==============================
def read_csv_source(label: str):
    """Permite escolher entre upload de arquivo ou URL (GitHub RAW)."""
    m = st.radio(f"Fonte do CSV • {label}", ["Upload", "GitHub RAW URL"], horizontal=True)
    df = None
    if m == "Upload":
        f = st.file_uploader(f"Selecione o CSV • {label}", type=["csv"], key=f"u_{label}")
        if f is not None:
            df = pd.read_csv(f)
    else:
        url = st.text_input(f"URL RAW do CSV • {label}", placeholder="https://github.com/larissafeitosa24/neoenergia-auditoria-qna/constatacoes.csv", key=f"url_{label}")
        if url:
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                df = pd.read_csv(io.StringIO(r.text))
            except Exception as e:
                st.error(f"Falha ao ler URL: {e}")
                df = None
    return df

def norm(s):
    if pd.isna(s): return ""
    return str(s).strip()

def to_iso(d):
    if not d or pd.isna(d): return ""
    s = str(d).strip()
    try:
        return date_parse(s, dayfirst=True).date().isoformat()
    except Exception:
        return s

def detect_aud(text):
    m = re.search(r"(aud)[\s\-]?(\d{4,7})", str(text), re.I)
    return f"AUD-{m.group(2)}" if m else None

def build_prompt_for_copilot(head_rows: pd.DataFrame, find_rows: pd.DataFrame, user_question: str):
    """Gera o Prompt Mestre + contexto dos dados selecionados para colar no Copilot."""
    # Seleciona apenas colunas relevantes, se existirem
    cols_head = [c for c in ["aud_code","title","company","emission_date","objetivo","escopo","risco_processo","alcance","classification","cronograma_inicio","cronograma_final"] if c in head_rows.columns]
    cols_find = [c for c in ["aud_code","finding_id","finding_title","finding_text","recommendation","impact","owner","due_date","status","tema"] if c in find_rows.columns]

    # Normaliza datas para ISO
    for c in ["emission_date","cronograma_inicio","cronograma_final","due_date"]:
        if c in head_rows.columns:
            head_rows[c] = head_rows[c].apply(to_iso)
        if c in find_rows.columns:
            find_rows[c] = find_rows[c].apply(to_iso)

    # Constrói blocos de contexto em texto (sem detalhes técnicos de colunas)
    # HEAD (máx. 5 linhas para não explodir o prompt)
    head_lines = []
    for _, r in head_rows.head(5).iterrows():
        line_parts = []
        aud = norm(r.get("aud_code"))
        title = norm(r.get("title"))
        comp = norm(r.get("company"))
        em = to_iso(r.get("emission_date"))
        clazz = norm(r.get("classification"))

        header = f"[{aud}] {title}" if title else f"[{aud}]"
        meta = " • ".join([x for x in [comp or "", em or "", f"Classificação: {clazz}" if clazz else ""] if x])
        if meta: header += f" — {meta}"
        head_lines.append(header)

        # Campos textuais chave (somente se existirem e não vazios)
        for lbl, col in [("Objetivo", "objetivo"), ("Riscos do processo", "risco_processo"),
                         ("Escopo", "escopo"), ("Alcance", "alcance")]:
            if col in head_rows.columns:
                v = norm(r.get(col))
                if v:
                    head_lines.append(f"- {lbl}: {v}")

        # Cronograma (compacto)
        ci = norm(r.get("cronograma_inicio")) if "cronograma_inicio" in head_rows.columns else ""
        cf = norm(r.get("cronograma_final")) if "cronograma_final" in head_rows.columns else ""
        if ci or cf:
            head_lines.append(f"- Cronograma: início {ci or '—'} • final {cf or '—'}")

        head_lines.append("")  # separador

    head_block = "\n".join([ln for ln in head_lines if ln is not None])

    # FINDINGS (máx. 6 linhas para manter conciso)
    find_lines = []
    for _, r in find_rows.head(6).iterrows():
        aud = norm(r.get("aud_code"))
        fid = norm(r.get("finding_id"))
        ft = norm(r.get("finding_title"))
        rec = norm(r.get("recommendation"))
        imp = norm(r.get("impact"))
        stt = norm(r.get("status"))
        owner = norm(r.get("owner"))
        due = norm(r.get("due_date"))

        tag = f"{aud}" + (f" – {fid}" if fid else "")
        bullet = f"[{tag}] **{ft or 'Constatação'}**"
        if imp:   bullet += f" — Impacto: {imp}"
        if rec:   bullet += f" — Recomendação: {rec}"
        if stt:   bullet += f" — Status: {stt}"
        if owner: bullet += f" — Resp.: {owner}"
        if due:   bullet += f" — Prazo: {due}"
        find_lines.append(bullet)

    findings_block = "\n".join(find_lines) if find_lines else ""

    # Prompt Mestre (curto) + pergunta + contexto
    system_rules = (
        "Você é o Assistente de Auditoria Interna da Neoenergia.\n"
        "Responda exclusivamente com base no CONTEXTO fornecido abaixo (dados de relatórios). "
        "Entregue resposta executiva, clara e direta, sem termos técnicos de base de dados. "
        "Se faltar informação, diga: \"Não encontrei essa informação nos arquivos fornecidos.\" "
        "Evite detalhes técnicos; destaque Objetivo, Escopo, Riscos, Classificação, Cronograma "
        "e 3–5 constatações com recomendações quando fizer sentido. Cite apenas [Fonte: AUD-xxxxx] "
        "e opcionalmente [Fonte: AUD-xxxxx – finding_id]."
    )

    prompt = f"""[INSTRUÇÕES]
{system_rules}

[PERGUNTA]
{user_question.strip()}

[CONTEXTO - HEAD]
{head_block}

[CONTEXTO - FINDINGS]
{findings_block}
"""
    return prompt

# ==============================
# Entrada dos dados
# ==============================
st.header("1) Carregue os dados")

colA, colB = st.columns(2)
with colA:
    st.subheader("CSV • HEAD (metadados por relatório)")
    st.markdown('<span class="small-hint">Colunas sugeridas: aud_code, title, company, emission_date, objetivo, escopo, risco_processo, alcance, classification, cronograma_inicio, cronograma_final.</span>', unsafe_allow_html=True)
    df_head = read_csv_source("HEAD")

with colB:
    st.subheader("CSV • FINDINGS (constatações & recomendações)")
    st.markdown('<span class="small-hint">Colunas sugeridas: aud_code, finding_id, finding_title, finding_text, recommendation, impact, owner, due_date, status, tema.</span>', unsafe_allow_html=True)
    df_find = read_csv_source("FINDINGS")

if df_head is not None and "aud_code" in df_head.columns:
    df_head["aud_code"] = df_head["aud_code"].astype(str).str.upper().str.replace(" ", "-", regex=False)
if df_find is not None and "aud_code" in df_find.columns:
    df_find["aud_code"] = df_find["aud_code"].astype(str).str.upper().str.replace(" ", "-", regex=False)

if df_head is None or df_find is None:
    st.info("Carregue os dois CSVs para habilitar filtros e geração de prompt.")
    st.stop()

# ==============================
# Filtros
# ==============================
st.header("2) Filtre os relatórios")

col1, col2, col3, col4 = st.columns(4)
aud_list = sorted(df_head["aud_code"].dropna().unique().tolist()) if "aud_code" in df_head.columns else []
company_list = sorted(df_head["company"].dropna().unique().tolist()) if "company" in df_head.columns else []
class_list = sorted(df_head["classification"].dropna().unique().tolist()) if "classification" in df_head.columns else []
tema_list = sorted(df_find["tema"].dropna().unique().tolist()) if "tema" in df_find.columns else []

with col1:
    f_aud = st.multiselect("AUD(s)", aud_list)
with col2:
    f_company = st.multiselect("Empresa(s)", company_list)
with col3:
    f_class = st.multiselect("Classificação", class_list)
with col4:
    f_tema = st.multiselect("Tema (findings)", tema_list)

# Ano (derivado de emission_date se existir)
year_vals = []
if "emission_date" in df_head.columns:
    year_vals = sorted({re.match(r"(\d{4})", str(x)).group(1) for x in df_head["emission_date"].dropna().astype(str) if re.match(r"\d{4}", str(x))})
f_year = st.multiselect("Ano (emission_date)", year_vals) if year_vals else []

def apply_filters_head(df):
    x = df.copy()
    if f_aud:     x = x[x["aud_code"].isin(f_aud)]
    if f_company: x = x[x["company"].isin(f_company)]
    if f_class and "classification" in x.columns: x = x[x["classification"].isin(f_class)]
    if f_year and "emission_date" in x.columns:
        x = x[x["emission_date"].astype(str).str[:4].isin(f_year)]
    return x

def apply_filters_find(df):
    x = df.copy()
    if f_aud:     x = x[x["aud_code"].isin(f_aud)]
    if f_tema and "tema" in x.columns: x = x[x["tema"].isin(f_tema)]
    return x

df_head_f = apply_filters_head(df_head)
df_find_f = apply_filters_find(df_find)

st.write("**HEAD filtrado (amostra):**")
cols_h_show = [c for c in ["aud_code","title","company","emission_date","classification","objetivo","escopo","risco_processo","alcance","cronograma_inicio","cronograma_final"] if c in df_head_f.columns]
st.dataframe(df_head_f[cols_h_show].head(20), use_container_width=True)

st.write("**FINDINGS filtrado (amostra):**")
cols_f_show = [c for c in ["aud_code","finding_id","finding_title","recommendation","impact","status","owner","due_date","tema"] if c in df_find_f.columns]
st.dataframe(df_find_f[cols_f_show].head(20), use_container_width=True)

# ==============================
# Geração do Prompt
# ==============================
st.header("3) Gere o Prompt para o Copilot")

question = st.text_input("Pergunta que você fará ao Copilot (ex.: “Resuma objetivo, escopo, riscos e as 3 recomendações mais críticas do AUD-12345.”)")
if st.button("Gerar Prompt"):
    if df_head_f.empty and df_find_f.empty:
        st.warning("Os filtros atuais não retornaram linhas. Ajuste os filtros ou informe ao menos um AUD.")
    else:
        prompt = build_prompt_for_copilot(df_head_f, df_find_f, question or "Faça um resumo executivo para a diretoria com base no contexto.")
        st.subheader("Prompt gerado (copie e cole no Copilot):")
        st.code(prompt, language="markdown")
        st.success("✅ Copie o prompt acima e cole no Copilot. Ele responderá com base neste contexto.")

st.markdown("---")
st.caption("Dica: deixe os CSVs no GitHub (RAW) e cole as URLs aqui para o app sempre ler a versão mais recente sem depender de OneDrive.")