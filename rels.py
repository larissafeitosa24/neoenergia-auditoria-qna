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
from docx.shared import Inches  # <-- NEW (para o DOCX detalhado)
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.colors import HexColor
# NEW: platypus para PDF detalhado
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
    # Garante colunas candidatas existirem (nem que vazias)
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
    """Conta findings únicos por AUD no subconjunto de resultados (apenas source_type=FIND)."""
    finds = contexts_df[contexts_df["source_type"] == "FIND"].copy()
    if finds.empty:
        return pd.DataFrame(columns=["aud_code", "qtd_findings"])
    # Pode haver chunks repetidos do mesmo finding; contar unique finding_id por aud_code
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
        # NEW: contagem por AUD nos resultados
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
# Evita NaN
for c in ["classification"]:
    if c in df_h.columns:
        df_h[c] = df_h[c].fillna("")

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
# Tema (negócio associado ou compromissos)
df_f = ensure_col(df_f, "tema", ["negocio_associado", "negcio_associado", "compromissos_da_auditoria"], default="")
# Impacto enriquecido (priorização)
df_f = ensure_col(df_f, "impact", ["impact"], default="")
df_f = enrich_impact(df_f)

# Evita NaN
for c in ["status", "tema", "impact"]:
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
# Filtros (locais)
# ===========================================================
st.subheader("🔎 Filtros")

cols = st.columns(4)
with cols[0]:
    f_aud = st.multiselect("AUD(s)", sorted(df_h["aud_code"].dropna().astype(str).unique()))
with cols[1]:
    if "classification" in df_h.columns:
        f_class = st.multiselect("Classificação", sorted(pd.Series(df_h["classification"]).dropna().astype(str).unique()))
    else:
        f_class = []
with cols[2]:
    if "status" in df_f.columns:
        f_status = st.multiselect("Status (Findings)", sorted(pd.Series(df_f["status"]).dropna().astype(str).unique()))
    else:
        f_status = []
with cols[3]:
    if "tema" in df_f.columns:
        f_tema = st.multiselect("Tema", sorted(pd.Series(df_f["tema"]).dropna().astype(str).unique()))
    else:
        f_tema = []

filtered_corpus = corpus.copy()
if f_aud:
    filtered_corpus = filtered_corpus[filtered_corpus["aud_code"].isin(f_aud)]
if f_status:
    tmp = df_f[df_f["status"].isin(f_status)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[filtered_corpus["finding_id"].isin(tmp)]
if f_tema:
    tmp = df_f[df_f["tema"].isin(f_tema)]["finding_id"].unique()
    filtered_corpus = filtered_corpus[filtered_corpus["finding_id"].isin(tmp)]

# NEW: Resumo por AUD (com base no filtro atual)
with st.expander("📊 Resumo de constatações por AUD (filtro atual)"):
    filt_finds = df_f.copy()
    if f_aud:
        filt_finds = filt_finds[filt_finds["aud_code"].isin(f_aud)]
    if f_status:
        filt_finds = filt_finds[filt_finds["status"].isin(f_status)]
    if f_tema:
        filt_finds = filt_finds[filt_finds["tema"].isin(f_tema)]
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
    # Coleta findings únicos por (aud_code, finding_id) presentes nos resultados
    res_finds = results_df[results_df["source_type"]=="FIND"][["aud_code","finding_id"]].drop_duplicates()
    # Se vazio, cria um PDF básico
    if res_finds.empty:
        return export_pdf("Nenhuma constatação no resultado atual para exportar.")

    # Mapeia para registros completos em df_f
    key = pd.MultiIndex.from_frame(res_finds)
    aux = df_f.set_index(["aud_code","finding_id"]).loc[key].reset_index()

    # Ordena por aud_code
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

    # Capa simples
    if logo_bytes:
        story.append(RLImage(io.BytesIO(logo_bytes), width=180, height=48))
        story.append(Spacer(1, 12))
    story.append(Paragraph("Neoenergia — Q&A de Relatórios (Export Detalhado)", title_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(datetime.datetime.now().strftime("%d/%m/%Y %H:%M"), normal))
    story.append(PageBreak())

    # Por AUD
    for aud, df_aud in aux.groupby("aud_code", sort=False):
        story.append(Paragraph(f"Relatório: {aud}", h2_style))
        # Puxa resumo do head se existir
        head_row = df_h[df_h["aud_code"]==aud].head(1)
        if len(head_row):
            hr = head_row.iloc[0]
            resumo = [
                f"<b>Título:</b> {hr.get('title','')}",
                f"<b>Objetivo:</b> {hr.get('objetivo','')}",
                f"<b>Escopo:</b> {hr.get('escopo','')}",
                f"<b>Riscos:</b> {hr.get('risco_processo','')}",
                f"<b>Alcance:</b> {hr.get('alcance','')}",
                f"<b>Cronograma:</b> início {to_iso(hr.get('cronograma_inicio',''))} • fim {to_iso(hr.get('cronograma_final',''))}",
            ]
            for line in resumo:
                story.append(Paragraph(line, normal))
            story.append(Spacer(1, 6))

        # Tabela de findings
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

        # Texto detalhado de cada finding (quebra leve entre eles)
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
    # Logo (opcional)
    if logo_bytes:
        # python-docx aceita stream em BytesIO
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
        # fallback: export simples informando ausência
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
            d.add_paragraph(f"Objetivo: {hr.get('objetivo','')}")
            d.add_paragraph(f"Escopo: {hr.get('escopo','')}")
            d.add_paragraph(f"Riscos: {hr.get('risco_processo','')}")
            d.add_paragraph(f"Alcance: {hr.get('alcance','')}")
            d.add_paragraph(f"Cronograma: início {to_iso(hr.get('cronograma_inicio',''))} • fim {to_iso(hr.get('cronograma_final',''))}")

        # Tabela
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

        # Texto detalhado por finding
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
