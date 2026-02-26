# ===========================================================
# Chat (OpenAI RAG)
# ===========================================================
import os
from openai import OpenAI

# Você pode trocar o modelo depois (ex: gpt-4.1, gpt-4o-mini etc.)
DEFAULT_MODEL = "gpt-4.1"

def format_context(results_df: pd.DataFrame, max_chars_total: int = 9000) -> str:
    """
    Monta o contexto com trechos do corpus (HEAD + FIND), com tags para rastreabilidade.
    Limita tamanho total para não estourar tokens.
    """
    parts = []
    total = 0
    for _, r in results_df.iterrows():
        tag = f"[{r['source_type']} | aud={r['aud_code']} | finding={r['finding_id']}]"
        text = str(r["text"] or "").strip()
        chunk_txt = f"{tag}\n{text}\n"
        if total + len(chunk_txt) > max_chars_total:
            break
        parts.append(chunk_txt)
        total += len(chunk_txt)
    return "\n---\n".join(parts)

def openai_answer(question: str, results_df: pd.DataFrame) -> str:
    """
    Faz a resposta LLM baseada SOMENTE nos trechos retornados.
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return (
            "⚠️ **OPENAI_API_KEY não encontrado.**\n\n"
            "Defina a variável de ambiente `OPENAI_API_KEY` e reinicie o app.\n"
            "Ex.: `export OPENAI_API_KEY=\"...\"` (mac/linux) ou `setx OPENAI_API_KEY \"...\"` (windows)."
        )

    client = OpenAI(api_key=api_key)

    context = format_context(results_df, max_chars_total=9000)

    system = (
        "Você é um assistente de auditoria interna. Responda em PT-BR.\n"
        "Use APENAS o CONTEXTO fornecido. Se faltar dado, diga explicitamente que não está no arquivo.\n"
        "Quando possível, cite as tags [HEAD|FIND ...] que embasam cada ponto.\n"
        "Se a pergunta pedir números, calcule com base no contexto e explique brevemente o critério."
    )

    user = (
        f"PERGUNTA:\n{question}\n\n"
        f"CONTEXTO (trechos dos CSVs do GitHub, já filtrados):\n{context}"
    )

    # store=False evita persistir o Response no dashboard (quando disponível) :contentReference[oaicite:4]{index=4}
    resp = client.responses.create(
        model=DEFAULT_MODEL,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        store=False,
    )

    # SDK agrega texto em output_text :contentReference[oaicite:5]{index=5}
    return resp.output_text.strip() if getattr(resp, "output_text", None) else str(resp)

st.subheader("💬 Pergunte sobre os relatórios (OpenAI)")
show_sources = st.checkbox("Mostrar fontes (trechos)", value=False)

if "history" not in st.session_state:
    st.session_state["history"] = []

q = st.chat_input("Digite sua pergunta...")

if q:
    # 1) Recupera trechos mais relevantes DO SEU corpus filtrado
    results = search_tf(q, filtered_corpus, top_k=12)

    # 2) LLM responde baseado nesses trechos
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
                    tag = f"[{r['source_type']} | {r['aud_code']} – {r['finding_id']}]"
                    html = f"<div class='source'><b>{tag}</b><br>{str(r['text'])[:500]}...</div>"
                    st.markdown(html, unsafe_allow_html=True)
