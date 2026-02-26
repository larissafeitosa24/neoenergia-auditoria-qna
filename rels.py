# ===========================================================
# Chat (OpenAI RAG)
# ===========================================================
import streamlit as st
import pandas as pd
from openai import OpenAI

DEFAULT_MODEL = "gpt-4.1"

def format_context(results_df: pd.DataFrame, max_chars_total: int = 9000) -> str:
    parts = []
    total = 0
    for _, r in results_df.iterrows():
        tag = f"[{r['source_type']} | aud={r['aud_code']} | finding={r['finding_id']}]"
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
        return "⚠️ **OPENAI_API_KEY não encontrado no Secrets do Streamlit Cloud.**"

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

def render_chat(search_tf_func, filtered_corpus: pd.DataFrame):
    st.subheader("💬 Pergunte sobre os relatórios (OpenAI)")
    show_sources = st.checkbox("Mostrar fontes (trechos)", value=False)

    if "history" not in st.session_state:
        st.session_state["history"] = []

    q = st.chat_input("Digite sua pergunta...")

    if q:
        results = search_tf_func(q, filtered_corpus, top_k=12)
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
                        html = f"<div class='source'><b>{tag}</b><br>{str(r.get('text',''))[:500]}...</div>"

                        
                        st.markdown(html, unsafe_allow_html=True)
