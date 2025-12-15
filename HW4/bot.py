import os
import json
import logging
import time
import random
import asyncio
from pathlib import Path

import numpy as np
import torch
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from sentence_transformers import SentenceTransformer
from llama_cpp import Llama

# ======================================================
# LOAD CONFIG
# ======================================================
BASE_DIR = Path(__file__).parent

with open(BASE_DIR / "config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# ======================================================
# CONFIG VALUES
# ======================================================

# PATHS
DOCUMENTS_DIR = Path(CONFIG["paths"]["documents_dir"])
MODEL_PATH = CONFIG["paths"]["model_path"]
KNOWLEDGE_FILE = Path(CONFIG["paths"]["knowledge_file"])
LOG_FILE = CONFIG["paths"]["log_file"]

# RAG
CHUNK_SIZE = CONFIG["rag"]["chunk_size"]
CHUNK_OVERLAP = CONFIG["rag"]["chunk_overlap"]
TOP_K = CONFIG["rag"]["top_k"]

# LLM
N_CTX = CONFIG["llm"]["n_ctx"]
N_THREADS = CONFIG["llm"]["n_threads"]
N_BATCH = CONFIG["llm"]["n_batch"]

# TELEGRAM
TELEGRAM_TOKEN = CONFIG["telegram"]["token"]
ADMIN_TELEGRAM_USERNAME = CONFIG["telegram"]["admin_username"]
ADMIN_TELEGRAM_CHAT_ID = CONFIG["telegram"]["admin_chat_id"]
ORGANIZER_CONTACT = CONFIG["telegram"]["organizer_contact"]

# MESSAGES
MSG = CONFIG["messages"]

RANDOM_FALLBACKS = [
    MSG["fallback_1"],
    MSG["fallback_2"],
    MSG["fallback_3"]
]

# ======================================================
# GLOBAL STATE
# ======================================================
PENDING_QUESTIONS = {}

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================================================
# DOCUMENTS & EMBEDDINGS
# ======================================================
def load_documents():
    if not DOCUMENTS_DIR.exists():
        raise RuntimeError("Папка documents не найдена")

    documents = []
    for path in DOCUMENTS_DIR.glob("*.txt"):
        text = path.read_text(encoding="utf-8")
        documents.append({"text": text, "source": path.name})
        logger.info(f"Загружен {path.name} ({len(text)} символов)")

    if not documents:
        raise RuntimeError("Нет .txt файлов в documents")

    return documents


def split_text(text, chunk_size, overlap):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - overlap
    return chunks


def create_chunks(documents):
    chunks = []
    for doc in documents:
        parts = split_text(doc["text"], CHUNK_SIZE, CHUNK_OVERLAP)
        for i, part in enumerate(parts):
            chunks.append({
                "content": part,
                "source": doc["source"],
                "chunk_id": i
            })
    logger.info(f"Создано чанков: {len(chunks)}")
    return chunks


def create_embeddings(chunks):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Загрузка embedding-модели ({device})")

    embedder = SentenceTransformer(
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        device=device
    )

    texts = [c["content"] for c in chunks]
    embeddings = embedder.encode(
        texts,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=True
    )

    return embedder, embeddings


def search_chunks(query, embedder, embeddings, chunks, k=TOP_K):
    q_emb = embedder.encode(query, normalize_embeddings=True, convert_to_numpy=True)
    scores = np.dot(embeddings, q_emb)
    top_idx = np.argsort(scores)[-k:][::-1]
    return [chunks[i] for i in top_idx]


def build_context(found_chunks):
    parts = []
    for i, ch in enumerate(found_chunks, 1):
        parts.append(f"[Источник {i}: {ch['source']}]\n{ch['content']}")
    return "\n\n".join(parts)

# ======================================================
# LLM
# ======================================================
def load_llm():
    logger.info("Загрузка LLM (Qwen / llama.cpp)")
    return Llama(
        model_path=MODEL_PATH,
        n_ctx=N_CTX,
        n_threads=N_THREADS,
        n_batch=N_BATCH,
        use_mmap=True,
        verbose=False
    )


def build_prompt(query, context):
    return f"""
Ты — система ответов.

ПРАВИЛА (ОБЯЗАТЕЛЬНО):
- Отвечай ТОЛЬКО на основе информации ниже
- Отвечай конкретно
- Если информации нет — напиши "В тексте нет информации"

    ИНФОРМАЦИЯ:
    {context}
    {context}

    ВОПРОС:
    {query}

    ОТВЕТ:
    """

# ======================================================
# ADMIN & SELF-LEARNING
# ======================================================
async def notify_admin(bot, query: str, username: str, user_id: int):
    text = MSG["admin_instruction"].format(
        user_id=user_id,
        username=username,
        query=query
    )

    msg = await bot.send_message(
        chat_id=ADMIN_TELEGRAM_CHAT_ID,
        text=text
    )

    PENDING_QUESTIONS[msg.message_id] = {
        "user_id": user_id,
        "username": username,
        "query": query,
        "timestamp": time.time()
    }


def save_admin_answer(query, answer):
    KNOWLEDGE_FILE.parent.mkdir(exist_ok=True)
    with open(KNOWLEDGE_FILE, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {"question": query, "answer": answer},
                ensure_ascii=False
            ) + "\n"
        )


def add_to_embeddings(query, answer, bot_data):
    text = f"Вопрос: {query}\nОтвет: {answer}"

    emb = bot_data["embedder"].encode(
        text,
        normalize_embeddings=True,
        convert_to_numpy=True
    )

    bot_data["embeddings"] = np.vstack([bot_data["embeddings"], emb])
    bot_data["chunks"].append({
        "content": text,
        "source": "admin",
        "chunk_id": len(bot_data["chunks"])
    })

# ======================================================
# ANSWER GENERATION
# ======================================================
def postprocess_answer(*, answer, query, bot, username, user_id):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Пользователь: {username}, id: {user_id}\n Вопрос: {query}\nОтвет: {answer}\n")
        f.write(
            f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Пользователь: {username}, id: {user_id}\n"
            f"Вопрос: {query}\nОтвет: {answer}\n"
        )

    if not answer or answer.isspace():
        return random.choice(RANDOM_FALLBACKS)

    if "В тексте нет информации" in answer:
        asyncio.create_task(
            notify_admin(bot, query, username, user_id)
        )
        return MSG["no_info_user"]

    return answer


def generate_answer(llm, prompt):
    result = llm(
        prompt,
        max_tokens=80,
        temperature=0.0,
        top_p=0.9,
        stop=["\n", "</s>"]
    )
    return result["choices"][0]["text"].strip()

# ======================================================
# TELEGRAM HANDLERS
# ======================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(MSG["start"])


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "unknown"

    await update.message.reply_text(MSG["processing"])

    query = update.message.text.strip()
    found_chunks = search_chunks(
        query,
        context.bot_data["embedder"],
        context.bot_data["embeddings"],
        context.bot_data["chunks"]
    )

    context_text = build_context(found_chunks)
    prompt = build_prompt(query, context_text)

    answer = generate_answer(context.bot_data["llm"], prompt)
    answer = postprocess_answer(
        answer=answer,
        query=query,
        bot=context.bot,
        username=username,
        user_id=user_id
    )

    await update.message.reply_text(answer)


async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message

    if not msg.reply_to_message:
        return

    replied_id = msg.reply_to_message.message_id
    if replied_id not in PENDING_QUESTIONS:
        return

    data = PENDING_QUESTIONS.pop(replied_id)

    raw_text = msg.text.strip()
    admin_answer = (
        raw_text.split("\n", 1)[1].strip()
        if "\n" in raw_text
        else raw_text
    )

    await context.bot.send_message(
        chat_id=data["user_id"],
        text=admin_answer
    )

    save_admin_answer(data["query"], admin_answer)
    add_to_embeddings(data["query"], admin_answer, context.bot_data)

    await msg.reply_text(MSG["admin_done"])

# ======================================================
# MAIN
# ======================================================
def main():
    documents = load_documents()
    chunks = create_chunks(documents)
    embedder, embeddings = create_embeddings(chunks)
    llm = load_llm()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.bot_data.update({
        "chunks": chunks,
        "embedder": embedder,
        "embeddings": embeddings,
        "llm": llm
    })

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.REPLY, handle_admin_reply))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()
