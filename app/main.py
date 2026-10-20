import asyncio
import json
import logging
import queue
import shutil
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from langgraph.types import Command
from pydantic import BaseModel

from . import events, llm, pdf_ingest, prompts, store
from .config import BASE_DIR, UPLOADS_DIR, load_models_config
from .graph import graph, process_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=__import__("sys").stdout,
)
log = logging.getLogger("study-agent")

app = FastAPI(title="Study Agent")
store.init_db()
store.clear_all_thread_ids()

STATIC_DIR = BASE_DIR / "static"


class StartGraphRequest(BaseModel):
    session_id: str
    model_id: str | None = None


class ConfirmTopicsRequest(BaseModel):
    session_id: str
    topics: list[dict]
    model_id: str | None = None


class ChatRequest(BaseModel):
    message: str


def _run_in_thread(fn, *args):
    threading.Thread(target=fn, args=args, daemon=True).start()


_running: set[str] = set()
_running_lock = threading.Lock()


def _try_acquire(session_id: str) -> bool:
    with _running_lock:
        if session_id in _running:
            return False
        _running.add(session_id)
        return True


def _release(session_id: str):
    with _running_lock:
        _running.discard(session_id)


def _graph_busy(session_id: str) -> bool:
    """Граф занят, только если он сейчас выполняется или реально ждёт на interrupt
    (чекпоинт в памяти этого процесса). Протухшие записи в БД игнорируются."""
    tid = store.get_thread_id(session_id)
    if not tid:
        return False
    with _running_lock:
        if session_id in _running:
            return True
    g = process_graph if tid.endswith(":process") else graph
    try:
        if g.get_state({"configurable": {"thread_id": tid}}).next:
            return True
    except Exception:
        pass
    store.clear_thread_id(session_id)
    return False


@app.get("/api/models")
def api_models():
    cfg = load_models_config()
    return {"default": cfg.get("default"),
            "models": [{"id": m["id"], "label": m.get("label", m["id"])} for m in cfg["models"]]}


@app.post("/api/session")
def api_session():
    sid = uuid.uuid4().hex[:12]
    store.create_session(sid)
    events.get_queue(sid)
    return {"session_id": sid}


@app.post("/api/upload")
async def api_upload(session_id: str = Form(...), files: list[UploadFile] = File(...)):
    if not store.session_exists(session_id):
        store.create_session(session_id)
    saved = []
    for f in files:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(400, f"Файл {f.filename} не PDF")
        dest = UPLOADS_DIR / session_id
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / f.filename
        with open(path, "wb") as out:
            shutil.copyfileobj(f.file, out)
        saved.append(path)

    def ingest():
        try:
            for path in saved:
                pdf_ingest.extract_pdf(path, session_id)
            events.emit(session_id, "ingest", "done",
                        f"Текст извлечён: {store.corpus_count(session_id)} страниц в корпусе",
                        progress=100)
        except Exception as e:
            events.emit(session_id, "ingest", "error", f"Ошибка обработки PDF: {e}")

    _run_in_thread(ingest)
    return {"ok": True, "files": [p.name for p in saved]}


@app.get("/api/corpus")
def api_corpus(session_id: str):
    return {"docs": store.get_corpus(session_id)}


@app.post("/api/graph/start")
def api_graph_start(req: StartGraphRequest):
    if store.corpus_count(req.session_id) == 0:
        raise HTTPException(400, "Корпус пуст — сначала загрузите презентацию")
    if _graph_busy(req.session_id):
        raise HTTPException(409, "Граф уже запущен или ждёт подтверждения тем")
    if not _try_acquire(req.session_id):
        raise HTTPException(409, "Граф уже запущен для этой сессии — дождитесь завершения")

    def run():
        cfg = {"configurable": {"thread_id": req.session_id}}
        try:
            store.set_thread_id(req.session_id, req.session_id)
            log.info("[%s] Запуск графа (модель: %s)", req.session_id, req.model_id)
            graph.invoke(
                {"session_id": req.session_id, "model_id": req.model_id, "topics": [], "cards_done": 0},
                config=cfg)
        except Exception:
            log.exception("[%s] Ошибка графа при извлечении тем", req.session_id)
            events.emit(req.session_id, "graph", "error", "Ошибка графа при извлечении тем")
        finally:
            # Если граф остановился на подтверждении тем, thread_id нужен для resume
            try:
                if not graph.get_state(cfg).next:
                    store.clear_thread_id(req.session_id)
            except Exception:
                store.clear_thread_id(req.session_id)
            _release(req.session_id)

    _run_in_thread(run)
    return {"ok": True}


@app.get("/api/topics")
def api_topics(session_id: str):
    return {"topics": store.get_topics(session_id)}


@app.post("/api/topics/confirm")
def api_topics_confirm(req: ConfirmTopicsRequest):
    topics = [t for t in req.topics if isinstance(t, dict) and t.get("title", "").strip()]
    if not topics:
        raise HTTPException(400, "Список тем пуст")

    if not _try_acquire(req.session_id):
        raise HTTPException(409, "Граф уже выполняется — дождитесь завершения")

    def run():
        try:
            tid = store.get_thread_id(req.session_id)
            cfg = {"configurable": {"thread_id": tid}} if tid else None
            # resume возможен, только если основной граф реально ждёт на interrupt
            waiting = False
            if tid and not tid.endswith(":process"):
                try:
                    waiting = bool(graph.get_state(cfg).next)
                except Exception:
                    waiting = False
            if waiting:
                log.info("[%s] Подтверждение тем, продолжение графа", req.session_id)
                graph.invoke(Command(resume=topics), config=cfg)
                store.clear_thread_id(req.session_id)
            else:
                # граф уже завершился — перезапускаем только генерацию карточек
                log.info("[%s] Повторное подтверждение тем, запуск генерации карточек",
                         req.session_id)
                store.save_topics(req.session_id, topics, status="confirmed")
                store.delete_cards_not_in(req.session_id, [t["title"] for t in topics])
                ptid = req.session_id + ":process"
                store.set_thread_id(req.session_id, ptid)
                try:
                    process_graph.invoke(
                        {"session_id": req.session_id, "model_id": req.model_id,
                         "topics": topics, "cards_done": 0},
                        config={"configurable": {"thread_id": ptid}})
                finally:
                    store.clear_thread_id(req.session_id)
        except Exception:
            log.exception("[%s] Ошибка при подтверждении тем", req.session_id)
            events.emit(req.session_id, "graph", "error", "Ошибка при подтверждении тем")
        finally:
            _release(req.session_id)

    _run_in_thread(run)
    return {"ok": True}


@app.get("/api/cards")
def api_cards(session_id: str):
    return {"cards": store.get_cards(session_id)}


@app.get("/api/cards/{card_id}")
def api_card(card_id: int):
    card = store.get_card(card_id)
    if not card:
        raise HTTPException(404, "Карточка не найдена")
    card["chat"] = store.get_chat(card_id)
    return card


@app.post("/api/cards/{card_id}/chat")
def api_card_chat(card_id: int, req: ChatRequest, model_id: str | None = None):
    card = store.get_card(card_id)
    if not card:
        raise HTTPException(404, "Карточка не найдена")
    store.add_chat(card_id, "user", req.message)
    history = store.get_chat(card_id)
    system = prompts.CHAT_SYSTEM.format(
        title=card["topic"],
        original=(card["original_md"] or "")[:5000],
        simplified=(card["simplified_md"] or "")[:5000])
    messages = [{"role": "system", "content": system}]
    for h in history[-12:]:
        messages.append({"role": h["role"], "content": h["content"]})
    try:
        answer = llm.chat(messages, model_id=model_id, max_tokens=2048)
    except llm.LLMError as e:
        raise HTTPException(502, str(e))
    store.add_chat(card_id, "assistant", answer)
    return {"answer": answer}


class CleanupRequest(BaseModel):
    session_id: str


@app.post("/api/cleanup")
def api_cleanup(req: CleanupRequest):
    if _graph_busy(req.session_id):
        raise HTTPException(409, "Граф ещё работает — дождитесь завершения перед очисткой")
    shutil.rmtree(UPLOADS_DIR / req.session_id, ignore_errors=True)
    store.clear_session_data(req.session_id)
    events.emit(req.session_id, "cleanup", "done",
                "Данные сессии удалены: файлы, корпус, темы и карточки")
    log.info("[%s] Данные сессии очищены по запросу пользователя", req.session_id)
    return {"ok": True}


@app.get("/api/stream")
async def api_stream(session_id: str):
    q = events.get_queue(session_id)

    async def gen():
        for ev in store.get_events(session_id, limit=100):
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
        while True:
            try:
                ev = await asyncio.to_thread(q.get, True, 30)
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
