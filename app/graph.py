import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from . import events, llm, prompts, search, store
from .state import AgentState

log = logging.getLogger("study-agent.graph")

CHUNK_SIZE = 12000
MIN_CHUNK = 2000
PAGE_PREVIEW = 900
TOP_DOCS = 14


def _corpus_blob(docs: list[dict]) -> str:
    parts = []
    for d in docs:
        parts.append(f"=== {d['source']} стр. {d['page']} ===\n{d['content']}")
    return "\n\n".join(parts)


def _chunks(text: str, size: int) -> list[str]:
    return [text[i:i + size] for i in range(0, len(text), size)]


def _ask_topics(text: str, model_id: str | None, system: str) -> list[dict]:
    out = llm.chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": text}],
        model_id=model_id)
    data = llm.extract_json(out)
    topics = data.get("topics", []) if isinstance(data, dict) else []
    return [t for t in topics if isinstance(t, dict) and t.get("title")]


def _ask_topics_iter(text: str, model_id: str | None, system: str, depth: int = 0) -> list[dict]:
    """Итеративное извлечение: при ошибке LLM (в т.ч. превышении контекста)
    делит фрагмент пополам и повторяет, пока фрагмент не станет достаточно мал."""
    try:
        return _ask_topics(text, model_id, system)
    except llm.LLMError:
        if len(text) <= MIN_CHUNK or depth >= 5:
            raise
        mid = len(text) // 2
        cut = text.rfind("\n", mid // 2, mid + mid // 2)
        if cut == -1:
            cut = mid
        log.warning("Фрагмент %d символов не обработался, делю пополам (глубина %d)",
                    len(text), depth + 1)
        return (_ask_topics_iter(text[:cut], model_id, system, depth + 1)
                + _ask_topics_iter(text[cut:], model_id, system, depth + 1))


def _dedup(topics: list[dict]) -> list[dict]:
    seen, out = set(), []
    for t in topics:
        k = (t.get("title") or "").strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(t)
    return out


def _merge_topics(partial: list[dict], model_id: str | None, sid: str) -> list[dict]:
    """Итеративное объединение: сначала батчами, затем финальный проход."""
    topics = partial
    events.emit(sid, "topics", "running",
                f"Объединение тем: {len(partial)} тем от фрагментов", progress=90)
    log.info("[%s] Объединение тем: %d тем до слияния", sid, len(partial))
    round_no = 0
    while len(topics) > 12:
        round_no += 1
        batches = [topics[i:i + 12] for i in range(0, len(topics), 12)]

        def work(batch: list[dict]) -> list[dict]:
            try:
                return _ask_topics_iter(
                    json.dumps(batch, ensure_ascii=False), model_id,
                    prompts.TOPICS_MERGE_SYSTEM)
            except llm.LLMError as e:
                log.warning("Слияние батча тем не удалось: %s", e)
                return batch

        log.info("[%s] Слияние тем: раунд %d, %d групп параллельно",
                 sid, round_no, len(batches))
        nxt: list[dict] = []
        done = 0
        with ThreadPoolExecutor(max_workers=min(4, len(batches))) as ex:
            futs = {ex.submit(work, b): b for b in batches}
            for fut in as_completed(futs):
                nxt.extend(fut.result())
                done += 1
                events.emit(sid, "topics", "running",
                            f"Объединение тем: раунд {round_no}, готово групп "
                            f"{done}/{len(batches)}",
                            progress=min(90 + round_no * 2 + done / len(batches), 96))
                log.info("[%s] Слияние тем: раунд %d, готово групп %d/%d",
                         sid, round_no, done, len(batches))
        topics = _dedup(nxt)
        events.emit(sid, "topics", "running",
                    f"Объединение тем: после раунда {round_no} осталось {len(topics)} тем",
                    progress=min(92 + round_no * 2, 96))
        log.info("[%s] Слияние тем: после раунда %d осталось %d тем", sid, round_no, len(topics))
    try:
        events.emit(sid, "topics", "running",
                    f"Финальное слияние {len(topics)} тем…", progress=97)
        log.info("[%s] Финальное слияние %d тем", sid, len(topics))
        merged = _ask_topics_iter(json.dumps(topics, ensure_ascii=False),
                                  model_id, prompts.TOPICS_MERGE_SYSTEM)
        return merged or topics
    except llm.LLMError as e:
        log.warning("Финальное слияние тем не удалось: %s", e)
        return topics


def extract_topics_node(state: AgentState) -> dict:
    sid = state["session_id"]
    model_id = state.get("model_id")
    events.emit(sid, "topics", "running", "Анализ корпуса и извлечение тем…")
    docs = store.get_corpus(sid)
    if not docs:
        events.emit(sid, "topics", "error", "Корпус пуст — сначала загрузите PDF")
        return {"topics": []}

    blob = _corpus_blob(docs)
    chunks = _chunks(blob, CHUNK_SIZE)
    log.info("[%s] Извлечение тем: корпус %d страниц, %d символов, %d фрагментов",
             sid, len(docs), len(blob), len(chunks))
    partial_lists: list[list[dict] | None] = [None] * len(chunks)
    failed = 0
    with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as ex:
        futs = {ex.submit(_ask_topics_iter, ch, model_id, prompts.TOPICS_SYSTEM): i
                for i, ch in enumerate(chunks)}
        done = 0
        for fut in as_completed(futs):
            i = futs[fut]
            done += 1
            try:
                partial_lists[i] = fut.result()
                events.emit(sid, "topics", "running",
                            f"Извлечение тем: готово частей {done}/{len(chunks)}",
                            progress=(done / len(chunks)) * 85)
            except llm.LLMError as e:
                failed += 1
                log.warning("[%s] Фрагмент %d/%d не обработан: %s", sid, i + 1, len(chunks), e)
                events.emit(sid, "topics", "running",
                            f"Часть {i + 1}/{len(chunks)} пропущена из-за ошибки: {e}",
                            progress=(done / len(chunks)) * 85)
    partial: list[dict] = [t for lst in partial_lists if lst for t in lst]

    if not partial:
        msg = f"Не удалось извлечь темы ни из одного фрагмента ({failed}/{len(chunks)} ошибок)"
        log.error("[%s] %s", sid, msg)
        events.emit(sid, "topics", "error", msg)
        return {"topics": []}

    if len(chunks) > 1:
        events.emit(sid, "topics", "running", "Объединение списков тем…", progress=90)
        topics = _merge_topics(_dedup(partial), model_id, sid)
    else:
        topics = _dedup(partial)

    store.save_topics(sid, topics, status="draft")
    log.info("[%s] Извлечено тем: %d (пропущено фрагментов: %d)", sid, len(topics), failed)
    events.emit(sid, "topics", "done",
                f"Найдено тем: {len(topics)}. Проверьте и подтвердите список.", progress=100)
    return {"topics": topics}


def confirm_topics_node(state: AgentState) -> dict:
    sid = state["session_id"]
    confirmed = interrupt({"type": "confirm_topics", "topics": state.get("topics", [])})
    if not isinstance(confirmed, list):
        confirmed = state.get("topics", [])
    norm = []
    for i, t in enumerate(confirmed):
        if isinstance(t, dict) and t.get("title"):
            norm.append({"title": t["title"].strip(),
                         "summary": (t.get("summary") or "").strip(),
                         "pages": t.get("pages", [])})
    store.save_topics(sid, norm, status="confirmed")
    store.delete_cards_not_in(sid, [t["title"] for t in norm])
    events.emit(sid, "topics", "done", f"Список тем подтверждён: {len(norm)}")
    return {"topics": norm}


def _relevant_docs(topic: dict, docs: list[dict]) -> list[dict]:
    text = (topic.get("title", "") + " " + topic.get("summary", "")).lower()
    words = {w for w in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{4,}", text)}
    scored = []
    for d in docs:
        content = d["content"].lower()
        score = sum(content.count(w) for w in words)
        if score > 0:
            scored.append((score, d))
    scored.sort(key=lambda x: -x[0])
    return [d for _, d in scored[:TOP_DOCS]] or docs[:6]


def _gather(topic: dict, docs: list[dict], model_id: str | None) -> str:
    rel = _relevant_docs(topic, docs)
    excerpts = "\n\n".join(
        f"--- {d['source']} стр. {d['page']} ---\n{d['content'][:2500]}" for d in rel)
    return llm.chat(
        [{"role": "system", "content": prompts.GATHER_SYSTEM},
         {"role": "user", "content": f"Тема: {topic['title']}\nОписание: {topic.get('summary','')}\n\nФрагменты слайдов:\n{excerpts}"}],
        model_id=model_id, max_tokens=50000)


def _verify(original_md: str, model_id: str | None) -> dict:
    out = llm.chat(
        [{"role": "system", "content": prompts.VERIFY_SYSTEM},
         {"role": "user", "content": f"Конспект темы:\n\n{original_md[:8000]}"}],
        model_id=model_id, max_tokens=50000)
    data = llm.extract_json(out)
    if not isinstance(data, dict):
        return {"has_errors": False, "errors": []}
    data.setdefault("has_errors", bool(data.get("errors")))
    data.setdefault("errors", [])
    return data


def _simplify(topic: dict, original_md: str, verification: dict, model_id: str | None) -> str:
    import json as _json
    system = prompts.SIMPLIFY_SYSTEM
    if verification.get("has_errors") and verification.get("errors"):
        system += prompts.SIMPLIFY_WITH_ERRORS_ADDON.format(
            errors=_json.dumps(verification["errors"], ensure_ascii=False, indent=2))
    return llm.chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": f"Тема: {topic['title']}\n\nДословный конспект:\n{original_md[:8000]}"}],
        model_id=model_id, max_tokens=50000)


def _examples(topic: dict, original_md: str, model_id: str | None) -> list[dict]:
    import json as _json
    try:
        out = llm.chat(
            [{"role": "system", "content": prompts.EXAMPLES_QUERIES_SYSTEM},
             {"role": "user", "content": f"Тема: {topic['title']}\n\nКонспект:\n{original_md[:3000]}"}],
            model_id=model_id, max_tokens=50000)
        queries = llm.extract_json(out).get("queries", [])[:3]
    except llm.LLMError:
        queries = [topic["title"]]

    results: list[dict] = []
    for q in queries:
        results.extend(search.web_search(q, max_results=4))

    if not results:
        try:
            out = llm.chat(
                [{"role": "system", "content": prompts.EXAMPLES_FALLBACK_SYSTEM},
                 {"role": "user", "content": f"Тема: {topic['title']}\n\nКонспект:\n{original_md[:3000]}"}],
                model_id=model_id, max_tokens=50000)
            return llm.extract_json(out).get("examples", [])
        except llm.LLMError:
            return []

    seen, uniq = set(), []
    for r in results:
        if r["url"] and r["url"] not in seen:
            seen.add(r["url"])
            uniq.append(r)
    listing = _json.dumps(uniq[:10], ensure_ascii=False, indent=1)
    try:
        out = llm.chat(
            [{"role": "system", "content": prompts.EXAMPLES_SELECT_SYSTEM},
{"role": "user", "content": f"Тема: {topic['title']}\n\nКраткий конспект:\n{original_md[:2000]}\n\nРезультаты поиска:\n{listing}"}],
        model_id=model_id, max_tokens=50000)
        return llm.extract_json(out).get("examples", [])
    except llm.LLMError:
        return [{"title": r["title"], "url": r["url"], "relevance": "", "snippet": r["snippet"]}
                for r in uniq[:4]]


SUBWEIGHTS = {"gather": 20, "verify": 30, "simplify": 25, "examples": 25}


def process_topics_node(state: AgentState) -> dict:
    sid = state["session_id"]
    model_id = state.get("model_id")
    topics = state.get("topics", [])
    docs = store.get_corpus(sid)
    total = len(topics)
    start = time.time()

    for i, topic in enumerate(topics):
        title = topic["title"]
        base_progress = (i / total) * 100
        def sub(stage: str, name: str):
            eta = None
            if i > 0:
                elapsed = time.time() - start
                eta = (elapsed / i) * (total - i)
            w_before = sum(v for k, v in SUBWEIGHTS.items()
                           if list(SUBWEIGHTS).index(k) < list(SUBWEIGHTS).index(stage))
            events.emit(sid, "cards", "running",
                        f"Тема {i + 1}/{total} «{title}»: {name}",
                        progress=base_progress + (w_before / total),
                        eta=eta, topic=title)

        try:
            sub("gather", "сбор материала из презентации")
            original_md = _gather(topic, docs, model_id)

            sub("verify", "проверка теории на ошибки")
            try:
                verification = _verify(original_md, model_id)
            except llm.LLMError:
                verification = {"has_errors": False, "errors": []}

            sub("simplify", "упрощённое изложение")
            simplified_md = _simplify(topic, original_md, verification, model_id)

            sub("examples", "поиск примеров в интернете")
            examples = _examples(topic, original_md, model_id)

            store.delete_card_by_topic(sid, title)
            card_id = store.save_card(
                sid, title, original_md, simplified_md,
                bool(verification.get("has_errors")),
                verification.get("errors", []), examples)
            log.info("[%s] Карточка «%s» сохранена (id=%s)", sid, title, card_id)

            err_note = " (найдены ошибки в теории)" if verification.get("has_errors") else ""
            events.emit(sid, "cards", "running",
                        f"Карточка «{title}» готова{err_note}",
                        progress=((i + 1) / total) * 100,
                        eta=0 if i + 1 == total else None,
                        topic=f"card:{card_id}")
        except llm.LLMError as e:
            log.warning("[%s] Тема «%s»: %s", sid, title, e)
            events.emit(sid, "cards", "error",
                        f"Тема «{title}»: {e}", topic=title)

    events.emit(sid, "finish", "done",
                f"Обработка завершена: {total} тем", progress=100, eta=0)
    return {"cards_done": total}


def build_graph():
    builder = StateGraph(AgentState)
    builder.add_node("extract_topics", extract_topics_node)
    builder.add_node("confirm_topics", confirm_topics_node)
    builder.add_node("process_topics", process_topics_node)
    builder.add_edge(START, "extract_topics")
    builder.add_edge("extract_topics", "confirm_topics")
    builder.add_edge("confirm_topics", "process_topics")
    builder.add_edge("process_topics", END)
    return builder.compile(checkpointer=InMemorySaver())


def build_process_graph():
    """Граф только генерации карточек — для повторного запуска после завершения основного."""
    builder = StateGraph(AgentState)
    builder.add_node("process_topics", process_topics_node)
    builder.add_edge(START, "process_topics")
    builder.add_edge("process_topics", END)
    return builder.compile(checkpointer=InMemorySaver())


graph = build_graph()
process_graph = build_process_graph()
