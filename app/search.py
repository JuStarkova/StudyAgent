try:
    from ddgs import DDGS
except ImportError:  # duckduckgo-search < 9
    from duckduckgo_search import DDGS


def web_search(query: str, max_results: int = 4) -> list[dict]:
    """Возвращает [{title, url, snippet}] или [] при ошибке."""
    try:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("href", "") or r.get("link", ""),
                    "snippet": r.get("body", "") or r.get("snippet", ""),
                })
        return results
    except Exception:
        return []
