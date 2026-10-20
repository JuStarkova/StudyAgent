from typing import TypedDict


class AgentState(TypedDict, total=False):
    session_id: str
    model_id: str | None
    topics: list[dict]
    cards_done: int
