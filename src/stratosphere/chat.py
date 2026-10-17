"""Ephemeral chat workflow; incomplete/canceled turns never enter model history."""

from collections.abc import AsyncIterator
from typing import TypedDict

from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from stratosphere.models.nim import NimChat

SYSTEM_PROMPT = """You are the chief research assistant for stratosphere, part of tradingsphere.
Respond in English. This development increment supports conversation only. You have no tools,
registry, source acquisition, backtest launcher, account access or trading actions. Never claim to
have performed an operation. Ask questions about ambiguous strategy ideas. Distinguish proposed
rules and assumptions from verified results. Do not ask users to paste credentials into chat.
"""


class ChatState(TypedDict):
    messages: list[dict[str, str]]
    reply: str


class ChatSession:
    def __init__(self, provider: NimChat):
        self.provider = provider
        self.history: list[dict[str, str]] = []
        self.busy = False
        graph = StateGraph(ChatState)
        graph.add_node("chat", self._chat)
        graph.add_edge(START, "chat")
        graph.add_edge("chat", END)
        self.graph = graph.compile()

    async def _chat(self, state: ChatState) -> dict[str, str]:
        write = get_stream_writer()
        reply = ""
        async for delta in self.provider.stream(state["messages"]):
            reply += delta
            write(delta)
        return {"reply": reply}

    async def respond(self, text: str) -> AsyncIterator[str]:
        if self.busy:
            raise ValueError("A response is already running.")
        self.busy = True
        request = [*self.history, {"role": "user", "content": text}]
        reply = None
        try:
            # LangSmith ships with the pinned LangGraph stack. Ambient tracing must
            # not export this ephemeral private conversation to a second service.
            with tracing_context(enabled=False):
                async for mode, value in self.graph.astream(
                    {"messages": [{"role": "system", "content": SYSTEM_PROMPT}, *request]},
                    stream_mode=["custom", "updates"],
                ):
                    if mode == "custom":
                        yield value
                    elif "chat" in value:
                        reply = value["chat"]["reply"]
            if reply is not None:
                self.history = [*request, {"role": "assistant", "content": reply}]
        finally:
            self.busy = False
