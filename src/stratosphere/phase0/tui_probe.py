"""Small Textual capability probe; not the stratosphere product interface."""

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Footer, Input, Static


class FrameworkProbe(App[None]):
    TITLE = "stratosphere — Phase 0 framework probe"
    BINDINGS = [("ctrl+q", "quit", "Quit probe")]
    CSS = """
    VerticalScroll { height: 1fr; padding: 1; }
    Input { dock: bottom; }
    """

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            yield Static("Framework probe only", id="result", markup=False)
        yield Input(placeholder="Type text and press Enter", id="input")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.query_one("#result", Static).update(event.value)
        event.input.value = ""


if __name__ == "__main__":
    FrameworkProbe().run()
