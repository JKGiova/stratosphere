"""Panel layouts chosen from the terminal's available character grid."""

from dataclasses import dataclass

MIN_PANEL_WIDTH = 48
MIN_PANEL_HEIGHT = 14
CHROME_HEIGHT = 4  # Brand, navigation, request status and keyboard footer.


@dataclass(frozen=True)
class WorkspaceLayout:
    name: str
    columns: int
    rows: int

    @property
    def count(self) -> int:
        return self.columns * self.rows


LAYOUTS = (
    WorkspaceLayout("six", 3, 2),
    WorkspaceLayout("four", 2, 2),
    WorkspaceLayout("two-horizontal", 2, 1),
    WorkspaceLayout("two-vertical", 1, 2),
    WorkspaceLayout("one", 1, 1),
)


def choose_layout(width: int, height: int) -> WorkspaceLayout:
    """Prefer the largest grid that preserves readable panel dimensions."""
    for layout in LAYOUTS[:-1]:
        if (
            width >= MIN_PANEL_WIDTH * layout.columns
            and height >= MIN_PANEL_HEIGHT * layout.rows + CHROME_HEIGHT
        ):
            return layout
    return LAYOUTS[-1]
