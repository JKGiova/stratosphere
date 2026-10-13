# Repository development instructions

## Language

Write all repository development material in English, including code identifiers, comments, docstrings, documentation, examples, commit messages, issues, and pull request titles and descriptions.

## Naming

Use **stratosphere** for this product and **tradingsphere** for the suite. Both names are lowercase; tradingsphere is one word. Preserve proper names and capitalization for external products and APIs.

## Development workflow

Keep development changes and their history on GitHub in this repository. GitHub is the shared source of truth. Update the relevant documentation when product behavior or technical decisions change.

Clearly distinguish planned features from implemented and verified functionality. Do not describe a connector, backtesting engine, or live execution integration as available before it exists and has been verified.

## Conversation transcript

The user explicitly requested the full planning chat in docs/conversation-log.md and an update whenever this conversation continues.

- For this same stratosphere planning conversation, update the log on each subsequent exchange, including the new user messages, user-visible progress updates, and the finalized assistant response.
- Read the current GitHub log before editing, preserve earlier entries, and append chronologically without duplicating already recorded messages.
- Preserve original language, wording, and corrections. This file is the explicit exception to the English-only repository convention; new code and plan documentation remain in English.
- Convert source citation cards to their actual Markdown source links and interactive widgets to readable text. Do not summarize or silently rewrite the historical dialogue.
- Do not include internal instructions, tool traces, environment metadata, unrelated memories, or confidential credentials. If redaction is necessary, mark it explicitly.
- Keep historical discussion separate from confirmed requirements in docs/decisions.md.
- Commit log updates to GitHub using the current branch head and verify the published content. Combine log updates with task changes when appropriate.
- If publishing fails or a later user instruction prohibits a repository write, explain the gap instead of claiming synchronization.
- This workflow does not provide an autonomous background trigger for ChatGPT messages. Do not create unrelated scheduled polling or imply that such a trigger exists.
