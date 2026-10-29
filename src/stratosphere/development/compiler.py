"""Generate one auditable Python form; only exact application output may be imported."""

import json

from stratosphere.development.contracts import validate_program
from stratosphere.phase0.contracts import fingerprint


def compile_strategy(spec):
    validate_program(spec)
    payload = json.dumps(spec.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return (
        '"""Application-compiled strategy; historical research only, no order authority."""\n'
        "import json\n"
        "from stratosphere.development.signals import SignalProgram\n\n"
        f"SPECIFICATION_SHA256 = {fingerprint(spec)!r}\n"
        f"_SPECIFICATION = json.loads({payload!r})\n\n"
        "class GeneratedStrategy(SignalProgram):\n"
        "    def __init__(self):\n"
        "        super().__init__(_SPECIFICATION)\n"
    ).encode()


def load_generated(path, spec):
    if path.is_symlink() or path.read_bytes() != compile_strategy(spec):
        raise ValueError("Generated strategy differs from the validated compiler output")
    # Read/compile the verified bytes directly; never reuse stale .pyc files.
    namespace = {"__name__": "stratosphere_compiled_strategy", "__builtins__": __builtins__}
    exec(compile(compile_strategy(spec), str(path), "exec"), namespace)
    return namespace["GeneratedStrategy"]
