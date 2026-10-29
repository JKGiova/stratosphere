"""Credential-free finite child processes; cancellation cannot leave late workers running."""

import asyncio
import json
import os
import sys
import tempfile
import uuid

from stratosphere.research.store import ResearchError


async def worker(directory, action, input_name, *, multiplier=1):
    directory = directory.resolve()
    output = directory / ("worker_" + uuid.uuid7().hex + ".json")
    with tempfile.TemporaryDirectory(prefix="stratosphere-development-") as cache:
        environment = {
            "PATH": os.defpath,
            "HOME": cache,
            "TMPDIR": cache,
            "PYTHONUTF8": "1",
            "PYTHONNOUSERSITE": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "OMP_NUM_THREADS": "1",
        }
        spawning = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "stratosphere.development.worker",
                action,
                "--input",
                str(directory / input_name),
                "--output",
                str(output),
                "--cache",
                cache,
                "--multiplier",
                str(multiplier),
                cwd=directory,
                env=environment,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        )
        try:
            process = await asyncio.shield(spawning)
        except asyncio.CancelledError:
            while not spawning.done():
                try:
                    await asyncio.shield(spawning)
                except asyncio.CancelledError:
                    continue
            process = spawning.result()
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        try:
            async with asyncio.timeout(180):
                await process.wait()
            if process.returncode or not output.is_file():
                raise ResearchError("Development worker failed; inspect retained attempt, no retry")
            return json.loads(output.read_text())
        except TimeoutError:
            raise ResearchError("Development worker exceeded 180 seconds; no replay") from None
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
