"""Public-only CONNECT proxy for optional research processes; validated peers are pinned."""

import asyncio
import contextlib
import ipaddress

from stratosphere.research.store import ResearchError
from stratosphere.sources.web import public_addresses


class PublicProxy:
    def __init__(self, hosts, *, resolver=public_addresses, connector=asyncio.open_connection):
        self.hosts = frozenset(hosts)
        self.resolver, self.connector = resolver, connector
        self.tasks = set()
        self.denials = 0
        self.transferred = 0

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0, limit=16384)
        self.url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def peer(self, host, port):
        if host not in self.hosts or port != 443:
            raise ValueError("Unapproved research destination")
        peers = await self.resolver(host)
        if not peers or any(not ipaddress.ip_address(p).is_global for p in peers):
            raise ValueError("Nonpublic research destination")
        return peers[0]

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        try:
            async with asyncio.timeout(50):
                head = await reader.readuntil(b"\r\n\r\n")
                method, target, protocol = head.split(b"\r\n", 1)[0].decode("ascii").split()
                if method != "CONNECT" or protocol not in {"HTTP/1.0", "HTTP/1.1"}:
                    raise ValueError("HTTPS tunnels only")
                host, port = target.rsplit(":", 1)
                address = await self.peer(host.lower(), int(port))
                remote, upstream = await self.connector(address, 443)
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()

                async def relay(source, destination):
                    while block := await source.read(65536):
                        self.transferred += len(block)
                        if self.transferred > 40 * 1024 * 1024:
                            raise ValueError("Research network byte budget exceeded")
                        destination.write(block)
                        await destination.drain()

                tasks = [
                    asyncio.create_task(relay(reader, upstream)),
                    asyncio.create_task(relay(remote, writer)),
                ]
                try:
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for child in tasks:
                        child.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except (
            ValueError,
            ResearchError,
            OSError,
            TimeoutError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ):
            self.denials += 1
            with contextlib.suppress(OSError):
                writer.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")
                await writer.drain()
        finally:
            for stream in (upstream, writer):
                if stream:
                    stream.close()
                    with contextlib.suppress(OSError):
                        await stream.wait_closed()
            self.tasks.discard(task)
