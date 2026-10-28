"""Headless MT5 connector; explicit local user approvals only."""

import argparse
import asyncio
import json
from pathlib import Path

from stratosphere.mt5.bridge import BridgeError, MT5Bridge, private_path
from stratosphere.mt5.trading import Deployment, MT5Trading, Order
from stratosphere.phase0.contracts import StrategyDocument


def read_file(path):
    path = Path(path)
    private_path(path)
    if path.stat().st_size > 8_000_000:
        raise BridgeError("Oversized local deployment input")
    return path.read_bytes()


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--root", type=Path)
    root.add_argument("--output", type=Path, help="New private diagnostic/result file")
    sub = root.add_subparsers(dest="command", required=True)
    for name in ("account", "positions", "orders"):
        sub.add_parser(name)
    for name in ("symbol", "quote"):
        sub.add_parser(name).add_argument("symbol")
    rates = sub.add_parser("rates")
    rates.add_argument("symbol")
    rates.add_argument("frame", help="M1..MN1, exact MT5 standard interval")
    rates.add_argument("--count", type=int, default=512)
    rates.add_argument("--end", type=int, help="Raw server/chart epoch; UTC mapping not inferred")
    deals = sub.add_parser("deals")
    deals.add_argument("start", type=int)
    deals.add_argument("end", type=int)
    draft = sub.add_parser("draft", help="Prepare final connector deployment; grants no authority")
    draft.add_argument("strategy", type=Path)
    draft.add_argument("report", type=Path)
    draft.add_argument(
        "limits", type=Path, help="Deployment JSON with explicit instrument/risk limits"
    )
    show = sub.add_parser("review")
    show.add_argument("deployment", type=Path)
    approve = sub.add_parser("approve")
    approve.add_argument("deployment", type=Path)
    approve.add_argument("digest")
    approve.add_argument("--allow-real", action="store_true")
    revoke = sub.add_parser("revoke")
    revoke.add_argument("digest")
    for name in ("check", "submit"):
        order = sub.add_parser(name)
        order.add_argument("digest")
        order.add_argument("order", type=Path, help="Typed signal/order JSON, no acquired code")
    close = sub.add_parser("close")
    close.add_argument("digest")
    close.add_argument("ticket")
    return root


async def execute(args):
    if args.command == "review":
        value = Deployment.model_validate_json(read_file(args.deployment))
        return {"deployment": value.model_dump(mode="json"), "digest": value.digest}
    bridge = MT5Bridge(args.root)
    trading = MT5Trading(bridge)
    if args.command == "draft":
        strategy_bytes, report_bytes = read_file(args.strategy), read_file(args.report)
        StrategyDocument.model_validate_json(strategy_bytes)
        account = await bridge.account()
        limits = json.loads(read_file(args.limits))
        # Pins are application-owned, not copied from a proposed model/deployment file.
        limits.update(
            strategy_sha256=bridge.retain_evidence(strategy_bytes),
            report_sha256=bridge.retain_evidence(report_bytes),
            implementation_sha256=bridge.implementation_digest,
            ea_sha256=account["ea_sha256"],
            login=account["login"],
            server=account["server"],
            mode=account["mode"],
            currency=account["currency"],
        )
        value = Deployment.model_validate(limits)
        return {"deployment": value.model_dump(mode="json"), "digest": value.digest}
    if args.command == "approve":
        value = Deployment.model_validate_json(read_file(args.deployment))
        return {"approved": await trading.approve(value, args.digest, allow_real=args.allow_real)}
    if args.command == "revoke":
        await trading.revoke(args.digest)
        return {"revoked": args.digest}
    if args.command in {"check", "submit"}:
        return await trading.order(
            args.digest,
            Order.model_validate_json(read_file(args.order)),
            check_only=args.command == "check",
        )
    if args.command == "close":
        return await trading.close(args.digest, args.ticket)
    fields = {
        name: getattr(args, name)
        for name in ("symbol", "start", "end", "count", "frame")
        if hasattr(args, name) and getattr(args, name) is not None
    }
    if "frame" in fields:
        fields["frame"] = "PERIOD_" + fields["frame"].removeprefix("PERIOD_")
    return await bridge.read(args.command, **fields)


def main():
    args = parser().parse_args()
    try:
        value = asyncio.run(execute(args))
        result = json.dumps(value, indent=2) + "\n"
        if args.output:
            private_path(args.output)
            import os

            fd = os.open(args.output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(result)
            print(f"Saved private MT5 result: {args.output}")
        else:
            print(result, end="")
    except (BridgeError, ValueError, OSError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
