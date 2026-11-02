"""Separate account exposure from exact application journal ownership."""

from decimal import Decimal

from stratosphere.mt5.bridge import BridgeError, decode


def rows(snapshot):
    """Validate a complete native position/order snapshot before classifying any row."""
    result, tickets = [], set()
    count = int(snapshot["count"])
    if not 0 <= count <= 1000:
        raise BridgeError("Invalid exposure count")
    for i in range(1, count + 1):
        prefix = f"p.{i}."
        row = {k.removeprefix(prefix): v for k, v in snapshot.items() if k.startswith(prefix)}
        if (
            not row.get("ticket", "").isdigit()
            or int(row["ticket"]) <= 0
            or row["ticket"] in tickets
            or not row.get("magic", "").isdigit()
            or not row.get("symbol")
            or not Decimal(row.get("volume", "NaN")).is_finite()
            or Decimal(row["volume"]) <= 0
        ):
            raise BridgeError("Malformed exposure identity/volume")
        tickets.add(row["ticket"])
        result.append(row)
    return result


def snapshot(rows):
    return {"count": str(len(rows))} | {
        f"p.{i}.{k}": v for i, row in enumerate(rows, 1) for k, v in row.items()
    }


def ownership(bridge, positions, orders):
    """Never infer application ownership from a symbol or a nonzero magic alone."""
    with bridge.journal() as db:
        attempts = db.execute(
            "SELECT payload,state,response FROM requests WHERE op='submit'"
        ).fetchall()
    owners = {}
    for payload, state, response in attempts:
        request = decode(payload)
        magic = request.get("magic")
        if not magic:
            continue
        if magic in owners:
            raise BridgeError("Duplicate application position identity")
        owners[magic] = (request, state, decode(response) if response else {})
    owned, external = [], []
    for row in rows(positions):
        match = owners.get(row["magic"])
        if match is None:
            external.append(row)
            continue
        request, state, response = match
        if (
            state != "observed"
            or response.get("status") != "ok"
            or response.get("retcode") not in {"10009", "10010"}
            or request.get("symbol") != row["symbol"]
            or Decimal(response.get("filled_volume", "0")) != Decimal(row["volume"])
        ):
            raise BridgeError("Application position changed or has an unresolved fill")
        owned.append(row | {"approval": request["approval"]})
    pending = rows(orders)
    if any(row["magic"] in owners for row in pending):
        raise BridgeError("Application pending order needs reconciliation")
    return owned, external, pending


def coexistence_block(bridge, positions, orders, symbol):
    owned, external, pending = ownership(bridge, positions, orders)
    if len(owned) > 1:
        raise BridgeError("Multiple application positions violate the approved scope")
    if owned:
        return "An application position already occupies the agent slot"
    if any(row["symbol"] == symbol for row in external + pending):
        return "External exposure on this symbol; no mixing or reinforcement"
    return None
