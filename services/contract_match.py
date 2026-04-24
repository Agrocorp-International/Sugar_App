"""Shared normalizer that collapses AGP/AGS contract references from
different sources onto a single master key for hedge matching.

    Physical Sub Contract Name  'SAGP/26/03/26260-1'  ─┐
    Positions contract_xl       'AGP/26/03/26260_2'   ─┼──►  'AGP/26/03/26260'
    Positions contract_xl       'AGS/25/07/27437'     ─┘
"""


def master_key(ref):
    """Return the master contract key (``AGP/YY/MM/NNNNN`` or ``AGS/...``)
    for an AGP/AGS reference, or ``None`` for anything else.

    Strips the leading ``S`` that physical-trade sub-contract names carry
    and drops the trailing ``-K`` shipment index / ``_N`` spread-leg suffix
    so both sides collapse onto the same master contract identifier.
    """
    s = (ref or "").strip().upper()
    if not s:
        return None
    if s.startswith("SAGP") or s.startswith("SAGS"):
        s = s[1:]
    s = s.split("-", 1)[0].split("_", 1)[0]
    if s.startswith("AGP/") or s.startswith("AGS/"):
        return s
    return None
