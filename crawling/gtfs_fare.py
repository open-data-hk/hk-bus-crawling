from collections import namedtuple

FareRecord = namedtuple("FareRecord", ["on_seq", "off_seq", "price"])


def compress_fares(fares):
    """Compress flat fare records into a compact (off-section × on-group) matrix.

    Returns (sections, groups) where:
      sections: list of (off_from, off_to) tuples
      groups:   list of (on_from, on_to, prices) tuples; prices indexed by sections
    """
    matrix = {}
    for f in fares:
        matrix.setdefault(f.on_seq, {})[f.off_seq] = float(f.price)

    if not matrix:
        return [], []

    # Find off_seq values where price changes (section boundaries)
    breakpoints = set()
    for offs in matrix.values():
        prev_price = None
        for off, price in sorted(offs.items()):
            if price != prev_price and prev_price is not None:
                breakpoints.add(off)
            prev_price = price

    all_offs = sorted({off for offs in matrix.values() for off in offs})
    breakpoints_sorted = sorted(breakpoints)

    section_starts = [all_offs[0]] + breakpoints_sorted
    section_ends = [bp - 1 for bp in breakpoints_sorted] + [all_offs[-1]]
    sections = list(zip(section_starts, section_ends))

    def section_price(on, off_from, off_to):
        offs = matrix[on]
        for off in range(off_from, off_to + 1):
            if off in offs:
                return offs[off]
        return None

    groups = []
    prev_vec = None
    group_start = None
    prev_on = None
    for on in sorted(matrix):
        vec = tuple(section_price(on, s, e) for s, e in sections)
        if vec != prev_vec or (prev_on is not None and on != prev_on + 1):
            if prev_vec is not None:
                groups.append((group_start, prev_on, list(prev_vec)))
            group_start = on
            prev_vec = vec
        prev_on = on
    if prev_vec is not None:
        groups.append((group_start, prev_on, list(prev_vec)))

    return sections, groups


def fare_list_to_csv(fares):
    """Convert per-boarding-stop fares to the compact fare CSV format."""
    fare_records = [
        FareRecord(on_seq, off_seq, fare)
        for on_seq, fare in enumerate(fares, start=1)
        for off_seq in range(on_seq + 1, len(fares) + 2)
    ]
    sections, groups = compress_fares(fare_records)
    return fares_to_csv(sections, groups)


def fares_to_csv(sections, groups):
    """Serialize compressed fare matrix to a compact CSV string.

    Example:
        ,2-52,53-74
        1-26,10,12.2
        27-35,7.8,10
        52-73,,6.7
    """
    header = "" + "".join(f",{s}-{e}" for s, e in sections)
    rows = [header]
    for on_from, on_to, prices in groups:
        cells = [f"{on_from}-{on_to}"]
        for p in prices:
            cells.append("" if p is None else f"{p:g}")
        rows.append(",".join(cells))
    return "\n".join(rows)


def parse_fare_csv(csv_str):
    """Parse a fare CSV string (produced by fares_to_csv) back into (sections, groups)."""
    lines = csv_str.strip().splitlines()
    header_cells = lines[0].split(",")
    sections = []
    for col in header_cells[1:]:
        off_from, off_to = col.split("-")
        sections.append((int(off_from), int(off_to)))

    groups = []
    for line in lines[1:]:
        cells = line.split(",")
        on_from, on_to = cells[0].split("-")
        prices = [None if c == "" else float(c) for c in cells[1:]]
        groups.append((int(on_from), int(on_to), prices))

    return sections, groups


def get_fare(csv_str, on_seq, off_seq):
    """Return the fare for boarding at on_seq and alighting at off_seq, or None."""
    sections, groups = parse_fare_csv(csv_str)

    off_idx = next((i for i, (s, e) in enumerate(sections) if s <= off_seq <= e), None)
    if off_idx is None:
        return None

    for on_from, on_to, prices in groups:
        if on_from <= on_seq <= on_to:
            return prices[off_idx]

    return None


def has_bidirectional_sectional_fare(csv_by_route_seq):
    """Return True if the route has sectional fares in both directions."""
    if len(csv_by_route_seq) < 2:
        return False

    def is_sectional(csv_str):
        _, groups = parse_fare_csv(csv_str)
        prices = {p for _, _, ps in groups for p in ps if p is not None}
        return len(prices) > 1

    return all(is_sectional(csv) for csv in csv_by_route_seq.values())
