"""Draws docs/outbox-flow.svg: how an event travels through the outbox, Kafka and a consumer.

Run from the repository root:   python docs/diagrams/outbox-flow.py
Then, to refresh the PNG, open docs/outbox-flow.svg in a browser and export it, or use any SVG-to-PNG tool.
"""
from html import escape
from pathlib import Path

W, H = 1720, 860
FONT = "Liberation Sans, Arial, Helvetica, sans-serif"
out = []


def add(s):
    out.append(s)


def text(x, y, lines, size=14, anchor="middle", weight="normal", italic_from=None, color="#111", halo=False):
    """Multi-line text; lines may be str or (str, style) where style in {'b','i'}."""
    if isinstance(lines, str):
        lines = [lines]
    lh = size * 1.3
    y0 = y - (len(lines) - 1) * lh / 2
    for i, line in enumerate(lines):
        fw, fill, fs = weight, color, "normal"
        if isinstance(line, tuple):
            line, st = line
            if st == "b":
                fw = "bold"
            elif st == "i":
                fs, fill = "italic", "#444"
        h = ' stroke="#ffffff" stroke-width="5" stroke-linejoin="round" paint-order="stroke"' if halo else ""
        add(f'<text x="{x}" y="{y0 + i * lh:.1f}" font-size="{size}" text-anchor="{anchor}" '
            f'dominant-baseline="middle" fill="{fill}" font-weight="{fw}" font-style="{fs}"{h}>{escape(line)}</text>')


def group(x, y, w, h, title, sub, fill, stroke):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
    text(x + 14, y + 22, [(title, "b")], size=16, anchor="start")
    if sub:
        text(x + 14, y + 42, [(sub, "i")], size=12.5, anchor="start")


def box(x, y, w, h, lines, fill, stroke, dash=False, width=1.5, size=14):
    d = ' stroke-dasharray="6 4"' if dash else ""
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" stroke="{stroke}" stroke-width="{width}"{d}/>')
    text(x + w / 2, y + h / 2, lines, size=size)


def cylinder(cx, cy, w, h, lines, fill="#c9c9ff", stroke="#4b4b9a"):
    rx, ry = w / 2, 9
    top, bot = cy - h / 2, cy + h / 2
    add(f'<path d="M{cx - rx},{top} L{cx - rx},{bot} A{rx},{ry} 0 0 0 {cx + rx},{bot} L{cx + rx},{top}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
    add(f'<ellipse cx="{cx}" cy="{top}" rx="{rx}" ry="{ry}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
    text(cx, cy + 5, lines)


def arrow(points, label=None, lx=None, ly=None, dash=False, anchor="middle", size=13):
    d = "M" + " L".join(f"{x},{y}" for x, y in points)
    da = ' stroke-dasharray="6 4"' if dash else ""
    add(f'<path d="{d}" fill="none" stroke="#444" stroke-width="1.6" marker-end="url(#arrow)"{da}/>')
    if label:
        text(lx, ly, label, size=size, anchor=anchor, halo=True)


add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">')
add('<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" '
    'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#444"/></marker></defs>')
add(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')
text(20, 34, [("Transactional outbox: how an event gets from one bounded context to another", "b")], size=22, anchor="start")
add('<text x="20" y="66" font-size="15" dominant-baseline="middle" fill="#111">'
    '<tspan font-weight="bold">Problem:</tspan> saving to the database and sending to Kafka cannot happen in one step. '
    'If one fails, data and events disagree.</text>')
add('<text x="20" y="90" font-size="15" dominant-baseline="middle" fill="#111">'
    '<tspan font-weight="bold">Solution:</tspan> save the event in the same database transaction as the change, '
    'and let a separate Outbox-Service send it.</text>')

# legend (top right)
lx, ly = 1140, 22
add(f'<rect x="{lx}" y="{ly}" width="560" height="82" rx="8" fill="#ffffff" stroke="#bbbbbb"/>')
for j, (fill, stroke, w, label) in enumerate([
        ("#ff99ff", "#7a3d7a", 1.5, "You write this (your service code)"),
        ("#ffffff", "#7a3d7a", 2.2, "Provided library, used inside your service"),
        ("#eeeeee", "#888888", 1.5, "Other boxes: provided infrastructure (Outbox-Service, Kafka, PostgreSQL)")]):
    yy = ly + 14 + j * 23
    add(f'<rect x="{lx + 14}" y="{yy}" width="26" height="16" rx="3" fill="{fill}" stroke="{stroke}" stroke-width="{w}"/>')
    text(lx + 50, yy + 8, [label], size=13.5, anchor="start")

add('<g transform="translate(0,40)">')

# --- producing service ------------------------------------------------------------------
group(20, 90, 330, 470, "Producing service", "container · your bounded context", "#fde7fd", "#7a3d7a")
box(60, 145, 250, 44, ["REST API"], "#ff99ff", "#7a3d7a")
box(60, 225, 250, 56, ["Application service", ("one transaction per use case", "i")], "#ff99ff", "#7a3d7a")
box(175, 322, 135, 70, ["Aggregate", ("changes state,", "i"), ("records events", "i")], "#ff99ff", "#7a3d7a", size=13)
box(60, 450, 250, 70, [("outbox-lib-jpa", "b"), "WriteOutbox.write(events)"], "#ffffff", "#7a3d7a", width=2.2)
arrow([(185, 189), (185, 223)], "1. command", 195, 206, anchor="start")
arrow([(242, 281), (242, 320)], "2. change", 250, 301, anchor="start", size=12.5)
arrow([(100, 281), (100, 448)], ["3. pass on", "the events"], 108, 410, anchor="start", size=12.5)

# --- one transaction (shaded behind the two step-4 arrows) ------------------------------
add('<rect x="316" y="232" width="80" height="292" rx="6" fill="#fff4c2" fill-opacity="0.75" '
    'stroke="#b39b00" stroke-dasharray="5 3"/>')
text(356, 350, [("4. ONE", "b"), ("trans-", "b"), ("action", "b")], size=12)

# --- producer database ------------------------------------------------------------------
group(400, 180, 210, 320, "PostgreSQL", "the service's own schema", "#e9e9ff", "#4b4b9a")
cylinder(505, 280, 150, 62, ["Business tables", ("e.g. reservation", "i")])
cylinder(505, 410, 150, 62, [("event_outbox", "b")])
arrow([(310, 253), (340, 253), (340, 282), (428, 282)], None)
arrow([(310, 485), (340, 485), (340, 418), (428, 418)], None)
text(372, 268, ["save"], size=12.5, halo=True)
text(372, 502, ["insert"], size=12.5, halo=True)

# --- outbox service ---------------------------------------------------------------------
group(690, 330, 220, 170, "Outbox-Service", "own container · one per database", "#fffde9", "#9a8f3d")
box(705, 390, 190, 70, ["Reads new rows", ("again and again", "i")], "#fffbd6", "#9a8f3d")
arrow([(582, 402), (703, 402)], "5. read new rows", 642, 384, size=12.5)
arrow([(705, 445), (582, 426)], ["7. delete once", "Kafka confirmed"], 642, 470, size=12.5)

# --- kafka ------------------------------------------------------------------------------
group(960, 150, 240, 420, "Kafka", "same key → same partition → in order", "#ffeaea", "#9a3d3d")
box(980, 210, 200, 66, [("bl.<aggregate>.events.v1", "b"), ("one message per change:", "i"), ("what happened", "i")], "#ffc2c2", "#9a3d3d", size=13)
box(980, 310, 200, 66, [("db.<aggregate>.ecst.v1", "b"), ("the full current state", "i"), ("after each change", "i")], "#ffc2c2", "#9a3d3d", size=13)
box(980, 480, 200, 56, [("<topic>.DLT", "b"), ("messages that keep failing", "i")], "#fff5f5", "#9a3d3d", dash=True, size=13)
arrow([(895, 410), (935, 410), (935, 243), (978, 243)], None)
arrow([(935, 343), (978, 343)], None)
text(928, 280, ["6. send", ("key = aggregate ID", "i")], size=12.5, anchor="end", halo=True)

# --- consuming service ------------------------------------------------------------------
group(1320, 90, 380, 350, "Consuming service", "container · another bounded context", "#fde7fd", "#7a3d7a")
box(1360, 140, 300, 46, ["Kafka listener"], "#ff99ff", "#7a3d7a")
box(1360, 215, 300, 62, [("consumer-lib", "b"), ("calls the right handler for each event type", "i")],
    "#ffffff", "#7a3d7a", width=2.2, size=13)
box(1360, 306, 300, 108, [("Your handler", "b"), "a. already handled? → skip (sent twice)",
                          "b. older than what I have? → skip (late)", "c. update own data"], "#ff99ff", "#7a3d7a", size=13.5)
arrow([(1510, 186), (1510, 213)], None)
arrow([(1510, 277), (1510, 304)], None)
arrow([(1180, 243), (1250, 243), (1250, 158), (1358, 158)], None)
text(1215, 226, ["8. receive"], size=12.5, halo=True)
text(1215, 260, [("at least once", "i")], size=12, halo=True)
arrow([(1180, 343), (1272, 343), (1272, 172), (1358, 172)], None, dash=True)
text(1222, 368, [("alternative:", "i"), ("read the state", "i")], size=11.5, halo=True)
arrow([(1360, 398), (1296, 398), (1296, 508), (1182, 508)], None, dash=True)
text(1238, 532, [("still failing", "i"), ("after retries", "i")], size=12, halo=True)

group(1320, 475, 380, 115, "PostgreSQL", "the consumer's own schema", "#e9e9ff", "#4b4b9a")
cylinder(1605, 540, 160, 56, ["Own tables", ("+ handled event IDs", "i")])
arrow([(1605, 414), (1605, 501)], None)
text(1597, 459, ["9. save, then mark", "message as read"], size=12.5, anchor="end", halo=True)

add('</g>')

# --- notes: remember + terms ------------------------------------------------------------
add('<rect x="20" y="650" width="1680" height="195" rx="10" fill="#f7f7f7" stroke="#bbbbbb"/>')
text(40, 674, [("Remember", "b")], size=15.5, anchor="start")
remember = [
    ("Never send to Kafka from your service.", "Write events to the outbox in the same transaction (4)."),
    ("Every message can arrive twice.", "A crash between 6 and 7 sends it again: handle duplicates (a)."),
    ("Order holds per aggregate only.", "Same key, same partition. Late messages are skipped (b)."),
    ("C4 Level 2:", "the Outbox-Service is a container; the libraries are code inside containers (no boxes)."),
]
for k, (head_, body_) in enumerate(remember):
    y = 704 + k * 30
    add(f'<text x="40" y="{y}" font-size="14" dominant-baseline="middle" fill="#111">'
        f'<tspan font-weight="bold">{escape(head_)}</tspan> {escape(body_)}</text>')
add('<line x1="1010" y1="664" x2="1010" y2="830" stroke="#cccccc"/>')
text(1030, 674, [("Terms", "b")], size=15.5, anchor="start")
terms = [
    ("At least once:", "nothing is lost, but duplicates are possible."),
    ("ECST:", "event-carried state transfer, the full state as a message."),
    ("Revision:", "1, 2, 3 … per aggregate; tells old from new."),
    ("DLT:", "dead-letter topic, where failing messages are parked."),
    ("Usually:", "listen to the events OR the state, one listener each."),
]
for k, (head_, body_) in enumerate(terms):
    y = 704 + k * 30
    add(f'<text x="1030" y="{y}" font-size="14" dominant-baseline="middle" fill="#111">'
        f'<tspan font-weight="bold">{escape(head_)}</tspan> {escape(body_)}</text>')

add("</svg>")
(Path(__file__).resolve().parent.parent / "outbox-flow.svg").write_text("\n".join(out), encoding="utf-8")
print("ok")
