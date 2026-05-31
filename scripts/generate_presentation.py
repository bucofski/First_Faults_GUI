"""
Generate the First Faults GUI project presentation (.pptx).
Run from the project root: python scripts/generate_presentation.py
"""

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt
from PIL import Image
import os

# ---------------------------------------------------------------------------
# Colour palette (ArcelorMittal-inspired — steel blue + orange accent)
# ---------------------------------------------------------------------------
DARK_BG    = RGBColor(0x1A, 0x23, 0x3A)   # dark navy
ACCENT     = RGBColor(0xE8, 0x6B, 0x1A)   # steel orange
WHITE      = RGBColor(0xFF, 0xFF, 0xFF)
LIGHT_GREY = RGBColor(0xD0, 0xD8, 0xE8)
MID_GREY   = RGBColor(0x6B, 0x7A, 0x99)
GREEN_OK   = RGBColor(0x2E, 0xCC, 0x71)
YELLOW_WIP = RGBColor(0xF3, 0x9C, 0x12)

SLIDE_W = Inches(13.33)
SLIDE_H = Inches(7.5)

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR    = os.path.join(BASE_DIR, "docs")
LOGO_PATH   = os.path.join(DOCS_DIR, "AML.png")
LOGO_ASPECT = 110 / 57   # ArcelorMittal logo native pixel ratio (w / h)
OUTPUT_PATH = os.path.join(BASE_DIR, "presentations", "FirstFaults_Presentation.pptx")

os.makedirs(os.path.join(BASE_DIR, "presentations"), exist_ok=True)

prs = Presentation()
prs.slide_width  = SLIDE_W
prs.slide_height = SLIDE_H

BLANK = prs.slide_layouts[6]   # completely blank layout


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def add_rect(slide, l, t, w, h, fill_rgb, alpha=None):
    shape = slide.shapes.add_shape(1, Inches(l), Inches(t), Inches(w), Inches(h))
    shape.line.fill.background()
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_rgb
    return shape


def add_text(slide, text, l, t, w, h, font_size=18, bold=False, color=WHITE,
             align=PP_ALIGN.LEFT, italic=False, wrap=True):
    txb = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    txb.word_wrap = wrap
    tf = txb.text_frame
    tf.word_wrap = wrap
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size  = Pt(font_size)
    run.font.bold  = bold
    run.font.color.rgb = color
    run.font.italic = italic
    return txb


def add_para(tf, text, font_size=16, bold=False, color=WHITE,
             align=PP_ALIGN.LEFT, italic=False, space_before=6):
    p = tf.add_paragraph()
    p.alignment = align
    p.space_before = Pt(space_before)
    run = p.add_run()
    run.text = text
    run.font.size  = Pt(font_size)
    run.font.bold  = bold
    run.font.color.rgb = color
    run.font.italic = italic
    return p


def content_box(path):
    """Fractional (left, top, right, bottom) of the opaque/visible content
    within an image, ignoring transparent margins added by cropping.
    Falls back to the full frame for images without alpha."""
    im = Image.open(path)
    w, h = im.width, im.height
    bbox = None
    if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
        alpha = im.convert("RGBA").getchannel("A")
        bbox = alpha.point(lambda a: 255 if a > 10 else 0).getbbox()
    if bbox is None:
        bbox = (0, 0, w, h)
    l, t, r, b = bbox
    return l / w, t / h, r / w, b / h


def add_logo(slide, height, l, t):
    """Place the ArcelorMittal logo at (l, t) with the given height (inches)."""
    if os.path.exists(LOGO_PATH):
        slide.shapes.add_picture(LOGO_PATH, Inches(l), Inches(t),
                                 height=Inches(height),
                                 width=Inches(height * LOGO_ASPECT))


def slide_base(title_text, subtitle_text=""):
    """Dark background slide with header bar."""
    slide = prs.slides.add_slide(BLANK)

    # full background
    add_rect(slide, 0, 0, 13.33, 7.5, DARK_BG)

    # top accent bar
    add_rect(slide, 0, 0, 13.33, 1.0, ACCENT)

    # title in bar
    add_text(slide, title_text,
             l=0.35, t=0.08, w=10.5, h=0.85,
             font_size=28, bold=True, color=WHITE)

    # logo in the top-right of the header bar
    logo_h = 0.55
    add_logo(slide, logo_h, l=13.33 - logo_h * LOGO_ASPECT - 0.3, t=(1.0 - logo_h) / 2)

    # slide number indicator (small, top-right)
    # subtitle below bar
    if subtitle_text:
        add_text(slide, subtitle_text,
                 l=0.35, t=1.05, w=12.5, h=0.5,
                 font_size=15, italic=True, color=LIGHT_GREY)

    return slide


def bullet_box(slide, l, t, w, h):
    """Returns (shape, text_frame) for a bullet list box."""
    txb = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    txb.word_wrap = True
    tf = txb.text_frame
    tf.word_wrap = True
    # clear default empty paragraph
    tf.paragraphs[0].text = ""
    return txb, tf


def section_label(slide, text, l=0.35, t=1.65):
    add_rect(slide, l, t, 0.18, 0.38, ACCENT)
    add_text(slide, text, l=l + 0.25, t=t, w=12, h=0.4,
             font_size=18, bold=True, color=ACCENT)


# ===========================================================================
# Slide 1 — Title / Cover
# ===========================================================================
slide = prs.slides.add_slide(BLANK)
add_rect(slide, 0, 0, 13.33, 7.5, DARK_BG)
add_rect(slide, 0, 0, 13.33, 0.08, ACCENT)          # thin top line
add_rect(slide, 0, 7.42, 13.33, 0.08, ACCENT)       # thin bottom line
add_rect(slide, 0, 2.8, 13.33, 2.35, RGBColor(0x0D, 0x15, 0x26))  # dark band

# ArcelorMittal logo, centred at the bottom of the dark band
cover_logo_h = 0.9
add_logo(slide, cover_logo_h,
         l=(13.33 - cover_logo_h * LOGO_ASPECT) / 2, t=5.55)

add_text(slide, "FIRST FAULTS GUI",
         l=0.5, t=0.6, w=12.3, h=1.0,
         font_size=46, bold=True, color=ACCENT, align=PP_ALIGN.CENTER)

add_text(slide, "Historical Alarm Analysis & Reporting Platform",
         l=0.5, t=1.55, w=12.3, h=0.6,
         font_size=20, italic=True, color=LIGHT_GREY, align=PP_ALIGN.CENTER)

add_text(slide, "Benoit Goethals  ·  Tom Van de Vyver",
         l=0.5, t=2.95, w=12.3, h=0.55,
         font_size=22, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

add_text(slide, "ArcelorMittal  ·  2025 – 2026",
         l=0.5, t=3.5, w=12.3, h=0.45,
         font_size=16, color=MID_GREY, align=PP_ALIGN.CENTER)

add_text(slide, "10-minute project presentation",
         l=0.5, t=6.8, w=12.3, h=0.45,
         font_size=13, italic=True, color=MID_GREY, align=PP_ALIGN.CENTER)


# ===========================================================================
# Slide 2 — Agenda
# ===========================================================================
slide = slide_base("Agenda", "What we'll cover in 10 minutes")
_, tf = bullet_box(slide, 0.6, 1.65, 11.8, 5.3)

items = [
    ("01", "Project description & problem statement"),
    ("02", "Goals & success criteria"),
    ("03", "Team & responsibilities"),
    ("04", "Working style & methodology"),
    ("05", "Architecture"),
    ("06", "Key features walkthrough"),
    ("07", "Progress timeline"),
    ("08", "What's next"),
]
for num, label in items:
    p = tf.add_paragraph()
    p.space_before = Pt(4)
    r1 = p.add_run()
    r1.text = f"  {num}  "
    r1.font.size  = Pt(17)
    r1.font.bold  = True
    r1.font.color.rgb = ACCENT
    r2 = p.add_run()
    r2.text = label
    r2.font.size  = Pt(17)
    r2.font.color.rgb = WHITE


# ===========================================================================
# Slide 3 — Project Description
# ===========================================================================
slide = slide_base("Project Description", "What problem are we solving?")

add_rect(slide, 0.35, 1.65, 5.9, 5.35, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 6.55, 1.65, 6.43, 5.35, RGBColor(0x0D, 0x15, 0x26))

section_label(slide, "The Problem", l=0.35, t=1.65)
_, tf = bullet_box(slide, 0.55, 2.15, 5.6, 4.7)
for line in [
    "CIMPLICITY logs thousands of alarms per day on the ArcelorMittal production line",
    "Existing tools only show real-time alarms — no historical analysis",
    "Engineers cannot easily see which faults are trending upward",
    "Finding the root cause of interlock chains is manual and time-consuming",
    "No automated reporting or PDF exports for shift handover",
]:
    add_para(tf, f"›  {line}", font_size=14, color=LIGHT_GREY, space_before=8)

section_label(slide, "Our Solution", l=6.55, t=1.65)
_, tf = bullet_box(slide, 6.75, 2.15, 6.0, 4.7)
for line in [
    "Python / Flask web app served through IIS — no new login required",
    "Interactive dashboard: 6 charts + heatmap from historical SQL Server data",
    "Recursive interlock chain tracer: find the root fault in seconds",
    "Weekly fault snapshots for long-term trend & regression detection",
    "One-click PDF export for both the dashboard and the interlock tree",
    "Sub-second queries via optimised SQL Server TVFs and indexes",
]:
    add_para(tf, f"›  {line}", font_size=14, color=LIGHT_GREY, space_before=8)


# ===========================================================================
# Slide 3a — Reason Why (motivation)
# ===========================================================================
slide = slide_base("Why This Project?", "The reasons that triggered First Faults")

# Six reason cards in a 2-column × 3-row grid.
reasons = [
    ("Database hit its ceiling",
     "The existing database wasn't performant enough — asking for larger "
     "time ranges of data was slow or returned nothing at all."),
    ("No tool for technicians",
     "Maintenance engineers had no dedicated application — only the raw, "
     "real-time CIMPLICITY alarms with no history."),
    ("Fault finding was hard",
     "Tracing a stop back to its first fault through nested interlock "
     "chains was manual, slow and error-prone."),
    ("No line-fault analysis",
     "There was no way to analyse line faults over time — no trends, "
     "no top-risers, no week-over-week comparison."),
    ("No historical reporting",
     "Nothing supported shift handover or documentation — no PDF exports "
     "or stored snapshots to look back on."),
    ("Reactive, not proactive",
     "Without trend visibility, maintenance stayed reactive — climbing "
     "faults went unnoticed until they caused a breakdown."),
]

col_x  = [0.35, 6.75]
col_w  = [6.0, 6.23]
row_y  = [1.7, 3.43, 5.16]
card_h = 1.55
for idx, (title, desc) in enumerate(reasons):
    c = idx % 2
    r = idx // 2
    x = col_x[c]
    y = row_y[r]
    w = col_w[c]
    add_rect(slide, x, y, w, card_h, RGBColor(0x0D, 0x15, 0x26))
    add_rect(slide, x, y, 0.12, card_h, ACCENT)          # accent spine
    # number chip
    add_rect(slide, x + 0.28, y + 0.22, 0.5, 0.5, ACCENT)
    add_text(slide, str(idx + 1), l=x + 0.28, t=y + 0.26, w=0.5, h=0.44,
             font_size=18, bold=True, color=DARK_BG, align=PP_ALIGN.CENTER)
    add_text(slide, title, l=x + 0.95, t=y + 0.18, w=w - 1.1, h=0.45,
             font_size=15, bold=True, color=WHITE)
    add_text(slide, desc, l=x + 0.95, t=y + 0.62, w=w - 1.15, h=0.85,
             font_size=11, color=LIGHT_GREY)


# ===========================================================================
# Slide 3b — Basic Circuits (what & why)
# ===========================================================================
slide = slide_base("Basic Circuits", "Series enable-chains (vrijgaveketens) behind every drive")

add_rect(slide, 0.35, 1.65, 6.0, 4.0, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 6.65, 1.65, 6.33, 4.0, RGBColor(0x0D, 0x15, 0x26))

section_label(slide, "What is a basic circuit?", l=0.35, t=1.65)
_, tf = bullet_box(slide, 0.55, 2.15, 5.7, 3.4)
for line in [
    "A basic circuit (basisschakeling) is a SERIES chain of conditions — AND logic",
    "The drive is only enabled (Vrijgave) when EVERY condition in the chain is true",
    "One broken link breaks the whole chain → the machine stops",
    "Each link is itself a basic circuit, so chains nest many levels deep",
]:
    add_para(tf, f"›  {line}", font_size=14, color=LIGHT_GREY, space_before=10)

section_label(slide, "Why it matters for First Faults", l=6.65, t=1.65)
_, tf = bullet_box(slide, 6.85, 2.15, 6.0, 3.4)
for line in [
    "A single stop can hide behind dozens of nested conditions",
    "Operators must find which link failed first — the 'first fault'",
    "Tracing this by hand across PLCs is slow and error-prone",
    "First Faults reconstructs the chain automatically and points to the root cause",
]:
    add_para(tf, f"›  {line}", font_size=14, color=LIGHT_GREY, space_before=10)

# Drawn series-chain illustration along the bottom
add_text(slide, "AND-logic in series — one failed link (red) stops the whole drive",
         l=0.35, t=5.8, w=12.6, h=0.35, font_size=12, italic=True, color=MID_GREY)
chain = [
    ("No current reduction", GREEN_OK),
    ("Position OK", GREEN_OK),
    ("Drives released", ACCENT),   # first fault
    ("No e-stop", GREEN_OK),
]
node_w, node_h, gap = 2.15, 0.6, 0.42
x = 0.35
y = 6.25
for i, (label, col) in enumerate(chain):
    add_rect(slide, x, y, node_w, node_h, col)
    add_text(slide, label, l=x, t=y + 0.13, w=node_w, h=0.34,
             font_size=10, bold=True,
             color=DARK_BG if col != ACCENT else WHITE, align=PP_ALIGN.CENTER)
    # connector to next node
    add_rect(slide, x + node_w, y + node_h / 2 - 0.02, gap, 0.04, MID_GREY)
    x += node_w + gap
# final enable node
add_rect(slide, x, y, node_w, node_h, RGBColor(0x14, 0x5A, 0x8A))
add_text(slide, "VRIJGAVE", l=x, t=y + 0.13, w=node_w, h=0.34,
         font_size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)


# ===========================================================================
# Slide 3c — Operator overview / entry point (first fault on the big picture)
# ===========================================================================
slide = slide_base("How the Operator Sees It",
                   "The overview screen flags the first fault")

ov_img  = os.path.join(DOCS_DIR, "bs_operator_overview.png")
det_img = os.path.join(DOCS_DIR, "bs_drill_1_tandem.png")

if os.path.exists(ov_img):
    # overview matrix — aspect 993/899
    slide.shapes.add_picture(ov_img, Inches(0.35), Inches(1.7),
                             width=Inches(4.6), height=Inches(4.6 / (993 / 899)))
if os.path.exists(det_img):
    # first diagnostic window the operator opens — aspect 1323/814
    slide.shapes.add_picture(det_img, Inches(5.25), Inches(1.7),
                             width=Inches(7.55), height=Inches(7.55 / (1323 / 814)))

add_text(slide, "Overview — status per drive (WT1–WT5); the red marker flags the first fault",
         l=0.35, t=5.9, w=4.7, h=0.6, font_size=11, italic=True, color=LIGHT_GREY)
add_text(slide, "Clicking the fault opens its diagnostic window — the start of the chain",
         l=5.25, t=6.45, w=7.55, h=0.4, font_size=11, italic=True, color=LIGHT_GREY)

# Callout band
add_rect(slide, 0.35, 6.95, 4.7, 0.45, RGBColor(0x0D, 0x15, 0x26))
add_text(slide, "From the big picture, the operator drills into the first fault →",
         l=0.45, t=6.99, w=4.6, h=0.4, font_size=10, color=ACCENT)


# ===========================================================================
# Slide 3d — Following the fault chain (cascade drill-down)
# ===========================================================================
slide = slide_base("Following the Fault Chain",
                   "Drill down red bar → red bar, until the very first fault")

# Cascade of diagnostic windows — each red condition opens the next window.
# (file, aspect w/h, step title, BS number)
drill = [
    ("bs_drill_1_tandem.png",     1323 / 814, "Tandem stop request",  "BS 1208"),
    ("bs_drill_2_algemeen.png",   1328 / 823, "General interlocks",   "BS 1206"),
    ("bs_drill_3_trekopbouw.png",  971 / 619, "Enable Trek-opbouw",   "BS 11221"),
    ("bs_drill_4_wt3.png",         972 / 619, "Drive WT3",            "BS 21214"),
    ("bs_drill_5_lijndata.png",    969 / 615, "Line data (Lijndata)", "BS 20106"),
]

img_w  = 4.4
x0, y0 = 0.4, 1.45
dx, dy = 1.2, 0.72
for i, (fname, aspect, _t, _bs) in enumerate(drill):
    img = os.path.join(DOCS_DIR, fname)
    x = x0 + i * dx
    y = y0 + i * dy
    img_h = img_w / aspect
    # default badge anchor = image frame corner; refined to the real window
    # corner so it stays put even when the PNG has transparent crop margins
    bx, by = x, y
    if os.path.exists(img):
        slide.shapes.add_picture(img, Inches(x), Inches(y),
                                 width=Inches(img_w), height=Inches(img_h))
        lf, tf, _r, _b = content_box(img)
        bx = x + lf * img_w
        by = y + tf * img_h
    # step badge at the top-left corner of each window
    add_rect(slide, bx - 0.02, by - 0.02, 0.42, 0.42, ACCENT)
    add_text(slide, str(i + 1), l=bx - 0.02, t=by + 0.02, w=0.42, h=0.36,
             font_size=16, bold=True, color=DARK_BG, align=PP_ALIGN.CENTER)

# Right-side legend: the drill path, step by step
add_rect(slide, 10.1, 1.5, 2.9, 5.4, RGBColor(0x0D, 0x15, 0x26))
add_text(slide, "Drill path", l=10.25, t=1.6, w=2.6, h=0.4,
         font_size=15, bold=True, color=ACCENT)
ly = 2.05
for i, (_f, _a, title, bs) in enumerate(drill):
    add_rect(slide, 10.25, ly, 0.4, 0.4, ACCENT)
    add_text(slide, str(i + 1), l=10.25, t=ly + 0.02, w=0.4, h=0.34,
             font_size=13, bold=True, color=DARK_BG, align=PP_ALIGN.CENTER)
    add_text(slide, title, l=10.78, t=ly - 0.04, w=2.2, h=0.34,
             font_size=11, bold=True, color=WHITE)
    add_text(slide, bs, l=10.78, t=ly + 0.26, w=2.2, h=0.3,
             font_size=10, italic=True, color=LIGHT_GREY)
    if i < len(drill) - 1:
        add_text(slide, "↓", l=10.32, t=ly + 0.5, w=0.4, h=0.3,
                 font_size=13, bold=True, color=ACCENT)
    ly += 0.93

# Caption
add_text(slide, "Each red bar opens its own sub-circuit — the operator follows the red "
                "thread down to the root fault.",
         l=0.4, t=7.05, w=9.4, h=0.4, font_size=11, italic=True, color=LIGHT_GREY)


# ===========================================================================
# Slide 3d — How the programmer sees it
# ===========================================================================
slide = slide_base("How the PLC Programmer Sees It",
                   "The same chain as PLC function-block logic")

prog_img = os.path.join(DOCS_DIR, "bs_programmer_logic.png")
if os.path.exists(prog_img):
    slide.shapes.add_picture(prog_img, Inches(0.35), Inches(1.7),
                             width=Inches(9.45), height=Inches(5.05))
    add_text(slide, "PLC programming environment — PilootTD rijtoestanden-logica",
             l=0.35, t=6.78, w=9.45, h=0.35, font_size=11, italic=True, color=LIGHT_GREY)

add_rect(slide, 9.95, 1.7, 3.03, 5.05, RGBColor(0x0D, 0x15, 0x26))
add_text(slide, "From bars to blocks", l=10.1, t=1.8, w=2.8, h=0.4,
         font_size=15, bold=True, color=ACCENT)
_, tf = bullet_box(slide, 10.1, 2.35, 2.8, 4.3)
for line in [
    "Same enable-chain, now as function-block logic",
    "Each BS1 / BS_VW block = one basic circuit",
    "PermVw in → Vrijgave out, daisy-chained left → right",
    "The green/red bars the operator sees are these block states",
    "First Faults reads this structure straight from the PLC database",
]:
    add_para(tf, f"·  {line}", font_size=12, color=LIGHT_GREY, space_before=10)


# ===========================================================================
# Slide 4 — Goals & Success Criteria
# ===========================================================================
slide = slide_base("Goals & Success Criteria", "What does 'done' look like?")

goals = [
    (GREEN_OK,   "DONE",  "DB connection via SQLAlchemy + pyodbc (SQL Server)"),
    (GREEN_OK,   "DONE",  "Interlock chain tracing with recursive SQL function"),
    (GREEN_OK,   "DONE",  "Diagrams dashboard: faults/hour, MTBF, top risers, heatmap …"),
    (GREEN_OK,   "DONE",  "Collapsible interlock tree table with server-side filters"),
    (GREEN_OK,   "DONE",  "PDF export — dashboard (6 charts) and interlock tree"),
    (GREEN_OK,   "DONE",  "Weekly fault snapshots + historical reference-date picker"),
    (GREEN_OK,   "DONE",  "Query performance: from ~8 s to < 1 s"),
    (YELLOW_WIP, "WIP",   "Scheduled daily snapshot runs + automated weekly email report"),
    (YELLOW_WIP, "WIP",   "Final IIS deployment + user acceptance test (target: July 2026)"),
    (YELLOW_WIP, "LATER", "Role-based access (read-only vs. admin configuration)"),
    (YELLOW_WIP, "LATER", "Configurable alert thresholds with Teams/email notification"),
]

y = 1.65
for color, tag, text in goals:
    add_rect(slide, 0.35, y, 1.0, 0.36, color)
    add_text(slide, tag, l=0.35, t=y, w=1.0, h=0.36,
             font_size=11, bold=True, color=DARK_BG, align=PP_ALIGN.CENTER)
    add_text(slide, text, l=1.5, t=y + 0.02, w=11.4, h=0.34,
             font_size=13, color=WHITE if color == GREEN_OK else LIGHT_GREY)
    y += 0.44


# ===========================================================================
# Slide 5 — Team & Responsibilities
# ===========================================================================
slide = slide_base("Team & Responsibilities")

# Benoit card
add_rect(slide, 0.35, 1.65, 5.9, 5.2, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 0.35, 1.65, 5.9, 0.55, ACCENT)
add_text(slide, "Benoit Goethals", l=0.5, t=1.68, w=5.6, h=0.48,
         font_size=20, bold=True, color=WHITE)
_, tf = bullet_box(slide, 0.55, 2.35, 5.5, 4.3)
for item in [
    "Frontend — UI design, Jinja2 templates, Bootstrap 5 layout",
    "Visualisations — Plotly charts (bar, pie, heatmap)",
    "Flask routes, Blueprint structure, form handling",
    "PDF export — DiagramPdfService, Kaleido + ReportLab",
    "JavaScript: collapsible tree, async PDF download, global spinner",
    "Documentation: README, CHANGELOG, PROJECT_PROGRESS, architecture docs",
    "Performance: query rewrites and DB index tuning",
]:
    add_para(tf, f"·  {item}", font_size=13, color=LIGHT_GREY, space_before=7)

# Tom card
add_rect(slide, 6.55, 1.65, 6.43, 5.2, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 6.55, 1.65, 6.43, 0.55, RGBColor(0x14, 0x5A, 0x8A))
add_text(slide, "Tom Van de Vyver", l=6.7, t=1.68, w=6.1, h=0.48,
         font_size=20, bold=True, color=WHITE)
_, tf = bullet_box(slide, 6.75, 2.35, 6.0, 4.3)
for item in [
    "Backend — database design and normalisation",
    "SQL Server: TVFs, stored procedures, views, indexes",
    "SQLAlchemy ORM models and repository layer",
    "InterlockService: root-cause chain analysis engine",
    "Snapshot system: weekly fault snapshots + backfill scripts",
    "MailService: SMTP-based automated email reports",
    "DB migration scripts and validation tooling",
]:
    add_para(tf, f"·  {item}", font_size=13, color=LIGHT_GREY, space_before=7)


# ===========================================================================
# Slide 6 — Working Style
# ===========================================================================
slide = slide_base("Working Style & Methodology")

cols = [
    ("Git Flow",
     ["main  →  Develop  →  feature branches",
      "Pull requests for every feature (50+ PRs merged)",
      "Branch names follow GitHub issue numbers",
      "No direct commits to Develop"]),
    ("Agile Iterations",
     ["Short cycles: build → test → review → merge",
      "Priorities shift based on user feedback from engineers",
      "Weekly sync between Benoit and Tom",
      "Progress reported monthly to stakeholders"]),
    ("Quality Practices",
     ["SQLAlchemy 2.0 compliance enforced throughout",
      "SOLID principles applied to service layer",
      "Logging added at service boundaries for traceability",
      "SQL scripts tested in isolation before integration"]),
    ("Tooling",
     ["PyCharm (IDE)  ·  GitHub (versioning & PRs)",
      "Flask dev server for local testing",
      "IIS as production run environment",
      "Plotly + Kaleido for chart-to-PNG rendering"]),
]

x_positions = [0.35, 3.5, 6.65, 9.8]
for i, (title, bullets) in enumerate(cols):
    x = x_positions[i]
    add_rect(slide, x, 1.65, 3.1, 5.35, RGBColor(0x0D, 0x15, 0x26))
    add_rect(slide, x, 1.65, 3.1, 0.5, ACCENT if i % 2 == 0 else RGBColor(0x14, 0x5A, 0x8A))
    add_text(slide, title, l=x + 0.1, t=1.68, w=2.9, h=0.45,
             font_size=15, bold=True, color=WHITE)
    _, tf = bullet_box(slide, x + 0.1, 2.28, 2.9, 4.5)
    for b in bullets:
        add_para(tf, f"·  {b}", font_size=12, color=LIGHT_GREY, space_before=9)


# ===========================================================================
# Slide 7 — Architecture
# ===========================================================================
slide = slide_base("Architecture", "Three-tier Flask application on IIS")

arch_img = os.path.join(DOCS_DIR, "HighLevelArchitectureDiagram.png")
if os.path.exists(arch_img):
    slide.shapes.add_picture(arch_img, Inches(0.35), Inches(1.65),
                             width=Inches(7.5), height=Inches(5.3))

# legend / summary on right
add_rect(slide, 8.05, 1.65, 5.0, 5.3, RGBColor(0x0D, 0x15, 0x26))
_, tf = bullet_box(slide, 8.2, 1.75, 4.7, 5.0)

layers = [
    ("Presentation layer", "Flask Blueprints · Jinja2 templates · Bootstrap 5"),
    ("Business layer",     "InterlockService · FaultCountService"),
    ("Data layer",         "SQLAlchemy ORM · SnapshotRepository · DB_Connection"),
    ("Database",           "SQL Server via pyodbc · TVFs · Views · Indexes"),
    ("Export",             "ReportLab PDF · Plotly + Kaleido PNG"),
    ("Auth",               "IIS Windows Authentication — no custom login"),
    ("Config",             "TOML config file loaded at startup"),
]
for layer, detail in layers:
    p = tf.add_paragraph()
    p.space_before = Pt(8)
    r1 = p.add_run()
    r1.text = f"{layer}\n"
    r1.font.size  = Pt(13)
    r1.font.bold  = True
    r1.font.color.rgb = ACCENT
    r2 = p.add_run()
    r2.text = detail
    r2.font.size  = Pt(11)
    r2.font.color.rgb = LIGHT_GREY

add_text(slide, "SQL Server  →  Repository  →  Service  →  Template  →  Browser",
         l=8.2, t=6.6, w=4.7, h=0.35,
         font_size=10, italic=True, color=MID_GREY)


# ===========================================================================
# Slide 8 — Key Features
# ===========================================================================
slide = slide_base("Key Features", "What the application delivers")

features = [
    ("Diagrams Dashboard",
     "/plc/diagrams",
     ["Faults per hour (bar chart)",
      "Faults per PLC — pie chart",
      "Top risers (week-over-week climbing faults)",
      "MTBF per PLC",
      "Top 10 climbing faults",
      "Repeat offenders (max occurrences per hour)",
      "Heatmap: hour × day per selected PLC",
      "Historical week/month picker via reference date"]),
    ("Interlock Tree",
     "/plc/table-tree",
     ["Recursive fault-chain tracer in SQL",
      "Filters: Target BSID, Top N, PLC, time range, condition mnemonic",
      "Collapsible tree rows — expand/collapse per node",
      "POST-redirect-GET pattern prevents double-submit",
      "Server-side validation with flash messages"]),
    ("PDF Exports",
     "/plc/diagrams-pdf  &  table-tree PDF",
     ["All 6 dashboard charts → PNG (Kaleido) → landscape PDF (ReportLab)",
      "Interlock tree exported as formatted PDF table",
      "Async download via fetch — no page reload",
      "Global loading spinner shown during generation"]),
    ("Snapshot & Reporting",
     "run_daily_snapshot.py",
     ["Weekly fault count snapshots stored in DB",
      "Backfill support across multiple production databases",
      "Long-term regression view: compare trends over months",
      "MailService ready for automated weekly email delivery"]),
]

x_pos = [0.35, 3.5, 6.65, 9.8]
for i, (title, route, bullets) in enumerate(features):
    x = x_pos[i]
    add_rect(slide, x, 1.65, 3.1, 5.35, RGBColor(0x0D, 0x15, 0x26))
    add_rect(slide, x, 1.65, 3.1, 0.45, ACCENT if i % 2 == 0 else RGBColor(0x14, 0x5A, 0x8A))
    add_text(slide, title, l=x + 0.1, t=1.67, w=2.9, h=0.4,
             font_size=14, bold=True, color=WHITE)
    add_text(slide, route, l=x + 0.1, t=2.15, w=2.9, h=0.3,
             font_size=10, italic=True, color=ACCENT)
    _, tf = bullet_box(slide, x + 0.1, 2.5, 2.9, 4.3)
    for b in bullets:
        add_para(tf, f"·  {b}", font_size=11, color=LIGHT_GREY, space_before=7)


# ===========================================================================
# Slide 9 — Progress Timeline
# ===========================================================================
slide = slide_base("Progress Timeline", "November 2025 → May 2026")

phases = [
    ("Nov 2025",      ACCENT,                    "Phase 1 — Setup & PoC",
     "Flask app · Blueprint routing · Plotly charts · Tree table · IIS auth"),
    ("Nov–Dec 2025",  RGBColor(0x14, 0x5A, 0x8A), "Phase 2 — Interlock Engine",
     "SQLAlchemy ORM · Recursive SQL chain tracer · PDF export · Snapshots · MailService"),
    ("Feb 2026",      RGBColor(0x27, 0x6E, 0x48), "Phase 3 — Stability",
     "InterlockService rename · SQL views · Migration validation tooling"),
    ("Apr 2026",      RGBColor(0x7D, 0x3C, 0x98), "Phase 4 — Dashboard & Performance",
     "6-chart dashboard · DiagramPdfService · Reference-date picker · 8 s → < 1 s query time · DB indexes"),
    ("May 2026",      MID_GREY,                  "Now — Polish",
     "Pie chart on home · Subtree toggle refactor · PDF null-fix · Documentation"),
]

y = 1.65
for date, color, title, detail in phases:
    add_rect(slide, 0.35, y, 1.6, 0.88, color)
    add_text(slide, date, l=0.35, t=y + 0.22, w=1.6, h=0.44,
             font_size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    add_rect(slide, 2.1, y + 0.34, 10.88, 0.04, color)  # connector line
    add_rect(slide, 2.1, y, 10.88, 0.88, RGBColor(0x0D, 0x15, 0x26))
    add_text(slide, title, l=2.25, t=y + 0.02, w=10.5, h=0.4,
             font_size=14, bold=True, color=color)
    add_text(slide, detail, l=2.25, t=y + 0.42, w=10.5, h=0.42,
             font_size=12, color=LIGHT_GREY)
    y += 1.02


# ===========================================================================
# Slide 10 — What's Next & Closing
# ===========================================================================
slide = slide_base("What's Next", "Road to MVP — 11 June 2026")

add_rect(slide, 0.35, 1.65, 6.3, 4.0, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 0.35, 1.65, 6.3, 0.45, ACCENT)
add_text(slide, "Before MVP (June 11)", l=0.5, t=1.67, w=6.0, h=0.4,
         font_size=15, bold=True, color=WHITE)
_, tf = bullet_box(slide, 0.55, 2.2, 5.9, 3.3)
for item in [
    "Schedule daily snapshot runs (Windows Task Scheduler / IIS)",
    "Wire MailService to automated weekly PDF email",
    "Final IIS deployment on production server",
    "User acceptance test with ArcelorMittal engineers",
    "About & Contact pages with real contact details",
    "End-to-end test pass: all pages, filters, PDF downloads",
]:
    add_para(tf, f"›  {item}", font_size=13, color=LIGHT_GREY, space_before=7)

add_rect(slide, 6.85, 1.65, 6.13, 4.0, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 6.85, 1.65, 6.13, 0.45, RGBColor(0x14, 0x5A, 0x8A))
add_text(slide, "Post-MVP Ambitions", l=7.0, t=1.67, w=5.8, h=0.4,
         font_size=15, bold=True, color=WHITE)
_, tf = bullet_box(slide, 7.05, 2.2, 5.7, 3.3)
for item in [
    "Role-based access: read-only vs. admin",
    "Configurable alert thresholds with email/Teams notifications",
    "Extended heatmap: drill-down to individual fault codes",
    "Promote ML fault prediction to production",
    "Multi-site support (multiple CIMPLICITY instances)",
    "REST API so other tools can query fault trends",
]:
    add_para(tf, f"›  {item}", font_size=13, color=LIGHT_GREY, space_before=7)

# Closing banner
add_rect(slide, 0.35, 5.85, 12.63, 1.3, RGBColor(0x0D, 0x15, 0x26))
add_rect(slide, 0.35, 5.85, 12.63, 0.06, ACCENT)
add_text(slide, "Thank you for your attention — questions welcome",
         l=0.5, t=6.1, w=12.3, h=0.6,
         font_size=22, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
add_text(slide, "Benoit Goethals · Tom Van de Vyver  |  ArcelorMittal  ·  2025–2026",
         l=0.5, t=6.7, w=12.3, h=0.35,
         font_size=13, italic=True, color=MID_GREY, align=PP_ALIGN.CENTER)


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
prs.save(OUTPUT_PATH)
print(f"Saved: {OUTPUT_PATH}")
