"""Phase 7 — generate the executive capstone deck.

Builds outputs/Delta_Reservations_Capstone.pptx with python-pptx so the deck is reproducible:
every number on a slide is read from the pipeline's outputs (Phases 2-6) and the charts are the
PNGs in outputs/figures/. Re-run the pipeline, then this script, and the deck updates itself.

Design rules (spec Phase 7): ~8 slides, one idea per slide, big numbers, recommendation BEFORE
methodology. Delta-ish styling (navy + red accent) — a student project, not official branding.
Speaker notes on each slide carry the talking points.

Run:  python -m src.build_deck
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.oxml.ns import qn
from pptx.oxml.xmlchemy import OxmlElement
from pptx.util import Emu, Inches, Pt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.erlang import apply_shrinkage  # noqa: E402
from src.forecast import FORECAST_CSV, METRICS_JSON  # noqa: E402
from src.optimize import STAFFING_CSV, SUMMARY_JSON  # noqa: E402
from src.simulate import VALIDATION_CSV, N_CONTACTS  # noqa: E402
from src.dashboard import DAILY_VOLUME_CSV, REASON_MIX_CSV  # noqa: E402

DECK_PATH = PROJECT_ROOT / "outputs" / "Delta_Reservations_Capstone.pptx"
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"
AUTHOR = "Frank An"
TABLEAU_URL = ("https://public.tableau.com/app/profile/frank.an5089/viz/"
               "DeltaATLReservationsStaffingOptimizer/Dashboard")

# Palette: navy dominates, red is the single sharp accent (matches the matplotlib figures).
NAVY = RGBColor(0x1B, 0x2A, 0x4A)
RED = RGBColor(0xC8, 0x10, 0x2E)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
MUTED = RGBColor(0x5B, 0x64, 0x75)      # captions on white
TINT = RGBColor(0xF1, 0xF3, 0xF7)       # card background on white
ICE = RGBColor(0xC9, 0xD3, 0xE6)        # secondary text on navy
NAVY_2 = RGBColor(0x2A, 0x3D, 0x66)     # card background on navy
FONT = "Arial"                          # ships with Office and macOS -> renders true-to-width

SLIDE_W, SLIDE_H = 13.333, 7.5          # 16:9, inches
MARGIN = 0.6


# --------------------------------------------------------------------------------------
# Numbers — all read from pipeline outputs
# --------------------------------------------------------------------------------------
def load_metrics() -> dict:
    fc = json.loads(METRICS_JSON.read_text())
    opt = json.loads(SUMMARY_JSON.read_text())
    cost = opt["cost"]
    val = pd.read_csv(VALIDATION_CSV)
    plan = pd.read_csv(STAFFING_CSV, parse_dates=["date"])
    forecast = pd.read_csv(FORECAST_CSV, parse_dates=["date"])
    daily = pd.read_csv(DAILY_VOLUME_CSV, parse_dates=["date"])
    reasons = pd.read_csv(REASON_MIX_CSV)

    hist = daily.dropna(subset=["actual"])
    fut = forecast[forecast["segment"] == "future"]
    # Same calendar window one year earlier, for the "January runs high" caveat.
    year = pd.DateOffset(years=1)
    prior = hist[(hist["date"] >= fut["date"].min() - year) & (hist["date"] <= fut["date"].max() - year)]

    normal_sched = apply_shrinkage(opt["normal_peak"], config.SHRINKAGE)
    irop_sched = apply_shrinkage(opt["irop_peak"], config.SHRINKAGE)
    return {
        "n_days": opt["n_days"], "block_hours": cost["block_hours"],
        "start": plan["date"].min(), "end": plan["date"].max(),
        # money
        "saved_real": cost["realistic_saved_dollars"], "pct_real": cost["realistic_pct_reduction"],
        "saved_naive": cost["saved_dollars"], "pct_naive": cost["pct_reduction"],
        "peak_flat": cost["peak_scheduled"],
        "defl_saved": opt["def_saved_dollars"], "defl_volume_cut": 1 - opt["deflection_factor"],
        # service (volume-weighted, same definitions as the dashboards)
        "sl": float(np.average(plan["predicted_sl"], weights=plan["volume"])),
        "asa": float(np.average(plan["predicted_asa_seconds"], weights=plan["volume"])),
        "occ": float(plan["offered_load_erlangs"].sum() / plan["required_agents"].sum()),
        "min_sl": opt["opt_min_sl"],
        # evidence
        "mape_normal": fc["mape_normal"], "mape_all": fc["mape_all"],
        "sim_gap_pts": float(val["sl_diff"].max() * 100), "n_scenarios": len(val),
        "sim_reps": int(val["reps"].iloc[0]),
        # IROP stress
        "normal_sched": normal_sched, "irop_sched": irop_sched,
        "irop_hours_x": opt["irop_day_hours"] / opt["normal_day_hours"], "aht_irop": opt["aht_irop"],
        # demand context + caveat
        "typical_day": float(hist["actual"].median()), "peak_day": float(hist["actual"].max()),
        "total_contacts": int(reasons["contacts"].sum()),
        "fut_mean": float(fut["forecast"].mean()), "prior_mean": float(prior["actual"].mean()),
    }


def usd(x: float) -> str:
    return f"${x:,.0f}"


def usd_k(x: float) -> str:
    return f"${x / 1000:,.0f}K"


def day(d: pd.Timestamp) -> str:
    return f"{d.strftime('%b')} {d.day}"          # "Jan 1" (portable: no %-d)


# --------------------------------------------------------------------------------------
# Drawing helpers (inches in, python-pptx units out)
# --------------------------------------------------------------------------------------
def _run(paragraph, text, size, color, bold=False, italic=False):
    r = paragraph.add_run()
    r.text = text
    f = r.font
    f.name, f.size, f.bold, f.italic = FONT, Pt(size), bold, italic
    f.color.rgb = color
    return r


def add_text(slide, x, y, w, h, text, size=16, color=NAVY, bold=False, italic=False,
             align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP):
    """A zero-padding text box; '\\n' in `text` starts a new paragraph with the same style."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = anchor
    for i, line in enumerate(text.split("\n")):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        _run(p, line, size, color, bold, italic)
    return tb


def add_box(slide, x, y, w, h, fill, shape=MSO_SHAPE.ROUNDED_RECTANGLE):
    s = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    s.fill.solid()
    s.fill.fore_color.rgb = fill
    s.line.fill.background()
    s.shadow.inherit = False                     # no theme drop shadow
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        s.adjustments[0] = 0.06
    return s


def add_badge(slide, x, y, d, label, fill=NAVY, size=16):
    """Numbered circle — the deck's recurring motif."""
    s = add_box(slide, x, y, d, d, fill, shape=MSO_SHAPE.OVAL)
    tf = s.text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    _run(p, label, size, WHITE, bold=True)
    return s


def add_picture(slide, path, x, y, w):
    """Place a PNG at width `w` (inches), keeping its aspect ratio; returns its height."""
    iw, ih = Image.open(path).size
    h = w * ih / iw
    slide.shapes.add_picture(str(path), Inches(x), Inches(y), Inches(w), Inches(h))
    return h


def add_stat(slide, x, y, w, number, label, num_size=40, num_color=NAVY, label_color=NAVY,
             label_size=15, num_h=0.75, label_h=0.6):
    add_text(slide, x, y, w, num_h, number, num_size, num_color, bold=True, anchor=MSO_ANCHOR.BOTTOM)
    add_text(slide, x, y + num_h + 0.05, w, label_h, label, label_size, label_color)


def new_slide(prs, bg=WHITE):
    s = prs.slides.add_slide(prs.slide_layouts[6])   # blank layout
    s.background.fill.solid()
    s.background.fill.fore_color.rgb = bg
    return s


def add_title(slide, text, color=NAVY):
    add_text(slide, MARGIN, 0.45, SLIDE_W - 2 * MARGIN, 1.1, text, 30, color, bold=True)


def add_footer(slide, number):
    add_text(slide, MARGIN, 7.0, 8, 0.25, "Delta ATL Reservations capstone  ·  synthetic data",
             10, MUTED)
    add_text(slide, SLIDE_W - MARGIN - 1, 7.0, 1, 0.25, str(number), 10, MUTED, align=PP_ALIGN.RIGHT)


def add_notes(slide, text):
    slide.notes_slide.notes_text_frame.text = text


def add_card(slide, x, y, w, h, header, body, header_color=NAVY, fill=TINT, body_size=15):
    add_box(slide, x, y, w, h, fill)
    add_text(slide, x + 0.3, y + 0.25, w - 0.6, 0.4, header, 19, header_color, bold=True)
    add_text(slide, x + 0.3, y + 0.75, w - 0.6, h - 0.95, body, body_size, NAVY)


# --------------------------------------------------------------------------------------
# Slides
# --------------------------------------------------------------------------------------
def slide_title(prs, m):
    s = new_slide(prs, NAVY)
    add_text(s, 0.8, 0.9, 11.7, 0.4, "DELTA RESERVATIONS  ·  CAPSTONE", 14, ICE, bold=True)
    add_text(s, 0.8, 1.45, 11.7, 1.8, "Optimizing ATL Reservations Staffing\nfor Service & Cost",
             40, WHITE, bold=True)
    add_text(s, 0.8, 3.35, 11.7, 0.5,
             "A forecast-driven, simulation-validated staffing plan for Delta's largest hub", 20, ICE)
    chips = [
        (usd_k(m["saved_real"]), f"saved in {m['n_days']} days vs fixed {m['block_hours']}-hour shifts"),
        (f"{m['sl']:.1%}", f"of calls answered within {config.TARGET_ANSWER_SECONDS} s "
                           f"(target {config.TARGET_SL:.0%})"),
        (f"{m['sim_gap_pts']:.1f} pts", "largest gap between simulation and the Erlang C math"),
    ]
    for i, (num, label) in enumerate(chips):
        x = 0.8 + i * (3.75 + 0.3)
        add_box(s, x, 4.35, 3.75, 1.55, NAVY_2)
        add_text(s, x + 0.3, 4.5, 3.15, 0.65, num, 34, WHITE, bold=True)
        add_text(s, x + 0.3, 5.2, 3.15, 0.6, label, 14, ICE)
    add_text(s, 0.8, 6.65, 11.7, 0.3,
             f"{AUTHOR}  ·  Synthetic data — a student project, not official Delta material", 12, ICE)
    add_notes(s, f"Open with the answer: staffing each half-hour to the forecast saves about "
                 f"{usd_k(m['saved_real'])} over {m['n_days']} days versus fixed "
                 f"{m['block_hours']}-hour shifts, while {m['sl']:.1%} of calls are still answered "
                 f"within {config.TARGET_ANSWER_SECONDS} seconds — and a simulation independently "
                 f"confirms the staffing math. All data is synthetic; real contact logs are confidential.")


def slide_problem(prs, m):
    s = new_slide(prs)
    add_title(s, "Demand swings hard — flat staffing either strands customers or pays for idle time")
    add_card(s, MARGIN, 1.95, 4.6, 2.05, "Understaffed",
             "Long holds and abandoned calls — worst during weather disruptions, exactly when "
             "customers need Delta most.", header_color=RED)
    add_card(s, MARGIN, 4.3, 4.6, 2.05, "Overstaffed",
             "Paying agents to sit idle. Sizing every hour to the daily peak wastes most of the "
             "day's paid time.")
    x = 5.6
    add_picture(s, FIG_DIR / "volume_overview.png", x, 1.95, SLIDE_W - MARGIN - x)
    stats = [(f"{m['typical_day']:,.0f}", "contacts on a typical day"),
             (f"{m['peak_day']:,.0f}", "on the busiest day of the year"),
             (f"{m['peak_day'] / m['typical_day']:.1f}×", "busiest vs typical day")]
    for i, (num, label) in enumerate(stats):
        add_stat(s, x + i * 2.45, 4.65, 2.2, num, label, num_size=28, num_h=0.55, label_size=13)
    add_text(s, x, 6.1, SLIDE_W - MARGIN - x, 0.5,
             "Daily contacts, 2025 (synthetic). Red dots = IROP (irregular operations) days; "
             "gold diamonds = holiday-travel peaks.", 11, MUTED)
    add_footer(s, 2)
    add_notes(s, "ATL is Delta's largest hub. Contact volume follows weekly and seasonal rhythms, but "
                 "weather disruptions (IROPs) spike it far above a normal day. Staffing flat either "
                 "leaves customers waiting when volume spikes or pays idle agents when it's quiet — "
                 "the plan has to follow demand half-hour by half-hour.")


def slide_recommendation(prs, m):
    s = new_slide(prs)
    add_title(s, f"Recommendation: staff each half-hour to the forecast — save "
                 f"{usd_k(m['saved_real'])} in {m['n_days']} days and still beat the service target")
    add_stat(s, MARGIN, 1.85, 4.4, usd(m["saved_real"]),
             f"saved over {m['n_days']} days vs fixed {m['block_hours']}-hour shifts "
             f"({m['pct_real']:.0%} fewer agent-hours)", num_size=48, num_color=RED, num_h=0.85)
    add_stat(s, MARGIN, 3.45, 4.4, f"{m['sl']:.1%}",
             f"of calls answered within {config.TARGET_ANSWER_SECONDS} s — target "
             f"{config.TARGET_SL:.0%}, met in every half-hour")
    add_stat(s, MARGIN, 4.95, 4.4, f"{m['asa']:.1f} s",
             f"average wait to answer; agents {m['occ']:.0%} busy (cap "
             f"{config.MAX_OCCUPANCY:.0%})")
    x, w = 5.33, SLIDE_W - MARGIN - 5.33
    h = add_picture(s, FIG_DIR / "savings.png", x, 1.85, w)
    add_text(s, x, 1.85 + h + 0.15, w, 0.5,
             f"Same forecast, four ways to staff it. Naive = peak headcount ({m['peak_flat']} agents) "
             f"around the clock; {m['block_hours']}h shifts = each shift staffed to its busiest "
             f"half-hour.", 11, MUTED)
    add_footer(s, 3)
    add_notes(s, f"Lead with the answer. Compared with a center running fixed {m['block_hours']}-hour "
                 f"shifts, each staffed to its busiest half-hour, interval-level Erlang C staffing cuts "
                 f"agent-hours by {m['pct_real']:.0%} — {usd(m['saved_real'])} over {m['n_days']} days "
                 f"at ${config.AGENT_HOURLY_COST:.0f}/hour — while {m['sl']:.1%} of calls are answered "
                 f"within {config.TARGET_ANSWER_SECONDS} seconds. Against naive peak staffing 24/7 the "
                 f"saving would be {m['pct_naive']:.0%} ({usd(m['saved_naive'])}), but no real center "
                 f"runs that way, so the realistic comparison is the headline.")


def slide_approach(prs, m):
    s = new_slide(prs)
    add_title(s, "How: four steps, each checked before the next")
    steps = [
        ("Forecast", "Prophet learns weekly and holiday patterns from a year of contacts; an "
                     "intraday profile splits each day into 48 half-hours.",
         f"{m['mape_normal']:.1f}% error on normal days"),
        ("Size", "Erlang C, the standard call-center formula, finds the fewest agents that answer "
                 f"{config.TARGET_SL:.0%} of calls in {config.TARGET_ANSWER_SECONDS} s; "
                 f"+{config.SHRINKAGE:.0%} covers breaks and training.",
         "5 Erlangs → 8 agents → 12 scheduled"),
        ("Validate", "A discrete-event simulation replays hundreds of thousands of calls and "
                     "measures the waits directly — no formula involved.",
         f"within {m['sim_gap_pts']:.1f} pts of Erlang C"),
        ("Optimize & price", f"Agent-hours × ${config.AGENT_HOURLY_COST:.0f}/h versus naive and "
                             "fixed-shift baselines, plus IROP and self-service scenarios.",
         f"−{m['pct_real']:.0%} vs {m['block_hours']}-hour shifts"),
    ]
    w, gap, y, h = 2.73, 0.4, 1.95, 4.1
    for i, (head, body, metric) in enumerate(steps):
        x = MARGIN + i * (w + gap)
        add_box(s, x, y, w, h, TINT)
        add_badge(s, x + 0.25, y + 0.3, 0.6, str(i + 1), size=18)
        add_text(s, x + 0.25, y + 1.1, w - 0.5, 0.45, head, 20, NAVY, bold=True)
        add_text(s, x + 0.25, y + 1.6, w - 0.5, 1.75, body, 14, NAVY)
        add_text(s, x + 0.25, y + 3.35, w - 0.5, 0.6, metric, 15, RED, bold=True,
                 anchor=MSO_ANCHOR.BOTTOM)
        if i < len(steps) - 1:
            add_box(s, x + w + 0.1, y + 0.45, 0.2, 0.3, ICE, shape=MSO_SHAPE.CHEVRON)
    add_text(s, MARGIN, 6.3, SLIDE_W - 2 * MARGIN, 0.4,
             "Validated two independent ways: the forecast is scored on held-out data, and the "
             "staffing math is confirmed by simulation.", 15, MUTED, italic=True)
    add_footer(s, 4)
    add_notes(s, "Four steps. (1) Forecast daily volume and split it into half-hours. (2) Erlang C — "
                 "the queueing formula real contact centers use — turns volume and handle time into the "
                 "minimum agents for 80/20, then shrinkage grosses that up to scheduled headcount. "
                 "(3) A simulation imitates calls and agents one event at a time and measures service "
                 "level independently. (4) Convert the curve to agent-hours and dollars against two "
                 "baselines.")


def slide_evidence(prs, m):
    s = new_slide(prs)
    add_title(s, "Evidence: the forecast tracks reality, and simulation confirms the staffing math")
    lx, lw = MARGIN, 6.55
    rx = 7.55
    rw = SLIDE_W - MARGIN - rx
    add_text(s, lx, 1.8, 1.9, 0.7, f"{m['mape_normal']:.1f}%", 36, NAVY, bold=True,
             anchor=MSO_ANCHOR.MIDDLE)
    add_text(s, lx + 2.0, 1.8, lw - 2.0, 0.7, "average error on normal days (held-out December; "
             "target under 10%)", 14, NAVY, anchor=MSO_ANCHOR.MIDDLE)
    add_picture(s, FIG_DIR / "forecast.png", lx, 2.75, lw)
    add_text(s, rx, 1.8, 1.9, 0.7, f"{m['sim_gap_pts']:.1f} pts", 36, NAVY, bold=True,
             anchor=MSO_ANCHOR.MIDDLE)
    add_text(s, rx + 2.0, 1.8, rw - 2.0, 0.7, "largest service-level gap vs Erlang C across "
             f"{m['n_scenarios']} load levels (tolerance 3 pts)", 14, NAVY, anchor=MSO_ANCHOR.MIDDLE)
    add_picture(s, FIG_DIR / "validation.png", rx, 2.75, rw)
    add_text(s, lx, 5.85, lw, 0.8,
             f"All test days: {m['mape_all']:.1f}% — the Christmas spike can't be learned from one "
             "year of history. Weather (IROP) days are left out of training and handled as a "
             "stress scenario instead.", 12, MUTED)
    add_text(s, rx, 5.85, rw, 0.8,
             f"SimPy M/M/c simulation: {m['sim_reps']} replications × {N_CONTACTS:,} calls per "
             "load level, compared with the Erlang C prediction.", 12, MUTED)
    add_footer(s, 5)
    add_notes(s, f"Two independent checks. The forecast is scored on a held-out month: "
                 f"{m['mape_normal']:.1f}% average error on normal days, under the 10% target; the "
                 f"all-days figure ({m['mape_all']:.1f}%) includes Christmas, which one year of history "
                 f"can't teach. The simulation imitates the queue call by call; across "
                 f"{m['n_scenarios']} load levels its service level lands within "
                 f"{m['sim_gap_pts']:.1f} points of the Erlang C prediction.")


def slide_irop(prs, m):
    s = new_slide(prs)
    ratio = m["irop_sched"] / m["normal_sched"]
    add_title(s, f"Disruption days need ~{ratio:.0f}× the agents — plan flex capacity before the storm")
    lw = 7.3
    h = add_picture(s, FIG_DIR / "staffing_curve.png", MARGIN, 1.95, lw)
    add_text(s, MARGIN, 1.95 + h + 0.15, lw, 0.6,
             f"Average forecast day (navy) vs an IROP day at {config.IROP_VOLUME_MULTIPLIER:g}× volume "
             "with longer rebooking calls (gold). Dashed red = naive flat staffing.", 12, MUTED)
    x = 8.3
    w = SLIDE_W - MARGIN - x
    add_stat(s, x, 1.85, w, f"{m['normal_sched']} → {m['irop_sched']}",
             f"scheduled agents at the afternoon peak; IROP-day agent-hours "
             f"×{m['irop_hours_x']:.1f}", num_color=RED, label_size=14, label_h=0.7)
    recs = [
        ("Flex capacity", "An on-call pool of cross-trained agents, activated when a weather "
                          "disruption is forecast."),
        ("Self-service deflection",
         f"Move {config.DEFLECTION_RATE:.0%} of app-friendly contacts (bookings, seats, SkyMiles) to "
         f"the Fly Delta app: −{m['defl_volume_cut']:.0%} volume, ~{usd_k(m['defl_saved'])} per "
         f"{m['n_days']} days."),
    ]
    for i, (head, body) in enumerate(recs):
        y = 3.7 + i * 1.45
        add_badge(s, x, y, 0.5, str(i + 1), size=15)
        add_text(s, x + 0.65, y + 0.05, w - 0.65, 0.4, head, 17, NAVY, bold=True)
        add_text(s, x + 0.65, y + 0.47, w - 0.65, 0.95, body, 14, NAVY)
    add_footer(s, 6)
    add_notes(s, f"On a disruption day, volume runs about {config.IROP_VOLUME_MULTIPLIER:g}× normal and "
                 f"rebooking calls run longer (about {m['aht_irop']:.0f} s). Peak scheduled agents go from "
                 f"{m['normal_sched']} to {m['irop_sched']}. You can't hire for that on the day, so the "
                 "recommendation is a pre-positioned flex pool plus pushing simple contacts to the app "
                 "— the deflection figure is a volume-only upper bound.")


def slide_next(prs, m):
    s = new_slide(prs, NAVY)
    add_title(s, "Impact and next steps", color=WHITE)
    impact = [
        (usd_k(m["saved_real"]), f"per {m['n_days']} days vs fixed {m['block_hours']}-hour shifts"),
        (f"+{usd_k(m['defl_saved'])}", "more with Fly Delta app deflection"),
        (f"{config.TARGET_SL * 100:.0f}/{config.TARGET_ANSWER_SECONDS}",
         "service target met in every half-hour of the plan"),
    ]
    for i, (num, label) in enumerate(impact):
        add_stat(s, 0.8, 1.7 + i * 1.5, 4.9, num, label, num_color=WHITE, label_color=ICE,
                 label_size=14, label_h=0.45)
    x = 6.3
    add_text(s, x, 1.85, 6.4, 0.45, "Next steps", 20, WHITE, bold=True)
    steps = [
        "Pilot at ATL: run interval-based schedules for one month against a control group.",
        "Turn half-hour requirements into real shift rosters (shift lengths, breaks, labor rules).",
        "Extend to chat and app channels, with intraday re-forecasting.",
        "Swap in real contact logs — the pipeline and its tests run unchanged.",
    ]
    for i, text in enumerate(steps):
        y = 2.5 + i * 0.95
        add_badge(s, x, y, 0.45, str(i + 1), fill=RED, size=14)
        add_text(s, x + 0.65, y - 0.02, 5.75, 0.8, text, 16, WHITE)
    tb = add_text(s, 0.8, 6.55, 11.7, 0.3, "Interactive dashboard: ", 13, ICE)
    link = _run(tb.text_frame.paragraphs[0], TABLEAU_URL, 13, WHITE)
    link.hyperlink.address = TABLEAU_URL
    add_notes(s, "Impact: the savings and service figures above. Next steps: pilot the schedule at ATL "
                 "against a control group; turn the half-hour requirement curve into real rosters "
                 "(out of scope here); add other channels and intraday re-forecasting; and plug in "
                 "real contact data — the pipeline doesn't change.")


def slide_appendix(prs, m):
    s = new_slide(prs)
    add_title(s, "Appendix: assumptions and data notes")
    rows = [
        ("Assumption", "Value (config.py)"),
        ("Service target", f"{config.TARGET_SL:.0%} of calls answered within "
                           f"{config.TARGET_ANSWER_SECONDS} s"),
        ("Max occupancy", f"{config.MAX_OCCUPANCY:.0%}"),
        ("Shrinkage", f"{config.SHRINKAGE:.0%} of paid time (breaks, training, meetings)"),
        ("Loaded agent cost", f"${config.AGENT_HOURLY_COST:.0f} per hour"),
        ("Interval / horizon", f"{config.INTERVAL_MINUTES} min · {m['n_days']} days "
                               f"({day(m['start'])}–{day(m['end'])}, {m['end'].year})"),
        ("Realistic baseline", f"{m['block_hours']}h shifts, each staffed to its busiest half-hour"),
        ("IROP scenario", f"{config.IROP_VOLUME_MULTIPLIER:g}× volume, {m['aht_irop']:.0f} s "
                          "average handle time"),
        ("Self-service deflection", f"{config.DEFLECTION_RATE:.0%} of "
                                    f"{', '.join(r.replace('_', ' ') for r in config.DEFLECTABLE_REASONS)}"),
        ("Data", f"{m['total_contacts'] / 1e6:.1f}M synthetic contacts (2025), seeded"),
    ]
    table = s.shapes.add_table(len(rows), 2, Inches(MARGIN), Inches(1.85), Inches(6.6),
                               Inches(0.42 * len(rows))).table
    table.columns[0].width, table.columns[1].width = Inches(2.3), Inches(4.3)
    for r, (k, v) in enumerate(rows):
        table.rows[r].height = Inches(0.42)
        for c, text in enumerate((k, v)):
            cell = table.cell(r, c)
            cell.fill.solid()
            cell.fill.fore_color.rgb = NAVY if r == 0 else (TINT if r % 2 else WHITE)
            cell.margin_left = cell.margin_right = Inches(0.12)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            _run(cell.text_frame.paragraphs[0], text, 12, WHITE if r == 0 else NAVY,
                 bold=(r == 0 or c == 0))
    x = 7.6
    w = SLIDE_W - MARGIN - x
    add_card(s, x, 1.85, w, 2.1, "Synthetic data",
             "Real Delta contact logs are confidential, so every contact is generated from "
             "documented assumptions. The method — not the dataset — is the deliverable; real data "
             "plugs into the same pipeline.", body_size=13)
    gap = m["fut_mean"] / m["prior_mean"] - 1
    add_card(s, x, 4.2, w, 2.1, "Known caveat",
             f"The January forecast averages {m['fut_mean']:,.0f} contacts/day, {gap:.0%} above last "
             f"January's {m['prior_mean']:,.0f}: one year of history can't teach the model January's "
             "seasonal dip. Absolute dollars are likely overstated; the percentage savings are more "
             "robust.", header_color=RED, body_size=13)
    add_footer(s, 8)
    add_notes(s, "Every business assumption lives in config.py — change one number and re-run the "
                 "pipeline and this deck. The data is synthetic by design. The main caveat is the "
                 "January forecast level, which one year of history can't correct.")


def _set_link_color(prs, hex_rgb: str) -> None:
    """Hyperlinks render in the theme's hlink color (run colors are ignored), which defaults to
    pure blue — unreadable on the navy closing slide. Recolor the theme's hlink/folHlink."""
    theme = prs.slide_master.part.part_related_by(RT.THEME)
    blob = theme.blob
    for tag in (b"hlink", b"folHlink"):
        blob = re.sub(rb'(<a:' + tag + rb'><a:srgbClr val=")[0-9A-Fa-f]{6}', rb"\g<1>" + hex_rgb.encode(),
                      blob)
    theme._blob = blob            # theme is a plain (non-XML) part in python-pptx


def _keynote_compat(prs) -> None:
    """Fix three things PowerPoint tolerates but Apple Keynote rejects as "file format is
    invalid" (see github.com/anthropics/skills/issues/1167):
      1. <p:sldSz> keeps the 4:3 template's type="screen4x3" on a 16:9 deck -> drop it;
      2. the notes master is related but not listed in <p:notesMasterIdLst> -> register it;
      3. the template's Windows printer-settings .bin part -> remove it.
    Call after all slides (and their notes) exist."""
    pres = prs.part._element
    pres.find(qn("p:sldSz")).attrib.pop("type", None)
    rels = prs.part.rels
    notes_rid = next((rid for rid, rel in rels.items() if rel.reltype == RT.NOTES_MASTER), None)
    if notes_rid and pres.find(qn("p:notesMasterIdLst")) is None:
        lst = OxmlElement("p:notesMasterIdLst")
        pres.find(qn("p:sldMasterIdLst")).addnext(lst)   # schema order: right after the masters
        entry = OxmlElement("p:notesMasterId")
        lst.append(entry)
        entry.set(qn("r:id"), notes_rid)                  # set after insertion -> reuses r: prefix
    for rid in [rid for rid, rel in rels.items() if rel.reltype == RT.PRINTER_SETTINGS]:
        rels.pop(rid)                                     # unreferenced part is then not written


def build_deck(path: Path = DECK_PATH) -> Path:
    m = load_metrics()
    prs = Presentation()
    # Exact 16:9 EMU size (Inches(13.333) would round to 12191695).
    prs.slide_width, prs.slide_height = Emu(12192000), Emu(6858000)
    for build in (slide_title, slide_problem, slide_recommendation, slide_approach,
                  slide_evidence, slide_irop, slide_next, slide_appendix):
        build(prs, m)
    _set_link_color(prs, str(ICE))
    _keynote_compat(prs)
    path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(path))
    print(f"Wrote {path.relative_to(PROJECT_ROOT)} ({len(prs.slides)} slides) — headline: "
          f"{usd(m['saved_real'])} ({m['pct_real']:.0%}) saved vs {m['block_hours']}h shifts, "
          f"SL {m['sl']:.1%}")
    return path


def main() -> None:
    build_deck()


if __name__ == "__main__":
    main()
