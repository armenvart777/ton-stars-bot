"""Chart generation for TON Stars Tracker."""

import io
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages

from ton_api import FullReport


def _setup_style():
    ...


def _to_bytes(fig) -> bytes:
    ...


def _fig_daily_profit(r: FullReport, sell_price: float):
    ...


def _fig_rate_history(r: FullReport):
    ...


def _fig_volume(r: FullReport):
    ...


def _fig_cumulative(r: FullReport, sell_price: float):
    ...


def chart_daily_profit(r: FullReport, sell_price: float) -> bytes:
    ...


def chart_rate_history(r: FullReport) -> bytes:
    ...


def chart_volume(r: FullReport) -> bytes:
    ...


def chart_cumulative(r: FullReport, sell_price: float) -> bytes:
    ...


def generate_pdf_report(r: FullReport, sp1: float, sp2: float) -> bytes:
    """Generate multi-page PDF with summary and all charts."""
    ...
