"""Advanced analytics for star transactions."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ton_api import FullReport, StarTransaction


@dataclass
class PeriodStats:
    label: str
    txs: list[StarTransaction]
    ton_rub: float

    @property
    def count(self) -> int:
        ...

    @property
    def total_stars(self) -> int:
        ...

    @property
    def total_ton(self) -> float:
        ...

    @property
    def total_rub(self) -> float:
        ...

    def profit(self, sell_price: float) -> float:
        ...

    @property
    def avg_stars_per_deal(self) -> float:
        ...

    @property
    def avg_ton_per_deal(self) -> float:
        ...

    @property
    def avg_cost_per_star(self) -> float:
        ...


def filter_period(txs: list[StarTransaction], days: int | None) -> list[StarTransaction]:
    ...


def get_period_stats(r: FullReport, days: int | None, label: str) -> PeriodStats:
    ...


@dataclass
class SizeBucket:
    label: str
    count: int
    total_stars: int
    total_ton: float
    pct_of_total: float


def size_distribution(r: FullReport) -> list[SizeBucket]:
    ...


def hourly_distribution(txs: list[StarTransaction]) -> dict[int, int]:
    ...


def hourly_bar_chart(txs: list[StarTransaction]) -> str:
    ...


@dataclass
class RateAnalysis:
    min_rate: float
    max_rate: float
    avg_rate: float
    min_tx: StarTransaction | None
    max_tx: StarTransaction | None
    avg_cost_rub: float
    min_cost_rub: float
    max_cost_rub: float


def analyze_rates(r: FullReport) -> RateAnalysis:
    ...


@dataclass
class PriceComparison:
    sell_price: float
    revenue: float
    profit: float
    margin_pct: float
    per_star_profit: float


def compare_prices(r: FullReport, prices: list[float]) -> list[PriceComparison]:
    ...


@dataclass
class DayEntry:
    date: str
    count: int
    stars: int
    ton: float
    cost_rub: float
    profit_1: float
    profit_2: float
    cumulative_stars: int
    cumulative_profit_1: float


def daily_breakdown(r: FullReport, sell_1: float, sell_2: float) -> list[DayEntry]:
    ...


@dataclass
class BestWorstDeals:
    cheapest_tx: StarTransaction | None
    expensive_tx: StarTransaction | None
    biggest_tx: StarTransaction | None
    most_profitable_tx: StarTransaction | None
    least_profitable_tx: StarTransaction | None
    cheapest_rub: float
    expensive_rub: float


def best_worst_deals(r: FullReport, sell_price: float) -> BestWorstDeals:
    ...


@dataclass
class ProfitForecast:
    days_tracked: int
    avg_daily_stars: float
    avg_daily_deals: float
    avg_daily_profit: float
    week_stars: float
    week_profit: float
    month_stars: float
    month_profit: float


def profit_forecast(r: FullReport, sell_price: float, recent_days: int = 30) -> ProfitForecast:
    ...


@dataclass
class SpreadByHour:
    hour: int
    avg_cost_rub: float
    count: int
    total_stars: int


def spread_by_hour(r: FullReport) -> list[SpreadByHour]:
    ...


@dataclass
class SpreadByWeekday:
    weekday: int
    name: str
    avg_cost_rub: float
    count: int
    total_stars: int


def spread_by_weekday(r: FullReport) -> list[SpreadByWeekday]:
    ...
