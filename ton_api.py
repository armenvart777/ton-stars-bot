import ssl
import aiohttp
import certifi
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _ssl_context() -> ssl.SSLContext:
    ...


FRAGMENT_ADDRESS = "0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a"


@dataclass
class StarTransaction:
    timestamp: datetime
    stars: int
    ton_amount: float
    rate_ton_per_star: float
    ref_code: str
    event_id: str

    @property
    def date_str(self) -> str:
        ...

    @property
    def short_date(self) -> str:
        ...

    def cost_rub(self, ton_rub: float) -> float:
        ...

    def cost_usdt(self, ton_usdt: float) -> float:
        ...

    def revenue(self, sell_price: float) -> float:
        ...

    def profit_rub(self, ton_rub: float, sell_price: float) -> float:
        ...

    def profit_pct(self, ton_rub: float, sell_price: float) -> float:
        ...


@dataclass
class Rates:
    ton_rub: float
    ton_usdt: float
    updated_at: datetime


@dataclass
class WalletInfo:
    address: str
    balance_ton: float
    status: str


async def fetch_rates(session: aiohttp.ClientSession) -> Rates:
    ...


async def fetch_wallet_info(session: aiohttp.ClientSession, base_url: str, wallet: str) -> WalletInfo:
    ...


async def fetch_all_events(session: aiohttp.ClientSession, base_url: str, wallet: str) -> list[dict]:
    ...


def parse_stars_from_comment(comment: str) -> Optional[int]:
    ...


def parse_ref_from_comment(comment: str) -> str:
    ...


def extract_star_transactions(events: list[dict]) -> list[StarTransaction]:
    ...


@dataclass
class FullReport:
    wallet: WalletInfo
    transactions: list[StarTransaction]
    rates: Rates

    @property
    def total_stars(self) -> int:
        ...

    @property
    def total_ton(self) -> float:
        ...

    @property
    def total_cost_rub(self) -> float:
        ...

    @property
    def total_cost_usdt(self) -> float:
        ...

    def total_revenue(self, sell_price: float) -> float:
        ...

    def total_profit(self, sell_price: float) -> float:
        ...

    def avg_cost_per_star_rub(self) -> float:
        ...

    def profit_pct(self, sell_price: float) -> float:
        ...


async def get_full_report(base_url: str, wallet: str) -> FullReport:
    ...
