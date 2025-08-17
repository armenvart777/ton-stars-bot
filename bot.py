import asyncio
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile,
)
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv

import aiohttp
from ton_api import get_full_report, FullReport, _ssl_context, fetch_rates
from analytics import (
    get_period_stats, size_distribution, hourly_bar_chart,
    analyze_rates, compare_prices, daily_breakdown,
    best_worst_deals, profit_forecast, spread_by_hour, spread_by_weekday,
)
from charts import (
    chart_daily_profit, chart_rate_history, chart_volume, chart_cumulative,
    generate_pdf_report,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TON_WALLET = os.getenv("TON_WALLET")  # Default wallet (Armen)
TONAPI_BASE = os.getenv("TONAPI_BASE", "https://tonapi.io/v2")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(",")))
SELL_PRICE_1 = float(os.getenv("SELL_PRICE_1", "1.3"))
SELL_PRICE_2 = float(os.getenv("SELL_PRICE_2", "1.27"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

PER_PAGE = 8


# ── Wallet Management ─────────────────────────────────────────────

WALLETS_FILE = Path(__file__).parent / "wallets.json"

# user_id -> their own wallet key
OWNER_MAP = {
    6629393200: "armen",
    7113414227: "sas",
}

WALLETS: dict[str, dict] = {}
_user_selected: dict[int, str] = {}  # user_id -> selected wallet key


def load_wallets():
    global WALLETS
    if WALLETS_FILE.exists():
        with open(WALLETS_FILE, encoding="utf-8") as f:
            WALLETS.update(json.load(f))
    else:
        WALLETS["armen"] = {"name": "Армен", "address": TON_WALLET}
        WALLETS["sas"] = {"name": "Сас", "address": None}
        save_wallets()


def save_wallets():
    with open(WALLETS_FILE, "w", encoding="utf-8") as f:
        json.dump(WALLETS, f, ensure_ascii=False, indent=2)


def get_selected_key(user_id: int) -> str:
    if user_id not in _user_selected:
        _user_selected[user_id] = OWNER_MAP.get(user_id, "armen")
    return _user_selected[user_id]


def get_selected_wallet(user_id: int) -> dict | None:
    key = get_selected_key(user_id)
    return WALLETS.get(key)


def get_wallet_address(user_id: int) -> str | None:
    w = get_selected_wallet(user_id)
    return w["address"] if w else None


def wname(user_id: int) -> str:
    w = get_selected_wallet(user_id)
    return w["name"] if w else "?"


def own_wallet_configured(user_id: int) -> bool:
    """Check if this user's OWN wallet is configured."""
    own_key = OWNER_MAP.get(user_id)
    if not own_key:
        return True  # Not in OWNER_MAP = no setup needed
    w = WALLETS.get(own_key)
    return bool(w and w.get("address"))


# ── Alert Storage ──────────────────────────────────────────────────

ALERTS_FILE = Path(__file__).parent / "alerts.json"
_rate_alerts: list[dict] = []
_last_tx_ids: dict[str, set] = {}
_monitor_initialized: set = set()


def load_alerts():
    global _rate_alerts
    if ALERTS_FILE.exists():
        try:
            with open(ALERTS_FILE, encoding="utf-8") as f:
                _rate_alerts.extend(json.load(f))
        except Exception:
            pass


def save_alerts():
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump(_rate_alerts, f, ensure_ascii=False)


# ── Goals Storage ─────────────────────────────────────────────────

GOALS_FILE = Path(__file__).parent / "goals.json"
_goals: list[dict] = []


def load_goals():
    global _goals
    if GOALS_FILE.exists():
        try:
            with open(GOALS_FILE, encoding="utf-8") as f:
                _goals.extend(json.load(f))
        except Exception:
            pass


def save_goals():
    with open(GOALS_FILE, "w", encoding="utf-8") as f:
        json.dump(_goals, f, ensure_ascii=False)


# ── Auto-Report Storage ──────────────────────────────────────────

AUTOREPORT_FILE = Path(__file__).parent / "autoreport.json"
_autoreport: dict = {"enabled": False, "last_sent_week": None}


def load_autoreport():
    global _autoreport
    if AUTOREPORT_FILE.exists():
        try:
            with open(AUTOREPORT_FILE, encoding="utf-8") as f:
                _autoreport.update(json.load(f))
        except Exception:
            pass


def save_autoreport():
    with open(AUTOREPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(_autoreport, f, ensure_ascii=False)


# ── Withdrawal Commission Definitions ────────────────────────────

PLAYEROK_USDT_RATE = 79.36  # Курс USDT на playerok.com (₽)

WITHDRAW_METHODS = [
    {"key": "sbp",    "name": "СБП",              "pct": 6.0,  "fixed": 0,  "min_fee": 60,  "type": "rub"},
    {"key": "card",   "name": "Банковская карта",  "pct": 6.0,  "fixed": 0,  "min_fee": 60,  "type": "rub"},
    {"key": "usdt",   "name": "USDT (TRC20)",      "pct": 4.0,  "fixed_usdt": 1, "min_fee": 0, "type": "usdt"},
    {"key": "fcard",  "name": "Иностранная карта", "pct": 10.0, "fixed": 0,  "min_fee": 150, "type": "rub"},
    {"key": "yoomoney","name": "ЮMoney",           "pct": 6.0,  "fixed": 0,  "min_fee": 60,  "type": "rub"},
]


def calc_withdraw_fee(method: dict, amount_rub: float) -> float:
    """Calculate withdrawal fee for a given method and amount in RUB."""
    if method["type"] == "usdt":
        pct_fee = amount_rub * method["pct"] / 100
        fixed_fee = method.get("fixed_usdt", 0) * PLAYEROK_USDT_RATE
        return pct_fee + fixed_fee
    else:
        pct_fee = amount_rub * method["pct"] / 100
        fixed = method.get("fixed", 0)
        fee = pct_fee + fixed
        min_fee = method.get("min_fee", 0)
        return max(fee, min_fee)


# ── FSM States ─────────────────────────────────────────────────────

class SetupState(StatesGroup):
    waiting_address = State()


class CalcState(StatesGroup):
    waiting_price = State()


class AlertState(StatesGroup):
    waiting_value = State()


class GoalState(StatesGroup):
    waiting_value = State()


# ── Security ───────────────────────────────────────────────────────

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def deny(obj):
    text = "🔒 Доступ запрещён.\nЭтот бот только для владельца."
    if isinstance(obj, Message):
        await obj.answer(text)
    elif isinstance(obj, CallbackQuery):
        await obj.answer(text, show_alert=True)


async def safe_edit(message, text: str, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass


async def safe_edit_w(message, text: str, user_id: int, reply_markup=None):
    """safe_edit with wallet indicator prepended."""
    prefix = f"👛 <i>{wname(user_id)}</i>\n\n"
    await safe_edit(message, prefix + text, reply_markup)


# ── Cache (per-wallet) ────────────────────────────────────────────

_cache: dict[str, dict] = {}


async def load_data(wallet_addr: str, force: bool = False) -> FullReport:
    now = datetime.now(tz=timezone.utc)
    entry = _cache.get(wallet_addr)
    if not force and entry and entry.get("report") and entry.get("updated_at"):
        if (now - entry["updated_at"]).total_seconds() < 120:
            return entry["report"]

    report = await get_full_report(TONAPI_BASE, wallet_addr)
    _cache[wallet_addr] = {"report": report, "updated_at": now}
    return report


async def get_data(user_id: int, force: bool = False) -> FullReport | None:
    addr = get_wallet_address(user_id)
    if not addr:
        return None
    return await load_data(addr, force)


async def _get_current_rate() -> float:
    for entry in _cache.values():
        r = entry.get("report")
        if r and r.rates.ton_rub > 0:
            return r.rates.ton_rub
    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_context())
        async with aiohttp.ClientSession(connector=connector) as session:
            rates = await fetch_rates(session)
            return rates.ton_rub
    except Exception:
        return 0


# ── Helpers ────────────────────────────────────────────────────────

def N(n: float, d: int = 2) -> str:
    return f"{n:,.{d}f}".replace(",", " ")


def E(val: float) -> str:
    return "🟢" if val >= 0 else "🔴"


# ── Keyboards ──────────────────────────────────────────────────────

def kb_main(user_id: int) -> InlineKeyboardMarkup:
    wallet_label = f"👛 {wname(user_id)}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=wallet_label, callback_data="switch_wallet")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [
            InlineKeyboardButton(text=f"💰 {SELL_PRICE_1}₽", callback_data=f"profit:{SELL_PRICE_1}"),
            InlineKeyboardButton(text=f"💰 {SELL_PRICE_2}₽", callback_data=f"profit:{SELL_PRICE_2}"),
        ],
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data="period:1"),
            InlineKeyboardButton(text="📅 7 дн", callback_data="period:7"),
            InlineKeyboardButton(text="📅 30 дн", callback_data="period:30"),
        ],
        [InlineKeyboardButton(text="📈 Аналитика курсов", callback_data="rates")],
        [InlineKeyboardButton(text="📦 По размерам сделок", callback_data="sizes")],
        [InlineKeyboardButton(text="🕐 Активность по часам", callback_data="hours")],
        [InlineKeyboardButton(text="📋 Сравнение цен", callback_data="compare")],
        [InlineKeyboardButton(text="🧾 История сделок", callback_data="hist:0")],
        [InlineKeyboardButton(text="📈 По дням + накопительно", callback_data="daily")],
        [InlineKeyboardButton(text="👛 Кошелёк", callback_data="wallet")],
        [InlineKeyboardButton(text="📊 Графики", callback_data="charts")],
        [InlineKeyboardButton(text="🆚 Сравнение кошельков", callback_data="cmp_wallets")],
        [InlineKeyboardButton(text="🏆 Лучшие/худшие сделки", callback_data="bestworst")],
        [InlineKeyboardButton(text="🔮 Прогноз прибыли", callback_data="forecast")],
        [InlineKeyboardButton(text="📉 Спред-анализ", callback_data="spread")],
        [InlineKeyboardButton(text="💸 Комиссия на вывод", callback_data="commission")],
        [InlineKeyboardButton(text="🎯 Цели", callback_data="goals")],
        [
            InlineKeyboardButton(text="🔔 Алерты", callback_data="alerts"),
            InlineKeyboardButton(text="📬 Авто-отчёт", callback_data="autoreport"),
        ],
        [
            InlineKeyboardButton(text="🧮 Калькулятор", callback_data="calc"),
            InlineKeyboardButton(text="📄 CSV", callback_data="export"),
            InlineKeyboardButton(text="📑 PDF", callback_data="pdf"),
        ],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
    ])


def kb_wallet_select(user_id: int) -> InlineKeyboardMarkup:
    selected_key = get_selected_key(user_id)
    rows = []
    for key, w in WALLETS.items():
        if not w.get("address"):
            continue
        mark = "✅ " if key == selected_key else ""
        rows.append([InlineKeyboardButton(
            text=f"{mark}{w['name']}",
            callback_data=f"selwallet:{key}",
        )])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])


def kb_profit_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"💰 {SELL_PRICE_1}₽", callback_data=f"profit:{SELL_PRICE_1}"),
            InlineKeyboardButton(text=f"💰 {SELL_PRICE_2}₽", callback_data=f"profit:{SELL_PRICE_2}"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])


def kb_history(page: int, total: int) -> InlineKeyboardMarkup:
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"hist:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"hist:{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_charts() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Прибыль по дням ({SELL_PRICE_1}₽)", callback_data=f"chart:profit:{SELL_PRICE_1}")],
        [InlineKeyboardButton(text=f"💰 Прибыль по дням ({SELL_PRICE_2}₽)", callback_data=f"chart:profit:{SELL_PRICE_2}")],
        [InlineKeyboardButton(text="💱 Курс за звезду", callback_data="chart:rate")],
        [InlineKeyboardButton(text="⭐ Объёмы по дням", callback_data="chart:volume")],
        [InlineKeyboardButton(text=f"📊 Накопительная ({SELL_PRICE_1}₽)", callback_data=f"chart:cumul:{SELL_PRICE_1}")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])


def kb_alerts(user_id: int) -> InlineKeyboardMarkup:
    my_alerts = [a for a in _rate_alerts if a["user_id"] == user_id]
    rows = [
        [InlineKeyboardButton(text="⬆️ Уведомить когда выше", callback_data="alert:above")],
        [InlineKeyboardButton(text="⬇️ Уведомить когда ниже", callback_data="alert:below")],
    ]
    if my_alerts:
        rows.append([InlineKeyboardButton(text="🗑 Удалить мои алерты", callback_data="alert:clear")])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_goals(user_id: int) -> InlineKeyboardMarkup:
    my_goals = [g for g in _goals if g["user_id"] == user_id]
    rows = [
        [InlineKeyboardButton(text="⭐ Цель по звёздам", callback_data="goal:stars")],
        [InlineKeyboardButton(text="💰 Цель по прибыли", callback_data="goal:profit")],
    ]
    if my_goals:
        rows.append([InlineKeyboardButton(text="🗑 Удалить цели", callback_data="goal:clear")])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_forecast() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 дн", callback_data="fc:7"),
            InlineKeyboardButton(text="14 дн", callback_data="fc:14"),
            InlineKeyboardButton(text="30 дн", callback_data="fc:30"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])


# ── Formatters ─────────────────────────────────────────────────────

def fmt_start(wallet_name: str | None = None) -> str:
    wn = f"\n👛 Кошелёк: <b>{wallet_name}</b>\n" if wallet_name else "\n"
    return (
        "╔══════════════════════════════╗\n"
        "║    💎 <b>TON Stars Tracker</b>    ║\n"
        "╚══════════════════════════════╝\n"
        f"{wn}"
        "Покупки Telegram Stars через\n"
        "Fragment • Прибыль • Аналитика\n"
        "\n"
        "Выберите раздел:"
    )


def fmt_setup() -> str:
    return (
        "╔══════════════════════════════╗\n"
        "║    💎 <b>TON Stars Tracker</b>    ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        "👋 Привет! Для начала отправь\n"
        "свой <b>TON адрес кошелька</b>.\n"
        "\n"
        "Например:\n"
        "<code>UQBu2ys7I0wYaPPyyl...</code>"
    )


def fmt_stats(r: FullReport) -> str:
    avg = r.avg_cost_per_star_rub()
    txs = r.transactions
    first = txs[-1].date_str if txs else "—"
    last = txs[0].date_str if txs else "—"

    top5 = sorted(txs, key=lambda t: t.stars, reverse=True)[:5]
    top_lines = "\n".join(
        f"  {i}. {t.stars:,}⭐ — {t.ton_amount:.2f} TON ({t.short_date})"
        for i, t in enumerate(top5, 1)
    )

    avg_usd = (avg / r.rates.ton_rub * r.rates.ton_usdt) if r.rates.ton_rub > 0 else 0

    return (
        "╔══════════════════════════════╗\n"
        "║      📊 <b>ОБЩАЯ СТАТИСТИКА</b>     ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"⭐ Всего звёзд:   <b>{N(r.total_stars, 0)}</b>\n"
        f"💎 Потрачено TON:  <b>{N(r.total_ton)}</b>\n"
        f"💳 В RUB:          <b>{N(r.total_cost_rub)} ₽</b>\n"
        f"💵 В USD:          <b>${N(r.total_cost_usdt)}</b>\n"
        f"\n"
        f"📦 Сделок: <b>{len(txs)}</b>\n"
        f"📅 Период: {first} — {last}\n"
        f"\n"
        f"┌─ 💱 Курсы ─────────────────┐\n"
        f"│ TON/RUB: <b>{N(r.rates.ton_rub)}</b>\n"
        f"│ TON/USD: <b>${N(r.rates.ton_usdt)}</b>\n"
        f"│ Себест. 1⭐: <b>{N(avg, 4)}₽</b> (${N(avg_usd, 4)})\n"
        f"└────────────────────────────┘\n"
        f"\n"
        f"┌─ 🏆 ТОП-5 покупок ─────────┐\n"
        f"{top_lines}\n"
        f"└────────────────────────────┘"
    )


def fmt_profit(r: FullReport, sp: float) -> str:
    rev = r.total_revenue(sp)
    cost = r.total_cost_rub
    profit = r.total_profit(sp)
    pct = r.profit_pct(sp)
    avg = r.avg_cost_per_star_rub()
    margin = sp - avg

    pairs = [(t, t.profit_rub(r.rates.ton_rub, sp)) for t in r.transactions]
    best = sorted(pairs, key=lambda x: x[1], reverse=True)[:5]
    worst = sorted(pairs, key=lambda x: x[1])[:3]

    best_l = "\n".join(
        f"  {E(p)} {t.stars}⭐ → <b>{N(p)}₽</b> ({t.short_date})"
        for t, p in best
    )
    worst_l = "\n".join(
        f"  {E(p)} {t.stars}⭐ → <b>{N(p)}₽</b> ({t.short_date})"
        for t, p in worst
    )

    ok = sum(1 for _, p in pairs if p >= 0)
    bad = len(pairs) - ok

    # USD equivalents
    rub_to_usd = r.rates.ton_usdt / r.rates.ton_rub if r.rates.ton_rub > 0 else 0
    cost_usd = cost * rub_to_usd
    profit_usd = profit * rub_to_usd

    return (
        "╔══════════════════════════════╗\n"
        f"║  💰 <b>ПРИБЫЛЬ ({sp}₽/⭐)</b>      ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"💳 Затраты:  <b>{N(cost)}₽</b> (${N(cost_usd)})\n"
        f"💵 Выручка:  <b>{N(rev)} ₽</b>\n"
        f"{E(profit)} Прибыль:  <b>{N(profit)}₽</b> (${N(profit_usd)})\n"
        f"📈 Маржа:    <b>{N(pct, 1)}%</b>\n"
        "\n"
        f"┌─ На 1 звезду ──────────────┐\n"
        f"│ Себест: <b>{N(avg, 4)}₽</b>\n"
        f"│ Продажа: <b>{sp}₽</b>\n"
        f"│ Маржа: <b>{N(margin, 4)}₽</b> ({E(margin)})\n"
        f"└────────────────────────────┘\n"
        "\n"
        f"📊 В плюсе: <b>{ok}</b> 🟢  В минусе: <b>{bad}</b> 🔴\n"
        "\n"
        f"┌─ 🏆 Лучшие 5 ──────────────┐\n"
        f"{best_l}\n"
        f"└────────────────────────────┘\n"
        f"┌─ 📉 Худшие 3 ──────────────┐\n"
        f"{worst_l}\n"
        f"└────────────────────────────┘"
    )


def fmt_period(r: FullReport, days: int | None) -> str:
    labels = {1: "СЕГОДНЯ", 7: "7 ДНЕЙ", 30: "30 ДНЕЙ", None: "ВСЁ ВРЕМЯ"}
    label = labels.get(days, f"{days} ДНЕЙ")
    ps = get_period_stats(r, days, label)

    if ps.count == 0:
        return (
            f"╔══════════════════════════════╗\n"
            f"║  📅 <b>{label}</b>               ║\n"
            f"╚══════════════════════════════╝\n"
            f"\nНет сделок за этот период."
        )

    p1 = ps.profit(SELL_PRICE_1)
    p2 = ps.profit(SELL_PRICE_2)

    return (
        f"╔══════════════════════════════╗\n"
        f"║  📅 <b>{label}</b>               ║\n"
        f"╚══════════════════════════════╝\n"
        f"\n"
        f"📦 Сделок: <b>{ps.count}</b>\n"
        f"⭐ Звёзд: <b>{N(ps.total_stars, 0)}</b>\n"
        f"💎 TON: <b>{N(ps.total_ton)}</b>\n"
        f"💳 RUB: <b>{N(ps.total_rub)} ₽</b>\n"
        f"\n"
        f"┌─ Средние показатели ───────┐\n"
        f"│ Звёзд/сделку: <b>{N(ps.avg_stars_per_deal, 0)}</b>\n"
        f"│ TON/сделку: <b>{N(ps.avg_ton_per_deal)}</b>\n"
        f"│ Себест. 1⭐: <b>{N(ps.avg_cost_per_star, 4)}₽</b>\n"
        f"└────────────────────────────┘\n"
        f"\n"
        f"{E(p1)} Прибыль ({SELL_PRICE_1}₽): <b>{N(p1)} ₽</b>\n"
        f"{E(p2)} Прибыль ({SELL_PRICE_2}₽): <b>{N(p2)} ₽</b>"
    )


def fmt_rates(r: FullReport) -> str:
    ra = analyze_rates(r)
    if ra.min_tx is None:
        return "Нет данных для анализа курсов."

    spread = ra.max_cost_rub - ra.min_cost_rub
    spread_pct = (spread / ra.avg_cost_rub * 100) if ra.avg_cost_rub > 0 else 0

    return (
        "╔══════════════════════════════╗\n"
        "║   📈 <b>АНАЛИТИКА КУРСОВ</b>      ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        "┌─ Цена за 1⭐ в TON ────────┐\n"
        f"│ Мин: <b>{ra.min_rate:.6f}</b>\n"
        f"│ Сред: <b>{ra.avg_rate:.6f}</b>\n"
        f"│ Макс: <b>{ra.max_rate:.6f}</b>\n"
        "└────────────────────────────┘\n"
        "\n"
        "┌─ Цена за 1⭐ в RUB ────────┐\n"
        f"│ Мин: <b>{N(ra.min_cost_rub, 4)} ₽</b>\n"
        f"│ Сред: <b>{N(ra.avg_cost_rub, 4)} ₽</b>\n"
        f"│ Макс: <b>{N(ra.max_cost_rub, 4)} ₽</b>\n"
        f"│ Разброс: <b>{N(spread, 4)} ₽</b> ({N(spread_pct, 1)}%)\n"
        "└────────────────────────────┘\n"
        "\n"
        f"🟢 Лучшая цена:\n"
        f"  {ra.min_tx.date_str} — {ra.min_tx.stars}⭐\n"
        f"  {ra.min_tx.ton_amount:.4f} TON\n"
        f"\n"
        f"🔴 Худшая цена:\n"
        f"  {ra.max_tx.date_str} — {ra.max_tx.stars}⭐\n"
        f"  {ra.max_tx.ton_amount:.4f} TON"
    )


def fmt_sizes(r: FullReport) -> str:
    buckets = size_distribution(r)

    lines = [
        "╔══════════════════════════════╗",
        "║  📦 <b>ПО РАЗМЕРАМ СДЕЛОК</b>     ║",
        "╚══════════════════════════════╝",
        "",
    ]

    for b in buckets:
        bar_len = round(b.pct_of_total / 5)
        bar = "█" * bar_len if bar_len > 0 else "░"
        lines.append(
            f"<b>{b.label}</b>\n"
            f"  {bar} {N(b.pct_of_total, 1)}%\n"
            f"  {b.count} сделок • {b.total_stars:,}⭐ • {N(b.total_ton)} TON\n"
        )

    return "\n".join(lines)


def fmt_hours(r: FullReport) -> str:
    chart = hourly_bar_chart(r.transactions)
    return (
        "╔══════════════════════════════╗\n"
        "║  🕐 <b>АКТИВНОСТЬ ПО ЧАСАМ</b>    ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"<pre>{chart}</pre>"
    )


def fmt_compare(r: FullReport) -> str:
    prices = [1.0, 1.1, 1.2, SELL_PRICE_2, SELL_PRICE_1, 1.35, 1.4, 1.5]
    comps = compare_prices(r, sorted(set(prices)))
    avg = r.avg_cost_per_star_rub()

    lines = [
        "╔══════════════════════════════╗",
        "║   📋 <b>СРАВНЕНИЕ ЦЕН</b>         ║",
        "╚══════════════════════════════╝",
        "",
        f"Себестоимость: <b>{N(avg, 4)} ₽/⭐</b>",
        f"Всего звёзд: <b>{N(r.total_stars, 0)}</b>",
        "",
    ]

    for c in comps:
        mark = " ◀️" if c.sell_price in (SELL_PRICE_1, SELL_PRICE_2) else ""
        lines.append(
            f"{E(c.profit)} <b>{c.sell_price}₽</b> → "
            f"{N(c.profit)}₽ ({N(c.margin_pct, 1)}%)"
            f"{mark}"
        )

    return "\n".join(lines)


def fmt_daily(r: FullReport) -> str:
    entries = daily_breakdown(r, SELL_PRICE_1, SELL_PRICE_2)

    lines = [
        "╔══════════════════════════════╗",
        "║  📈 <b>ПО ДНЯМ + НАКОПИТЕЛЬНО</b>  ║",
        "╚══════════════════════════════╝",
        "",
    ]

    for d in reversed(entries):
        lines.append(
            f"📅 <b>{d.date}</b> ({d.count} сд.)\n"
            f"  ⭐ {d.stars:,}  💎 {d.ton:.2f} TON  💳 {N(d.cost_rub)}₽\n"
            f"  {E(d.profit_1)} {SELL_PRICE_1}₽: <b>{N(d.profit_1)}₽</b>  "
            f"{E(d.profit_2)} {SELL_PRICE_2}₽: <b>{N(d.profit_2)}₽</b>\n"
            f"  📊 Нарастающий: {d.cumulative_stars:,}⭐ / {E(d.cumulative_profit_1)} {N(d.cumulative_profit_1)}₽\n"
        )

    return "\n".join(lines)


def fmt_history(r: FullReport, page: int) -> str:
    txs = r.transactions
    total_pages = max(1, (len(txs) + PER_PAGE - 1) // PER_PAGE)
    start = page * PER_PAGE
    page_txs = txs[start:start + PER_PAGE]
    ton_rub = r.rates.ton_rub

    lines = [
        "╔══════════════════════════════╗",
        f"║  🧾 <b>ИСТОРИЯ</b>  стр. {page + 1}/{total_pages}       ║",
        "╚══════════════════════════════╝",
        "",
    ]

    for i, t in enumerate(page_txs, start=start + 1):
        cost_r = t.cost_rub(ton_rub)
        p1 = t.profit_rub(ton_rub, SELL_PRICE_1)
        p2 = t.profit_rub(ton_rub, SELL_PRICE_2)

        lines.append(
            f"<b>{i}.</b> {t.date_str}\n"
            f"   ⭐ {t.stars:,} — {t.ton_amount:.2f} TON — {N(cost_r)}₽\n"
            f"   {E(p1)} {SELL_PRICE_1}₽: {N(p1)}₽  "
            f"{E(p2)} {SELL_PRICE_2}₽: {N(p2)}₽\n"
        )

    return "\n".join(lines)


def fmt_wallet(r: FullReport) -> str:
    w = r.wallet
    bal_rub = w.balance_ton * r.rates.ton_rub
    bal_usd = w.balance_ton * r.rates.ton_usdt

    return (
        "╔══════════════════════════════╗\n"
        "║        👛 <b>КОШЕЛЁК</b>           ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"📋 Адрес:\n<code>{w.address}</code>\n"
        "\n"
        f"💎 <b>{N(w.balance_ton, 4)} TON</b>\n"
        f"💳 ≈ <b>{N(bal_rub)} ₽</b>\n"
        f"💵 ≈ <b>${N(bal_usd)}</b>\n"
        f"\n"
        f"┌─ 💱 Курсы ─────────────────┐\n"
        f"│ 1 TON = <b>{N(r.rates.ton_rub)} ₽</b>\n"
        f"│ 1 TON = <b>${N(r.rates.ton_usdt)}</b>\n"
        f"└────────────────────────────┘"
    )


def fmt_bestworst(r: FullReport, sp: float) -> str:
    bw = best_worst_deals(r, sp)
    if bw.cheapest_tx is None:
        return "Нет данных для анализа."

    ton_rub = r.rates.ton_rub

    def _tx_line(t, label_emoji, label_text):
        cost_rub = t.cost_rub(ton_rub)
        p = t.profit_rub(ton_rub, sp)
        rate_rub = t.rate_ton_per_star * ton_rub
        return (
            f"{label_emoji} <b>{label_text}</b>\n"
            f"  📅 {t.date_str}\n"
            f"  ⭐ {t.stars:,} — {t.ton_amount:.4f} TON — {cost_rub:.2f}₽\n"
            f"  💱 Курс: {rate_rub:.4f}₽/⭐\n"
            f"  {E(p)} Прибыль: {N(p)}₽\n"
        )

    return (
        "╔══════════════════════════════╗\n"
        f"║  🏆 <b>ЛУЧШИЕ/ХУДШИЕ ({sp}₽)</b>   ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        + _tx_line(bw.cheapest_tx, "🟢", f"Самая дешёвая ({bw.cheapest_rub:.4f}₽/⭐)")
        + "\n"
        + _tx_line(bw.expensive_tx, "🔴", f"Самая дорогая ({bw.expensive_rub:.4f}₽/⭐)")
        + "\n"
        + _tx_line(bw.biggest_tx, "📦", f"Самая крупная ({bw.biggest_tx.stars:,}⭐)")
        + "\n"
        + _tx_line(bw.most_profitable_tx, "💰", "Самая прибыльная")
        + "\n"
        + _tx_line(bw.least_profitable_tx, "📉", "Самая убыточная")
    )


def fmt_forecast(r: FullReport, sp: float, recent_days: int) -> str:
    fc = profit_forecast(r, sp, recent_days)
    if fc.days_tracked == 0:
        return "Нет данных для прогноза."

    return (
        "╔══════════════════════════════╗\n"
        f"║  🔮 <b>ПРОГНОЗ ({sp}₽/⭐)</b>       ║\n"
        "╚══════════════════════════════╝\n"
        f"\n📊 База: последние <b>{recent_days}</b> дн "
        f"(<b>{fc.days_tracked}</b> активных)\n"
        "\n"
        "┌─ Среднее в день ────────────┐\n"
        f"│ Сделок: <b>{N(fc.avg_daily_deals, 1)}</b>\n"
        f"│ Звёзд: <b>{N(fc.avg_daily_stars, 0)}</b>\n"
        f"│ Прибыль: <b>{E(fc.avg_daily_profit)} {N(fc.avg_daily_profit)}₽</b>\n"
        "└────────────────────────────┘\n"
        "\n"
        "┌─ Прогноз на неделю ────────┐\n"
        f"│ ⭐ {N(fc.week_stars, 0)} звёзд\n"
        f"│ {E(fc.week_profit)} {N(fc.week_profit)}₽ прибыли\n"
        "└────────────────────────────┘\n"
        "\n"
        "┌─ Прогноз на месяц ─────────┐\n"
        f"│ ⭐ {N(fc.month_stars, 0)} звёзд\n"
        f"│ {E(fc.month_profit)} {N(fc.month_profit)}₽ прибыли\n"
        "└────────────────────────────┘"
    )


def fmt_spread(r: FullReport) -> str:
    by_hour = spread_by_hour(r)
    by_day = spread_by_weekday(r)

    active_hours = [h for h in by_hour if h.count > 0]
    if not active_hours:
        return "Нет данных для спред-анализа."

    best_hour = min(active_hours, key=lambda h: h.avg_cost_rub)
    worst_hour = max(active_hours, key=lambda h: h.avg_cost_rub)

    active_days = [d for d in by_day if d.count > 0]
    best_day = min(active_days, key=lambda d: d.avg_cost_rub) if active_days else None
    worst_day = max(active_days, key=lambda d: d.avg_cost_rub) if active_days else None

    # Top-5 cheapest hours
    top_hours = sorted(active_hours, key=lambda h: h.avg_cost_rub)[:5]
    hours_lines = "\n".join(
        f"  🟢 {h.hour:02d}:00 — {h.avg_cost_rub:.4f}₽/⭐ ({h.count} сд, {h.total_stars:,}⭐)"
        for h in top_hours
    )

    days_lines = "\n".join(
        f"  {'🟢' if d == best_day else '🔴' if d == worst_day else '⚪'} "
        f"{d.name}: {d.avg_cost_rub:.4f}₽/⭐ ({d.count} сд)"
        for d in by_day if d.count > 0
    )

    spread_val = worst_hour.avg_cost_rub - best_hour.avg_cost_rub

    return (
        "╔══════════════════════════════╗\n"
        "║    📉 <b>СПРЕД-АНАЛИЗ</b>          ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"Разброс: <b>{N(spread_val, 4)}₽/⭐</b>\n"
        f"\n"
        f"🟢 Лучшее время: <b>{best_hour.hour:02d}:00</b> ({best_hour.avg_cost_rub:.4f}₽)\n"
        f"🔴 Худшее время: <b>{worst_hour.hour:02d}:00</b> ({worst_hour.avg_cost_rub:.4f}₽)\n"
        "\n"
        "┌─ ТОП-5 дешёвых часов ──────┐\n"
        f"{hours_lines}\n"
        "└────────────────────────────┘\n"
        "\n"
        "┌─ По дням недели ──────────┐\n"
        f"{days_lines}\n"
        "└────────────────────────────┘"
    )


def fmt_compare_wallets(reports: dict[str, FullReport], sp: float) -> str:
    lines = [
        "╔══════════════════════════════╗",
        f"║  🆚 <b>СРАВНЕНИЕ ({sp}₽/⭐)</b>      ║",
        "╚══════════════════════════════╝",
        "",
    ]

    for key, r in reports.items():
        name = WALLETS[key]["name"]
        avg = r.avg_cost_per_star_rub()
        profit = r.total_profit(sp)
        pct = r.profit_pct(sp)
        usd_cost = r.total_cost_usdt

        lines.append(
            f"👛 <b>{name}</b>\n"
            f"  ⭐ {r.total_stars:,} звёзд ({len(r.transactions)} сд.)\n"
            f"  💎 {N(r.total_ton)} TON\n"
            f"  💳 {N(r.total_cost_rub)}₽ / ${N(usd_cost)}\n"
            f"  💱 Себест: {N(avg, 4)}₽/⭐\n"
            f"  {E(profit)} Прибыль: <b>{N(profit)}₽</b> ({N(pct, 1)}%)\n"
        )

    # Summary
    total_stars = sum(r.total_stars for r in reports.values())
    total_profit = sum(r.total_profit(sp) for r in reports.values())
    total_cost = sum(r.total_cost_rub for r in reports.values())
    total_usd = sum(r.total_cost_usdt for r in reports.values())

    lines.append(
        "┌─ ИТОГО ────────────────────┐\n"
        f"│ ⭐ {total_stars:,} звёзд\n"
        f"│ 💳 {N(total_cost)}₽ / ${N(total_usd)}\n"
        f"│ {E(total_profit)} Прибыль: <b>{N(total_profit)}₽</b>\n"
        "└────────────────────────────┘"
    )

    return "\n".join(lines)


def fmt_goals(user_id: int, reports: dict[str, FullReport], sp: float) -> str:
    my_goals = [g for g in _goals if g["user_id"] == user_id]

    # Aggregate across all wallets
    total_stars = sum(r.total_stars for r in reports.values())
    total_profit = sum(r.total_profit(sp) for r in reports.values())

    lines = [
        "╔══════════════════════════════╗",
        "║        🎯 <b>ЦЕЛИ</b>              ║",
        "╚══════════════════════════════╝",
        "",
    ]

    if not my_goals:
        lines.append("Нет активных целей.\nУстановите цель по звёздам или прибыли!")
        return "\n".join(lines)

    for g in my_goals:
        target = g["target"]
        if g["type"] == "stars":
            current = total_stars
            label = "⭐ Звёзды"
            current_str = f"{current:,}"
            target_str = f"{target:,.0f}"
            unit = "⭐"
        else:
            current = total_profit
            label = "💰 Прибыль"
            current_str = f"{current:,.0f}₽"
            target_str = f"{target:,.0f}₽"
            unit = "₽"

        pct = min(current / target * 100, 100) if target > 0 else 0
        bar_len = 20
        filled = round(pct / 100 * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)

        emoji = "✅" if pct >= 100 else "🔄"
        lines.append(
            f"{emoji} <b>{label}</b>\n"
            f"  {bar} {pct:.1f}%\n"
            f"  {current_str} / {target_str}\n"
        )

    return "\n".join(lines)


def fmt_autoreport_summary(reports: dict[str, FullReport], sp1: float, sp2: float) -> str:
    """Weekly summary for auto-report."""
    lines = [
        "╔══════════════════════════════╗",
        "║   📬 <b>ЕЖЕНЕДЕЛЬНЫЙ ОТЧЁТ</b>    ║",
        "╚══════════════════════════════╝",
        "",
    ]

    total_stars = 0
    total_profit_1 = 0.0
    total_profit_2 = 0.0
    total_cost = 0.0
    total_usd = 0.0

    for key, r in reports.items():
        name = WALLETS[key]["name"]
        p1 = r.total_profit(sp1)
        p2 = r.total_profit(sp2)
        total_stars += r.total_stars
        total_profit_1 += p1
        total_profit_2 += p2
        total_cost += r.total_cost_rub
        total_usd += r.total_cost_usdt

        lines.append(
            f"👛 <b>{name}</b>\n"
            f"  ⭐ {r.total_stars:,} | 💎 {N(r.total_ton)} TON\n"
            f"  {E(p1)} {sp1}₽: {N(p1)}₽ | {E(p2)} {sp2}₽: {N(p2)}₽\n"
        )

    lines.append(
        f"┌─ ОБЩИЙ ИТОГ ───────────────┐\n"
        f"│ ⭐ {total_stars:,} звёзд\n"
        f"│ 💳 {N(total_cost)}₽ / ${N(total_usd)}\n"
        f"│ {E(total_profit_1)} {sp1}₽: <b>{N(total_profit_1)}₽</b>\n"
        f"│ {E(total_profit_2)} {sp2}₽: <b>{N(total_profit_2)}₽</b>\n"
        f"│ 💱 TON/RUB: {N(list(reports.values())[0].rates.ton_rub) if reports else '?'}\n"
        f"└────────────────────────────┘"
    )

    return "\n".join(lines)


def fmt_commission(r: FullReport, sp: float) -> str:
    rev = r.total_revenue(sp)
    cost = r.total_cost_rub
    profit = rev - cost

    # Рыночный курс USDT/RUB (из CoinGecko через TON)
    market_usdt_rub = r.rates.ton_rub / r.rates.ton_usdt if r.rates.ton_usdt > 0 else 0
    rate_diff = market_usdt_rub - PLAYEROK_USDT_RATE

    lines = [
        "╔══════════════════════════════╗",
        f"║  💸 <b>КОМИССИЯ НА ВЫВОД</b>      ║",
        "╚══════════════════════════════╝",
        "",
        f"Цена продажи: <b>{sp}₽/⭐</b>",
        f"Выручка: <b>{N(rev)}₽</b>",
        f"Затраты: <b>{N(cost)}₽</b>",
        f"Прибыль до вывода: <b>{E(profit)} {N(profit)}₽</b>",
        "",
        f"┌─ 💱 Курсы USDT ────────────┐",
        f"│ Playerok: <b>{PLAYEROK_USDT_RATE}₽</b>",
        f"│ Рыночный: <b>{N(market_usdt_rub, 2)}₽</b>",
        f"│ Разница: <b>{N(rate_diff, 2)}₽</b> ({'+' if rate_diff >= 0 else ''}{N(rate_diff / PLAYEROK_USDT_RATE * 100 if PLAYEROK_USDT_RATE else 0, 1)}%)",
        f"└────────────────────────────┘",
        "",
    ]

    best_net = -999_999_999
    best_method = ""

    for m in WITHDRAW_METHODS:
        if m["type"] == "usdt":
            # USDT: конвертация по курсу playerok + комиссия + потеря на курсе
            amount_usdt = rev / PLAYEROK_USDT_RATE if PLAYEROK_USDT_RATE > 0 else 0
            fee_usdt = amount_usdt * m["pct"] / 100 + m.get("fixed_usdt", 0)
            net_usdt = amount_usdt - fee_usdt
            # Реальная стоимость полученных USDT в рублях по рыночному курсу
            net_revenue = net_usdt * market_usdt_rub if market_usdt_rub > 0 else net_usdt * PLAYEROK_USDT_RATE
            total_loss = rev - net_revenue
            fee_pct_actual = (total_loss / rev * 100) if rev > 0 else 0
            net_profit = net_revenue - cost

            fee_label = f"{m['pct']}% + {m.get('fixed_usdt', 0)} USDT"
            lines.append(
                f"{'─' * 30}\n"
                f"<b>{m['name']}</b> — {fee_label}\n"
                f"  Получите: <b>{N(net_usdt, 2)} USDT</b>\n"
                f"  В рублях (рын. курс): <b>{N(net_revenue)}₽</b>\n"
                f"  Потери (комиссия+курс): <b>{N(total_loss)}₽</b> ({fee_pct_actual:.1f}%)\n"
                f"  {E(net_profit)} Чистая прибыль: <b>{N(net_profit)}₽</b>"
            )
        else:
            fee = calc_withdraw_fee(m, rev)
            net_revenue = rev - fee
            net_profit = net_revenue - cost
            fee_pct_actual = (fee / rev * 100) if rev > 0 else 0

            fee_label = f"{m['pct']}%"
            if m.get("min_fee", 0) > 0:
                fee_label += f" (мин. {m['min_fee']}₽)"

            lines.append(
                f"{'─' * 30}\n"
                f"<b>{m['name']}</b> — {fee_label}\n"
                f"  Комиссия: <b>{N(fee)}₽</b> ({fee_pct_actual:.1f}%)\n"
                f"  После вывода: <b>{N(net_revenue)}₽</b>\n"
                f"  {E(net_profit)} Чистая прибыль: <b>{N(net_profit)}₽</b>"
            )

        if net_profit > best_net:
            best_net = net_profit
            best_method = m["name"]

    lines.append("")
    lines.append(
        f"┌─ 💡 Лучший способ ─────────┐\n"
        f"│ <b>{best_method}</b>\n"
        f"│ {E(best_net)} Чистая прибыль: <b>{N(best_net)}₽</b>\n"
        f"└────────────────────────────┘"
    )

    return "\n".join(lines)


def generate_export(r: FullReport) -> str:
    ton_rub = r.rates.ton_rub
    lines = ["Дата;Звёзды;TON;RUB;Прибыль_1.3;Прибыль_1.27;Ref"]

    for t in r.transactions:
        cost = t.cost_rub(ton_rub)
        p1 = t.profit_rub(ton_rub, SELL_PRICE_1)
        p2 = t.profit_rub(ton_rub, SELL_PRICE_2)
        lines.append(
            f"{t.date_str};{t.stars};{t.ton_amount:.4f};"
            f"{cost:.2f};{p1:.2f};{p2:.2f};{t.ref_code}"
        )

    lines.append("")
    lines.append(f"Итого звёзд;{r.total_stars}")
    lines.append(f"Итого TON;{r.total_ton:.2f}")
    lines.append(f"Итого RUB;{r.total_cost_rub:.2f}")
    lines.append(f"Прибыль {SELL_PRICE_1};{r.total_profit(SELL_PRICE_1):.2f}")
    lines.append(f"Прибыль {SELL_PRICE_2};{r.total_profit(SELL_PRICE_2):.2f}")
    lines.append(f"Курс TON/RUB;{ton_rub}")

    return "\n".join(lines)


# ── Persistent Reply Keyboard ─────────────────────────────────────

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📋 Меню"), KeyboardButton(text="📊 Стата"),
         KeyboardButton(text="🔄 Обновить")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)


# ── Handlers ───────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)
    await state.clear()

    uid = message.from_user.id

    # If user's own wallet not configured yet, ask for address
    if not own_wallet_configured(uid):
        await state.set_state(SetupState.waiting_address)
        await message.answer(fmt_setup(), parse_mode="HTML", reply_markup=REPLY_KB)
        return

    await message.answer(
        fmt_start(wname(uid)),
        reply_markup=kb_main(uid),
        parse_mode="HTML",
    )
    # Ensure persistent keyboard is set
    await message.answer("⬇️", reply_markup=REPLY_KB)


@router.message(F.text == "📋 Меню")
async def msg_menu(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)
    await state.clear()
    uid = message.from_user.id
    if not own_wallet_configured(uid):
        await state.set_state(SetupState.waiting_address)
        await message.answer(fmt_setup(), parse_mode="HTML")
        return
    await message.answer(
        fmt_start(wname(uid)),
        reply_markup=kb_main(uid),
        parse_mode="HTML",
    )


@router.message(F.text == "📊 Стата")
async def msg_quick_stats(message: Message):
    if not is_admin(message.from_user.id):
        return await deny(message)
    uid = message.from_user.id
    msg = await message.answer("⏳ Загружаю...")
    r = await get_data(uid)
    if not r:
        await safe_edit(msg, "❌ Кошелёк не настроен. Отправь /start", kb_back())
        return
    prefix = f"👛 <i>{wname(uid)}</i>\n\n"
    await safe_edit(msg, prefix + fmt_stats(r), kb_profit_back())


@router.message(F.text == "🔄 Обновить")
async def msg_quick_refresh(message: Message):
    if not is_admin(message.from_user.id):
        return await deny(message)
    uid = message.from_user.id
    msg = await message.answer("🔄 Обновляю...")
    r = await get_data(uid, force=True)
    if not r:
        await safe_edit(msg, "❌ Кошелёк не настроен.", kb_back())
        return
    prefix = f"👛 <i>{wname(uid)}</i>\n\n"
    await safe_edit(msg, prefix + fmt_stats(r), kb_profit_back())


@router.message(SetupState.waiting_address)
async def setup_wallet_address(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)

    uid = message.from_user.id
    address = message.text.strip() if message.text else ""

    # Basic validation
    if len(address) < 20:
        await message.answer(
            "❌ Адрес слишком короткий.\n"
            "Отправь полный TON адрес кошелька.",
            parse_mode="HTML",
        )
        return

    # Try to verify via API
    try:
        from ton_api import _ssl_context
        import aiohttp
        connector = aiohttp.TCPConnector(ssl=_ssl_context())
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(f"{TONAPI_BASE}/accounts/{address}") as resp:
                if resp.status != 200:
                    await message.answer(
                        "❌ Не удалось найти этот адрес в сети TON.\n"
                        "Проверь адрес и отправь ещё раз.",
                        parse_mode="HTML",
                    )
                    return
    except Exception:
        pass  # If API check fails, accept the address anyway

    # Save wallet
    own_key = OWNER_MAP.get(uid, f"user_{uid}")
    if own_key in WALLETS:
        WALLETS[own_key]["address"] = address
    else:
        WALLETS[own_key] = {"name": f"User {uid}", "address": address}
    save_wallets()

    await state.clear()
    _user_selected[uid] = own_key

    await message.answer(
        f"✅ Кошелёк сохранён!\n\n"
        f"👛 <b>{WALLETS[own_key]['name']}</b>\n"
        f"<code>{address}</code>",
        parse_mode="HTML",
    )
    await message.answer(
        fmt_start(wname(uid)),
        reply_markup=kb_main(uid),
        parse_mode="HTML",
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        return await deny(message)
    uid = message.from_user.id
    msg = await message.answer("⏳ Загружаю...")
    r = await get_data(uid)
    if not r:
        await safe_edit(msg, "❌ Кошелёк не настроен. Отправь /start", kb_back())
        return
    await safe_edit_w(msg, fmt_stats(r), uid, kb_profit_back())


# ── Callbacks ──────────────────────────────────────────────────────

@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@router.callback_query(F.data == "main")
async def cb_main(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    await state.clear()
    uid = cb.from_user.id
    await safe_edit(cb.message, fmt_start(wname(uid)), kb_main(uid))
    await cb.answer()


@router.callback_query(F.data == "switch_wallet")
async def cb_switch_wallet(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await safe_edit(
        cb.message,
        "╔══════════════════════════════╗\n"
        "║   👛 <b>ВЫБОР КОШЕЛЬКА</b>        ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        "Выберите кошелёк для просмотра:",
        kb_wallet_select(uid),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("selwallet:"))
async def cb_select_wallet(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    key = cb.data.split(":")[1]

    if key not in WALLETS or not WALLETS[key].get("address"):
        await cb.answer("Кошелёк не настроен", show_alert=True)
        return

    _user_selected[uid] = key
    wallet_name = WALLETS[key]["name"]
    await cb.answer(f"👛 {wallet_name}")
    await safe_edit(cb.message, fmt_start(wallet_name), kb_main(uid))


@router.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_stats(r), uid, kb_profit_back())


@router.callback_query(F.data.startswith("profit:"))
async def cb_profit(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    sp = float(cb.data.split(":")[1])
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_profit(r, sp), uid, kb_back())


@router.callback_query(F.data.startswith("period:"))
async def cb_period(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    days = int(cb.data.split(":")[1])
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_period(r, days), uid, kb_back())


@router.callback_query(F.data == "rates")
async def cb_rates(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_rates(r), uid, kb_back())


@router.callback_query(F.data == "sizes")
async def cb_sizes(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_sizes(r), uid, kb_back())


@router.callback_query(F.data == "hours")
async def cb_hours(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_hours(r), uid, kb_back())


@router.callback_query(F.data == "compare")
async def cb_compare(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_compare(r), uid, kb_back())


@router.callback_query(F.data.startswith("hist:"))
async def cb_history(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    page = int(cb.data.split(":")[1])
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_history(r, page), uid, kb_history(page, len(r.transactions)))
    await cb.answer()


@router.callback_query(F.data == "daily")
async def cb_daily(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_daily(r), uid, kb_back())


@router.callback_query(F.data == "wallet")
async def cb_wallet(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_wallet(r), uid, kb_back())


@router.callback_query(F.data == "calc")
async def cb_calc(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    await state.set_state(CalcState.waiting_price)
    await safe_edit(
        cb.message,
        "╔══════════════════════════════╗\n"
        "║      🧮 <b>КАЛЬКУЛЯТОР</b>         ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        "Введите свою цену продажи за 1⭐\n"
        "в рублях (например: <b>1.35</b>):",
        kb_back(),
    )
    await cb.answer()


@router.message(CalcState.waiting_price)
async def msg_calc_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)

    uid = message.from_user.id
    text = message.text.replace(",", ".").strip()
    try:
        price = float(text)
        if price <= 0 or price > 100:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите число от 0.01 до 100, например: <b>1.35</b>", parse_mode="HTML")
        return

    await state.clear()
    r = await get_data(uid)
    if not r:
        await message.answer("❌ Кошелёк не настроен. Отправь /start", parse_mode="HTML")
        return
    prefix = f"👛 <i>{wname(uid)}</i>\n\n"
    await message.answer(prefix + fmt_profit(r, price), reply_markup=kb_back(), parse_mode="HTML")


@router.callback_query(F.data == "export")
async def cb_export(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("📄 Генерирую...")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    csv_text = generate_export(r)
    buf = io.BytesIO(csv_text.encode("utf-8-sig"))
    now_str = datetime.now().strftime("%Y%m%d_%H%M")
    doc = BufferedInputFile(buf.getvalue(), filename=f"stars_{wname(uid)}_{now_str}.csv")
    await cb.message.answer_document(doc, caption=f"📄 {wname(uid)} — {len(r.transactions)} сделок")


@router.callback_query(F.data == "refresh")
async def cb_refresh(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("🔄 Обновляю...")
    r = await get_data(uid, force=True)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_stats(r), uid, kb_profit_back())


# ── Charts Handlers ────────────────────────────────────────────────

@router.callback_query(F.data == "charts")
async def cb_charts(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await safe_edit(
        cb.message,
        f"👛 <i>{wname(uid)}</i>\n\n"
        "╔══════════════════════════════╗\n"
        "║      📊 <b>ГРАФИКИ</b>             ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        "Выберите график:",
        kb_charts(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("📊 Генерирую...")

    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return

    parts = cb.data.split(":")
    chart_type = parts[1]

    if chart_type == "profit":
        sp = float(parts[2])
        img_data = chart_daily_profit(r, sp)
        caption = f"👛 {wname(uid)} — Прибыль по дням ({sp}₽/⭐)"
    elif chart_type == "rate":
        img_data = chart_rate_history(r)
        caption = f"👛 {wname(uid)} — Курс за 1⭐ (₽)"
    elif chart_type == "volume":
        img_data = chart_volume(r)
        caption = f"👛 {wname(uid)} — Объём покупок по дням"
    elif chart_type == "cumul":
        sp = float(parts[2])
        img_data = chart_cumulative(r, sp)
        caption = f"👛 {wname(uid)} — Накопительная прибыль ({sp}₽/⭐)"
    else:
        await cb.answer("Неизвестный тип графика", show_alert=True)
        return

    if not img_data:
        await cb.answer("Нет данных для графика", show_alert=True)
        return

    photo = BufferedInputFile(img_data, filename="chart.png")
    await cb.message.answer_photo(photo, caption=caption, reply_markup=kb_back())


# ── Compare Wallets Handler ────────────────────────────────────────

@router.callback_query(F.data == "cmp_wallets")
async def cb_cmp_wallets(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    await cb.answer("⏳ Загружаю...")

    reports = {}
    for key, w in WALLETS.items():
        addr = w.get("address")
        if not addr:
            continue
        reports[key] = await load_data(addr)

    if len(reports) < 2:
        await safe_edit(cb.message, "Нужно минимум 2 кошелька для сравнения.", kb_back())
        return

    text = fmt_compare_wallets(reports, SELL_PRICE_1)
    await safe_edit(cb.message, text, kb_back())


# ── Best/Worst Handler ────────────────────────────────────────────

@router.callback_query(F.data == "bestworst")
async def cb_bestworst(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_bestworst(r, SELL_PRICE_1), uid, kb_back())


# ── Forecast Handler ──────────────────────────────────────────────

@router.callback_query(F.data == "forecast")
async def cb_forecast(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_forecast(r, SELL_PRICE_1, 30), uid, kb_forecast())


@router.callback_query(F.data.startswith("fc:"))
async def cb_forecast_period(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    days = int(cb.data.split(":")[1])
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_forecast(r, SELL_PRICE_1, days), uid, kb_forecast())


# ── Spread Handler ────────────────────────────────────────────────

@router.callback_query(F.data == "spread")
async def cb_spread(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    await safe_edit_w(cb.message, fmt_spread(r), uid, kb_back())


# ── Commission Handler ────────────────────────────────────────────

@router.callback_query(F.data == "commission")
async def cb_commission(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"💸 {SELL_PRICE_1}₽", callback_data=f"comm:{SELL_PRICE_1}"),
            InlineKeyboardButton(text=f"💸 {SELL_PRICE_2}₽", callback_data=f"comm:{SELL_PRICE_2}"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])
    await safe_edit_w(cb.message, fmt_commission(r, SELL_PRICE_1), uid, kb)


@router.callback_query(F.data.startswith("comm:"))
async def cb_commission_price(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    sp = float(cb.data.split(":")[1])
    await cb.answer("⏳")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"💸 {SELL_PRICE_1}₽", callback_data=f"comm:{SELL_PRICE_1}"),
            InlineKeyboardButton(text=f"💸 {SELL_PRICE_2}₽", callback_data=f"comm:{SELL_PRICE_2}"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])
    await safe_edit_w(cb.message, fmt_commission(r, sp), uid, kb)


# ── Goals Handlers ────────────────────────────────────────────────

@router.callback_query(F.data == "goals")
async def cb_goals(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("⏳")

    reports = {}
    for key, w in WALLETS.items():
        addr = w.get("address")
        if addr:
            reports[key] = await load_data(addr)

    text = fmt_goals(uid, reports, SELL_PRICE_1)
    await safe_edit(cb.message, text, kb_goals(uid))


@router.callback_query(F.data.startswith("goal:"))
async def cb_goal_action(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    action = cb.data.split(":")[1]

    if action == "clear":
        _goals[:] = [g for g in _goals if g["user_id"] != uid]
        save_goals()
        await cb.answer("✅ Цели удалены")
        await safe_edit(cb.message, "✅ Все цели удалены.", kb_goals(uid))
        return

    if action in ("stars", "profit"):
        label = "звёздам" if action == "stars" else "прибыли"
        example = "100000" if action == "stars" else "50000"
        await state.set_state(GoalState.waiting_value)
        await state.update_data(goal_type=action)
        await safe_edit(
            cb.message,
            "╔══════════════════════════════╗\n"
            "║       🎯 <b>НОВАЯ ЦЕЛЬ</b>          ║\n"
            "╚══════════════════════════════╝\n"
            f"\nЦель по <b>{label}</b>\n"
            f"\nВведите целевое значение\n"
            f"(например: <b>{example}</b>):",
            kb_back(),
        )
        await cb.answer()


@router.message(GoalState.waiting_value)
async def msg_goal_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)

    uid = message.from_user.id
    text = message.text.replace(",", ".").replace(" ", "").strip()
    try:
        value = float(text)
        if value <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите положительное число.", parse_mode="HTML")
        return

    data = await state.get_data()
    goal_type = data.get("goal_type", "stars")
    await state.clear()

    # Remove existing goal of same type for this user
    _goals[:] = [g for g in _goals if not (g["user_id"] == uid and g["type"] == goal_type)]
    _goals.append({
        "user_id": uid,
        "type": goal_type,
        "target": value,
    })
    save_goals()

    label = "звёздам" if goal_type == "stars" else "прибыли"
    unit = "⭐" if goal_type == "stars" else "₽"
    await message.answer(
        f"✅ Цель установлена!\n\n"
        f"🎯 По {label}: <b>{value:,.0f}{unit}</b>",
        reply_markup=kb_back(),
        parse_mode="HTML",
    )


# ── PDF Handler ───────────────────────────────────────────────────

@router.callback_query(F.data == "pdf")
async def cb_pdf(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    await cb.answer("📑 Генерирую PDF...")
    r = await get_data(uid)
    if not r:
        await safe_edit(cb.message, "❌ Кошелёк не настроен.", kb_back())
        return
    pdf_data = generate_pdf_report(r, SELL_PRICE_1, SELL_PRICE_2)
    if not pdf_data:
        await cb.answer("Нет данных для PDF", show_alert=True)
        return
    now_str = datetime.now().strftime("%Y%m%d_%H%M")
    doc = BufferedInputFile(pdf_data, filename=f"report_{wname(uid)}_{now_str}.pdf")
    await cb.message.answer_document(doc, caption=f"📑 {wname(uid)} — Полный PDF-отчёт")


# ── Auto-Report Handler ──────────────────────────────────────────

@router.callback_query(F.data == "autoreport")
async def cb_autoreport(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)

    enabled = _autoreport.get("enabled", False)
    status = "✅ Включён" if enabled else "❌ Выключен"
    toggle_text = "🔴 Выключить" if enabled else "🟢 Включить"

    text = (
        "╔══════════════════════════════╗\n"
        "║    📬 <b>АВТО-ОТЧЁТ</b>            ║\n"
        "╚══════════════════════════════╝\n"
        f"\nСтатус: <b>{status}</b>\n"
        "\nЕженедельная сводка по всем кошелькам.\n"
        "Отправляется в понедельник утром."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle_text, callback_data="autoreport:toggle")],
        [InlineKeyboardButton(text="📬 Отправить сейчас", callback_data="autoreport:now")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])
    await safe_edit(cb.message, text, kb)
    await cb.answer()


@router.callback_query(F.data == "autoreport:toggle")
async def cb_autoreport_toggle(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)

    _autoreport["enabled"] = not _autoreport.get("enabled", False)
    save_autoreport()
    status = "включён ✅" if _autoreport["enabled"] else "выключен ❌"
    await cb.answer(f"Авто-отчёт {status}")

    # Re-render
    enabled = _autoreport["enabled"]
    toggle_text = "🔴 Выключить" if enabled else "🟢 Включить"
    text = (
        "╔══════════════════════════════╗\n"
        "║    📬 <b>АВТО-ОТЧЁТ</b>            ║\n"
        "╚══════════════════════════════╝\n"
        f"\nСтатус: <b>{'✅ Включён' if enabled else '❌ Выключен'}</b>\n"
        "\nЕженедельная сводка по всем кошелькам.\n"
        "Отправляется в понедельник утром."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle_text, callback_data="autoreport:toggle")],
        [InlineKeyboardButton(text="📬 Отправить сейчас", callback_data="autoreport:now")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="main")],
    ])
    await safe_edit(cb.message, text, kb)


@router.callback_query(F.data == "autoreport:now")
async def cb_autoreport_now(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    await cb.answer("📬 Отправляю отчёт...")

    reports = {}
    for key, w in WALLETS.items():
        addr = w.get("address")
        if addr:
            reports[key] = await load_data(addr, force=True)

    if not reports:
        await cb.answer("Нет данных", show_alert=True)
        return

    text = fmt_autoreport_summary(reports, SELL_PRICE_1, SELL_PRICE_2)
    for uid in ADMIN_IDS:
        try:
            await bot.send_message(uid, text, parse_mode="HTML")
        except Exception:
            pass


# ── Alerts Handlers ────────────────────────────────────────────────

@router.callback_query(F.data == "alerts")
async def cb_alerts(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id

    ton_rub = await _get_current_rate()
    my_alerts = [a for a in _rate_alerts if a["user_id"] == uid]

    alerts_text = ""
    if my_alerts:
        alerts_text = "\n📋 Активные алерты:\n"
        for i, a in enumerate(my_alerts, 1):
            direction = "⬆️ Выше" if a["direction"] == "above" else "⬇️ Ниже"
            alerts_text += f"  {i}. {direction} {a['value']:.2f}₽\n"
    else:
        alerts_text = "\nНет активных алертов.\n"

    text = (
        "╔══════════════════════════════╗\n"
        "║     🔔 <b>АЛЕРТЫ КУРСА TON</b>     ║\n"
        "╚══════════════════════════════╝\n"
        f"\n💱 Текущий курс: <b>{N(ton_rub)} ₽</b>\n"
        f"{alerts_text}"
    )

    await safe_edit(cb.message, text, kb_alerts(uid))
    await cb.answer()


@router.callback_query(F.data.startswith("alert:"))
async def cb_alert_action(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await deny(cb)
    uid = cb.from_user.id
    action = cb.data.split(":")[1]

    if action == "clear":
        _rate_alerts[:] = [a for a in _rate_alerts if a["user_id"] != uid]
        save_alerts()
        await cb.answer("✅ Алерты удалены")
        ton_rub = await _get_current_rate()
        text = (
            "╔══════════════════════════════╗\n"
            "║     🔔 <b>АЛЕРТЫ КУРСА TON</b>     ║\n"
            "╚══════════════════════════════╝\n"
            f"\n💱 Текущий курс: <b>{N(ton_rub)} ₽</b>\n"
            "\nНет активных алертов.\n"
        )
        await safe_edit(cb.message, text, kb_alerts(uid))
        return

    if action in ("above", "below"):
        direction_text = "выше" if action == "above" else "ниже"
        await state.set_state(AlertState.waiting_value)
        await state.update_data(alert_direction=action)
        await safe_edit(
            cb.message,
            "╔══════════════════════════════╗\n"
            "║     🔔 <b>НОВЫЙ АЛЕРТ</b>          ║\n"
            "╚══════════════════════════════╝\n"
            f"\nУведомить когда TON/RUB <b>{direction_text}</b>\n"
            f"\nВведите значение в рублях\n"
            f"(например: <b>350</b>):",
            kb_back(),
        )
        await cb.answer()


@router.message(AlertState.waiting_value)
async def msg_alert_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await deny(message)

    uid = message.from_user.id
    text = message.text.replace(",", ".").strip()
    try:
        value = float(text)
        if value <= 0 or value > 100000:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите число, например: <b>350</b>", parse_mode="HTML")
        return

    data = await state.get_data()
    direction = data.get("alert_direction", "above")
    await state.clear()

    _rate_alerts.append({
        "user_id": uid,
        "direction": direction,
        "value": value,
    })
    save_alerts()

    direction_text = "вырастет выше" if direction == "above" else "упадёт ниже"
    emoji = "⬆️" if direction == "above" else "⬇️"
    await message.answer(
        f"✅ Алерт установлен!\n\n"
        f"{emoji} Уведомлю когда TON/RUB {direction_text} <b>{value:.2f}₽</b>",
        reply_markup=kb_back(),
        parse_mode="HTML",
    )


# ── Background Tasks ──────────────────────────────────────────────

async def _monitor_loop():
    """Check wallets for new transactions every 5 minutes."""
    await asyncio.sleep(30)
    while True:
        try:
            for key, w in WALLETS.items():
                addr = w.get("address")
                if not addr:
                    continue

                report = await load_data(addr, force=True)
                current_ids = {t.event_id for t in report.transactions}

                if addr not in _monitor_initialized:
                    _last_tx_ids[addr] = current_ids
                    _monitor_initialized.add(addr)
                    continue

                new_ids = current_ids - _last_tx_ids.get(addr, set())
                if new_ids:
                    new_txs = sorted(
                        [t for t in report.transactions if t.event_id in new_ids],
                        key=lambda t: t.timestamp,
                    )
                    for t in new_txs:
                        cost_rub = t.cost_rub(report.rates.ton_rub)
                        p1 = t.profit_rub(report.rates.ton_rub, SELL_PRICE_1)
                        text = (
                            f"🔔 Новая сделка!\n"
                            f"👛 {w['name']}\n\n"
                            f"⭐ {t.stars:,} звёзд\n"
                            f"💎 {t.ton_amount:.4f} TON\n"
                            f"💳 {cost_rub:.2f} ₽\n"
                            f"{'🟢' if p1 >= 0 else '🔴'} Прибыль ({SELL_PRICE_1}₽): {p1:.2f}₽\n"
                            f"📅 {t.date_str}"
                        )
                        for uid in ADMIN_IDS:
                            try:
                                await bot.send_message(uid, text, parse_mode=None)
                            except Exception:
                                pass

                _last_tx_ids[addr] = current_ids
        except Exception:
            pass

        await asyncio.sleep(300)


async def _alert_loop():
    """Check TON/RUB rate for alerts every 5 minutes."""
    await asyncio.sleep(60)
    while True:
        try:
            if _rate_alerts:
                connector = aiohttp.TCPConnector(ssl=_ssl_context())
                async with aiohttp.ClientSession(connector=connector) as session:
                    rates = await fetch_rates(session)

                ton_rub = rates.ton_rub
                if ton_rub > 0:
                    triggered = []
                    for alert in _rate_alerts:
                        if alert["direction"] == "above" and ton_rub >= alert["value"]:
                            triggered.append(alert)
                        elif alert["direction"] == "below" and ton_rub <= alert["value"]:
                            triggered.append(alert)

                    for alert in triggered:
                        _rate_alerts.remove(alert)
                        direction_text = "вырос выше" if alert["direction"] == "above" else "упал ниже"
                        emoji = "📈" if alert["direction"] == "above" else "📉"
                        text = (
                            f"{emoji} Алерт курса TON!\n\n"
                            f"Курс TON/RUB {direction_text} {alert['value']:.2f}₽\n"
                            f"💱 Текущий курс: {ton_rub:.2f}₽"
                        )
                        try:
                            await bot.send_message(alert["user_id"], text, parse_mode=None)
                        except Exception:
                            pass

                    if triggered:
                        save_alerts()
        except Exception:
            pass

        await asyncio.sleep(300)


async def _autoreport_loop():
    """Send weekly summary on Mondays at ~10:00 MSK."""
    await asyncio.sleep(180)
    while True:
        try:
            if _autoreport.get("enabled"):
                # MSK = UTC+3
                msk = timezone(timedelta(hours=3))
                now = datetime.now(tz=msk)
                week_key = now.strftime("%Y-W%W")

                if now.weekday() == 0 and now.hour >= 10 and _autoreport.get("last_sent_week") != week_key:
                    reports = {}
                    for key, w in WALLETS.items():
                        addr = w.get("address")
                        if addr:
                            reports[key] = await load_data(addr, force=True)

                    if reports:
                        text = fmt_autoreport_summary(reports, SELL_PRICE_1, SELL_PRICE_2)
                        for uid in ADMIN_IDS:
                            try:
                                await bot.send_message(uid, text, parse_mode="HTML")
                            except Exception:
                                pass

                    _autoreport["last_sent_week"] = week_key
                    save_autoreport()
        except Exception:
            pass

        await asyncio.sleep(3600)


# ── Entry ──────────────────────────────────────────────────────────

async def main():
    load_wallets()
    load_alerts()
    load_goals()
    load_autoreport()
    asyncio.create_task(_monitor_loop())
    asyncio.create_task(_alert_loop())
    asyncio.create_task(_autoreport_loop())
    print("💎 TON Stars Tracker v5 запущен!", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
