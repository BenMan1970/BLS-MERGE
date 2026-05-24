"""
BLUESTAR MERGE v3.1 — Production-grade Streamlit application.

Multi-scanner JSON merge engine with auto-detection, canonical pivot model,
heuristic fallback, full pipeline diagnostics, and hardened against malformed
input, DoS, and partial failures.

Architecture:
    Upload → Parse (cached) → Detect (registry) → Adapt → Merge → Enrich
           → Correlate → Render + Export

Safety guarantees:
    - No stage can crash the pipeline (defensive _safe_call boundaries)
    - Hard upper bounds on file count, file size, asset/zone/event counts
    - Deterministic adapter selection (stable scoring tie-break)
    - Pydantic v2 strict validation with clamping rather than crashing
    - Streamlit caching keyed on content fingerprints (deterministic)
    - No shared mutable defaults (pydantic Field(default_factory=...))

Deploy: place this file as `app.py` and point Streamlit to it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Final,
    Generic,
    Iterable,
    Literal,
    Sequence,
    TypeVar,
    cast,
)

import streamlit as st
from pydantic import BaseModel, ConfigDict, Field, field_validator

try:
    from rapidfuzz import fuzz as _rf_fuzz

    _HAS_RAPIDFUZZ: Final[bool] = True
except ImportError:  # pragma: no cover
    _rf_fuzz = None  # type: ignore[assignment]
    _HAS_RAPIDFUZZ = False

T = TypeVar("T")

# ════════════════════════════════════════════════════════════════════════════
# PRODUCTION LIMITS — hard caps to prevent DoS and runaway memory
# ════════════════════════════════════════════════════════════════════════════

MAX_FILES: Final[int] = 32
MAX_FILE_SIZE_BYTES: Final[int] = 25 * 1024 * 1024  # 25 MB per file
MAX_TOTAL_SIZE_BYTES: Final[int] = 100 * 1024 * 1024  # 100 MB combined
MAX_ASSETS: Final[int] = 5_000
MAX_ZONES_PER_ASSET: Final[int] = 64
MAX_EVENTS_PER_ASSET: Final[int] = 128
MAX_RSI_READINGS_PER_ASSET: Final[int] = 16
MAX_BIASES_PER_ASSET: Final[int] = 16
MAX_SIGNALS_OUT: Final[int] = 10_000
MAX_HOT_ZONES_OUT: Final[int] = 500
MAX_CORRELATION_GROUP_SIZE: Final[int] = 50
MAX_PROVENANCE_ENTRIES: Final[int] = 32

SCHEMA_VERSION: Final[str] = "3.1.0"

# ════════════════════════════════════════════════════════════════════════════
# LOGGING — structured, prod-ready
# ════════════════════════════════════════════════════════════════════════════

_LOG = logging.getLogger("bluestar_merge")
if not _LOG.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    _LOG.addHandler(_handler)
_LOG.setLevel(os.environ.get("BLUESTAR_LOG_LEVEL", "INFO").upper())
_LOG.propagate = False


# ════════════════════════════════════════════════════════════════════════════
# DIAGNOSTICS — Result[T] carrier, never raise from pipeline stages
# ════════════════════════════════════════════════════════════════════════════

class Severity(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class Diagnostic:
    stage: str
    severity: Severity
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


@dataclass(slots=True)
class Result(Generic[T]):
    value: T | None
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.value is not None and not self.has(Severity.ERROR, Severity.CRITICAL)

    def has(self, *sev: Severity) -> bool:
        s = set(sev)
        return any(d.severity in s for d in self.diagnostics)

    def add(self, d: Diagnostic) -> None:
        self.diagnostics.append(d)

    def extend(self, diags: Iterable[Diagnostic]) -> None:
        self.diagnostics.extend(diags)


def _safe_call(
    stage: str,
    code: str,
    fn: Callable[[], T],
    default: T,
    severity: Severity = Severity.ERROR,
) -> tuple[T, Diagnostic | None]:
    """Defensive wrapper — converts any exception into a diagnostic."""
    try:
        return fn(), None
    except Exception as e:  # noqa: BLE001 — boundary by design
        tb_lines = traceback.format_exc(limit=3).splitlines()
        _LOG.warning("safe_call boundary caught %s in %s/%s: %s", type(e).__name__, stage, code, e)
        return default, Diagnostic(
            stage=stage,
            severity=severity,
            code=code,
            message=f"{type(e).__name__}: {e}",
            context={"trace_tail": tb_lines[-3:]},
        )


def _is_finite_number(value: Any) -> bool:
    """True iff value is a finite (non-NaN, non-inf) number."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    # NaN != NaN; inf comparison is safe
    return f == f and f not in (float("inf"), float("-inf"))


# ════════════════════════════════════════════════════════════════════════════
# SYMBOL NORMALIZATION
# ════════════════════════════════════════════════════════════════════════════

class AssetClass(str, Enum):
    FOREX = "forex"
    METAL = "metal"
    INDEX = "index"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    UNKNOWN = "unknown"


_FIAT_ISO: Final[frozenset[str]] = frozenset({
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY", "ZAR",
    "MXN", "SGD", "HKD", "CNH", "CNY", "INR", "BRL", "RUB", "ILS", "KRW",
})
_STABLE_QUOTES: Final[frozenset[str]] = frozenset({"USDT", "USDC", "BUSD", "DAI", "TUSD"})

_METAL_HINT: Final[re.Pattern[str]] = re.compile(
    r"^(XAU|XAG|XPT|XPD|GOLD|SILVER|PLAT)", re.I
)
_INDEX_HINT: Final[re.Pattern[str]] = re.compile(
    r"^(US\d+|SPX|NDX|DAX|FTSE|NIKKEI|HSI|ASX|UK\d+|GER\d+|"
    r"JP\d+|FRA\d+|EUSTX|VIX|NAS|DOW)",
    re.I,
)
_CRYPTO_HINT: Final[re.Pattern[str]] = re.compile(
    r"^(BTC|ETH|XRP|LTC|BCH|ADA|SOL|DOT|DOGE|AVAX|MATIC|"
    r"LINK|UNI|ATOM|BNB|TRX|SHIB)",
    re.I,
)
_SEP_RE: Final[re.Pattern[str]] = re.compile(r"[\s/_\-.:|]+")
_MAX_SYMBOL_LEN: Final[int] = 64


@dataclass(frozen=True, slots=True)
class CanonicalSymbol:
    raw: str
    canonical: str
    base: str
    quote: str | None
    asset_class: AssetClass


def _classify(base: str, quote: str | None) -> AssetClass:
    b = (base or "").upper()
    q = (quote or "").upper() if quote else None
    if _METAL_HINT.search(b):
        return AssetClass.METAL
    if _CRYPTO_HINT.match(b) or (q and q in _STABLE_QUOTES):
        return AssetClass.CRYPTO
    if _INDEX_HINT.match(b):
        return AssetClass.INDEX
    if b in _FIAT_ISO and (q is None or q in _FIAT_ISO):
        return AssetClass.FOREX
    if q and q in _FIAT_ISO and len(b) == 3 and b.isalpha():
        return AssetClass.FOREX
    return AssetClass.UNKNOWN


_EMPTY_SYMBOL: Final[CanonicalSymbol] = CanonicalSymbol(
    "", "", "", None, AssetClass.UNKNOWN
)


def normalize_symbol(raw: Any) -> CanonicalSymbol:
    if raw is None:
        return _EMPTY_SYMBOL
    s = str(raw).strip().upper()[:_MAX_SYMBOL_LEN]
    if not s:
        return _EMPTY_SYMBOL

    parts = [p for p in _SEP_RE.split(s) if p]
    if len(parts) >= 2:
        base, quote = parts[0], parts[1]
        return CanonicalSymbol(s, f"{base}/{quote}", base, quote, _classify(base, quote))

    token = parts[0] if parts else s
    for q in _STABLE_QUOTES:
        if token.endswith(q) and len(token) > len(q):
            base = token[: -len(q)]
            return CanonicalSymbol(s, f"{base}/{q}", base, q, AssetClass.CRYPTO)
    for q in _FIAT_ISO:
        if token.endswith(q) and len(token) > len(q):
            base = token[: -len(q)]
            return CanonicalSymbol(s, f"{base}/{q}", base, q, _classify(base, q))

    return CanonicalSymbol(s, token, token, None, _classify(token, None))


# ════════════════════════════════════════════════════════════════════════════
# TIMEFRAMES
# ════════════════════════════════════════════════════════════════════════════

class Timeframe(str, Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN = "MN"
    UNKNOWN = "UNKNOWN"


_TF_ALIAS: Final[dict[str, Timeframe]] = {
    "1m": Timeframe.M1, "m1": Timeframe.M1,
    "5m": Timeframe.M5, "m5": Timeframe.M5,
    "15m": Timeframe.M15, "m15": Timeframe.M15,
    "30m": Timeframe.M30, "m30": Timeframe.M30,
    "1h": Timeframe.H1, "h1": Timeframe.H1, "60m": Timeframe.H1, "hourly": Timeframe.H1,
    "4h": Timeframe.H4, "h4": Timeframe.H4, "240m": Timeframe.H4,
    "d": Timeframe.D1, "d1": Timeframe.D1, "daily": Timeframe.D1,
    "day": Timeframe.D1, "1d": Timeframe.D1,
    "w": Timeframe.W1, "w1": Timeframe.W1, "weekly": Timeframe.W1,
    "week": Timeframe.W1, "1w": Timeframe.W1,
    "mn": Timeframe.MN, "monthly": Timeframe.MN, "month": Timeframe.MN,
    "1mn": Timeframe.MN,
}
_TF_EXTRACT_RE: Final[re.Pattern[str]] = re.compile(
    r"(1m|5m|15m|30m|1h|4h|1d|1w|h1|h4|d1|w1|mn|"
    r"daily|weekly|monthly|hourly)",
    re.I,
)


def parse_timeframe(raw: Any) -> Timeframe:
    if raw is None:
        return Timeframe.UNKNOWN
    s = str(raw).strip().lower()
    if not s:
        return Timeframe.UNKNOWN
    if s in _TF_ALIAS:
        return _TF_ALIAS[s]
    m = _TF_EXTRACT_RE.search(s)
    if m:
        return _TF_ALIAS.get(m.group(1).lower(), Timeframe.UNKNOWN)
    return Timeframe.UNKNOWN


def safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not _is_finite_number(f):
        return None
    return f


def safe_int(value: Any, default: int = 0) -> int:
    f = safe_float(value)
    if f is None:
        return default
    try:
        return int(f)
    except (OverflowError, ValueError):
        return default


def safe_str(value: Any, *, max_len: int = 256) -> str:
    if value is None:
        return ""
    return str(value)[:max_len]


# ════════════════════════════════════════════════════════════════════════════
# CANONICAL MODELS
# ════════════════════════════════════════════════════════════════════════════

class Direction(str, Enum):
    BULLISH = "Bullish"
    BEARISH = "Bearish"
    NEUTRAL = "Neutral"


class DivergenceKind(str, Enum):
    NONE = "None"
    BULL = "Bullish"
    BEAR = "Bearish"


_BASE_CFG: Final[ConfigDict] = ConfigDict(
    extra="ignore",
    validate_assignment=False,
    arbitrary_types_allowed=True,
    str_strip_whitespace=True,
)


class RSIReading(BaseModel):
    model_config = _BASE_CFG
    timeframe: Timeframe
    value: float | None = None
    divergence: DivergenceKind = DivergenceKind.NONE

    @field_validator("value")
    @classmethod
    def _clip(cls, v: float | None) -> float | None:
        if v is None or not _is_finite_number(v):
            return None
        if v < 0.0 or v > 100.0:
            return None
        return float(v)


class TrendBias(BaseModel):
    model_config = _BASE_CFG
    timeframe: Timeframe
    bias: str
    direction: Direction = Direction.NEUTRAL


class SRZone(BaseModel):
    model_config = _BASE_CFG
    side: Literal["BUY", "SELL", "UNKNOWN"] = "UNKNOWN"
    level: float
    score: float = 0.0
    weighted_score: float = 0.0
    status: str = "Unknown"
    distance_pct: float = 999.0
    alert: str = ""
    timeframes: list[Timeframe] = Field(default_factory=list)
    has_weekly: bool = False
    has_daily: bool = False
    has_h4: bool = False


class PriceContext(BaseModel):
    model_config = _BASE_CFG
    raw: str = ""
    support_level: float | None = None
    support_dist_pct: float | None = None
    support_tag: str | None = None
    resistance_level: float | None = None
    resistance_dist_pct: float | None = None
    resistance_tag: str | None = None
    is_intermediate: bool = False


class StructureEvent(BaseModel):
    model_config = _BASE_CFG
    signal_id: str
    kind: str
    direction: Direction
    timeframe: Timeframe
    level: float | None = None
    close_price: float | None = None
    current_price: float | None = None
    confluence_score: float | None = None
    status: str = "Unknown"
    signal_time: datetime | None = None
    distance_pct: float | None = None
    distance_atr_multiple: float | None = None
    volatility: str | None = None
    force: str | None = None
    bb_regime: str | None = None
    session: str | None = None
    candles_elapsed: int = 0


class MTFConsensus(BaseModel):
    model_config = _BASE_CFG
    pct: int = 0
    direction: Direction = Direction.NEUTRAL
    quality: str | None = None
    nc: int = 0
    age_d1: int = 0
    atr_h1: float | None = None
    atr_h4: float | None = None
    atr_daily: float | None = None
    biases: dict[str, str] = Field(default_factory=dict)

    @field_validator("pct", mode="before")
    @classmethod
    def _clamp_pct(cls, v: Any) -> int:
        i = safe_int(v, default=0)
        return max(0, min(100, i))

    @field_validator("nc", "age_d1", mode="before")
    @classmethod
    def _coerce_int(cls, v: Any) -> int:
        return max(0, safe_int(v, default=0))


class CanonicalAsset(BaseModel):
    model_config = _BASE_CFG
    symbol: str
    base: str = ""
    quote: str | None = None
    asset_class: AssetClass = AssetClass.UNKNOWN

    rsi: list[RSIReading] = Field(default_factory=list)
    biases: list[TrendBias] = Field(default_factory=list)
    mtf: MTFConsensus | None = None
    price_context: PriceContext | None = None
    zones: list[SRZone] = Field(default_factory=list)
    structure_events: list[StructureEvent] = Field(default_factory=list)
    provenance: dict[str, list[str]] = Field(default_factory=dict)

    @classmethod
    def from_symbol(cls, sym: CanonicalSymbol) -> CanonicalAsset:
        return cls(
            symbol=sym.canonical,
            base=sym.base,
            quote=sym.quote,
            asset_class=sym.asset_class,
        )

    def add_provenance(self, source: str, tag: str) -> None:
        """Safe provenance accumulation with cap to prevent unbounded growth."""
        bucket = self.provenance.get(source)
        if bucket is None:
            bucket = []
            self.provenance[source] = bucket
        if len(bucket) < MAX_PROVENANCE_ENTRIES:
            bucket.append(tag)


class EnrichmentQuality(BaseModel):
    model_config = _BASE_CFG
    status: Literal["complete", "partial", "minimal", "empty"] = "empty"
    scanners_matched: int = 0
    scanners_total: int = 0


class EnrichedSignal(BaseModel):
    model_config = _BASE_CFG
    event: StructureEvent
    asset: CanonicalAsset
    htf_aligned: bool = False
    nearest_aligned_zone: SRZone | None = None
    tp_zones: list[SRZone] = Field(default_factory=list)
    confluence_total: float = 0.0
    enrichment: EnrichmentQuality = Field(default_factory=EnrichmentQuality)
    warnings: list[str] = Field(default_factory=list)


class MergeMeta(BaseModel):
    model_config = _BASE_CFG
    generated_at: datetime
    version: str = SCHEMA_VERSION
    scanners_detected: list[str] = Field(default_factory=list)
    scanners_unknown: int = 0
    assets_count: int = 0
    signals_count: int = 0
    elapsed_ms: float = 0.0


class MergeOutput(BaseModel):
    model_config = _BASE_CFG
    meta: MergeMeta
    assets: dict[str, CanonicalAsset]
    signals: list[EnrichedSignal]
    correlation_groups: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    hot_zones: list[dict[str, Any]] = Field(default_factory=list)
    top_consensus: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = Field(default_factory=list)


# ════════════════════════════════════════════════════════════════════════════
# ADAPTERS — abstract base + concrete implementations
# ════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class AdapterMatch:
    score: float
    reason: str


class ScannerAdapter(ABC):
    name: str = "unknown"
    priority: int = 0  # tie-breaker; higher wins on equal score

    @abstractmethod
    def detect(self, payload: Any) -> AdapterMatch: ...

    @abstractmethod
    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]: ...


# ---- GPS adapter ------------------------------------------------------------

_MTF_PCT_RE: Final[re.Pattern[str]] = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_MTF_DIR_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(bullish|bearish|neutral|range)\b", re.I
)

_GPS_TF_KEYS: Final[dict[str, Timeframe]] = {
    "M": Timeframe.MN, "Monthly": Timeframe.MN, "MN": Timeframe.MN,
    "W": Timeframe.W1, "Weekly": Timeframe.W1, "W1": Timeframe.W1,
    "D": Timeframe.D1, "Daily": Timeframe.D1, "D1": Timeframe.D1,
    "4H": Timeframe.H4, "H4": Timeframe.H4,
    "1H": Timeframe.H1, "H1": Timeframe.H1,
    "15m": Timeframe.M15, "M15": Timeframe.M15,
}


def _parse_mtf_string(raw: Any) -> tuple[int, Direction]:
    if raw is None:
        return 0, Direction.NEUTRAL
    s = str(raw)
    pct = 0
    m = _MTF_PCT_RE.search(s)
    if m:
        pct = max(0, min(100, safe_int(m.group(1))))
    d = _MTF_DIR_RE.search(s)
    if d:
        t = d.group(1).lower()
        if t == "bullish":
            return pct, Direction.BULLISH
        if t == "bearish":
            return pct, Direction.BEARISH
    return pct, Direction.NEUTRAL


class GPSAdapter(ScannerAdapter):
    name = "gps"
    priority = 10

    def detect(self, payload: Any) -> AdapterMatch:
        if not isinstance(payload, list) or not payload:
            return AdapterMatch(0.0, "not non-empty list")
        sample = payload[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "items not dicts")
        keys = set(sample.keys())
        hits = len({"Paire", "MTF", "Quality"} & keys)
        if hits >= 2:
            return AdapterMatch(0.60 + 0.15 * hits, f"signature {hits}/3")
        return AdapterMatch(0.0, "no GPS signature")

    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, list):
            res.add(Diagnostic("gps", Severity.ERROR, "bad_root", "expected list"))
            return res

        for idx, raw in enumerate(payload):
            if len(out) >= MAX_ASSETS:
                res.add(Diagnostic("gps", Severity.WARNING, "cap_reached",
                                   f"MAX_ASSETS={MAX_ASSETS}"))
                break
            asset = self._build_asset(raw, idx, res)
            if asset is not None:
                out.append(asset)
        return res

    @staticmethod
    def _build_asset(
        raw: Any, idx: int, res: Result[list[CanonicalAsset]]
    ) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            res.add(Diagnostic("gps", Severity.DEBUG, "skip",
                               "non-dict", {"i": idx}))
            return None
        sym_raw = raw.get("Paire") or raw.get("pair") or raw.get("symbol")
        if not sym_raw:
            res.add(Diagnostic("gps", Severity.WARNING, "no_symbol",
                               "missing pair", {"i": idx}))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)

        pct, direction = _parse_mtf_string(raw.get("MTF", ""))
        biases: dict[str, str] = {}
        for k, tf in _GPS_TF_KEYS.items():
            v = raw.get(k)
            if v is not None:
                biases[tf.value] = safe_str(v, max_len=64)

        try:
            asset.mtf = MTFConsensus(
                pct=pct,
                direction=direction,
                quality=safe_str(raw["Quality"], max_len=8) if raw.get("Quality") else None,
                nc=safe_int(raw.get("NC")),
                age_d1=safe_int(raw.get("Age D1") or raw.get("AgeD1")),
                atr_h1=safe_float(raw.get("ATR H1")),
                atr_h4=safe_float(raw.get("ATR H4")),
                atr_daily=safe_float(raw.get("ATR Daily") or raw.get("ATR D1")),
                biases=biases,
            )
        except Exception as e:  # noqa: BLE001
            res.add(Diagnostic("gps", Severity.WARNING, "mtf_invalid",
                               f"{type(e).__name__}: {e}", {"i": idx}))
            return None

        asset.add_provenance("gps", "mtf")
        return asset


# ---- RSI adapter ------------------------------------------------------------

_DIV_MAP: Final[dict[str, DivergenceKind]] = {
    "none": DivergenceKind.NONE, "aucune": DivergenceKind.NONE, "no": DivergenceKind.NONE,
    "bull": DivergenceKind.BULL, "bullish": DivergenceKind.BULL,
    "haussiere": DivergenceKind.BULL, "haussière": DivergenceKind.BULL,
    "bear": DivergenceKind.BEAR, "bearish": DivergenceKind.BEAR,
    "baissiere": DivergenceKind.BEAR, "baissière": DivergenceKind.BEAR,
}


def _norm_div(v: Any) -> DivergenceKind:
    if v is None:
        return DivergenceKind.NONE
    return _DIV_MAP.get(str(v).strip().lower(), DivergenceKind.NONE)


class RSIAdapter(ScannerAdapter):
    name = "rsi"
    priority = 9

    def detect(self, payload: Any) -> AdapterMatch:
        items = self._items(payload)
        if not items:
            return AdapterMatch(0.0, "no items")
        sample = items[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict")
        keys_lc = {k.lower() for k in sample.keys() if isinstance(k, str)}
        if not ({"pair", "devises", "symbol", "instrument", "paire"} & keys_lc):
            return AdapterMatch(0.0, "no symbol key")
        if "timeframes" in keys_lc and isinstance(sample.get("timeframes"), dict):
            return AdapterMatch(0.9, "nested timeframes")
        if any(k.startswith("rsi") for k in keys_lc):
            return AdapterMatch(0.8, "flat rsi fields")
        return AdapterMatch(0.0, "no RSI fields")

    @staticmethod
    def _items(payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for k in ("instruments", "data", "items", "rsi", "assets"):
                v = payload.get(k)
                if isinstance(v, list):
                    return v
        return []

    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        items = self._items(payload)
        if not items:
            res.add(Diagnostic("rsi", Severity.ERROR, "empty", "no instruments"))
            return res

        for idx, raw in enumerate(items):
            if len(out) >= MAX_ASSETS:
                res.add(Diagnostic("rsi", Severity.WARNING, "cap_reached",
                                   f"MAX_ASSETS={MAX_ASSETS}"))
                break
            asset = self._build_asset(raw, idx, res)
            if asset is not None:
                out.append(asset)
        return res

    @staticmethod
    def _build_asset(
        raw: Any, idx: int, res: Result[list[CanonicalAsset]]
    ) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            return None
        sym_raw = (raw.get("pair") or raw.get("Devises") or raw.get("Paire")
                   or raw.get("symbol") or raw.get("instrument"))
        if not sym_raw:
            res.add(Diagnostic("rsi", Severity.WARNING, "no_symbol",
                               "missing pair", {"i": idx}))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        readings = RSIAdapter._extract_readings(raw)
        asset.rsi = readings[:MAX_RSI_READINGS_PER_ASSET]
        asset.add_provenance("rsi", f"{len(asset.rsi)}tf")
        return asset

    @staticmethod
    def _extract_readings(raw: dict[str, Any]) -> list[RSIReading]:
        readings: list[RSIReading] = []
        tfs = raw.get("timeframes")
        if isinstance(tfs, dict):
            for k, v in tfs.items():
                tf = parse_timeframe(k)
                if tf is Timeframe.UNKNOWN or not isinstance(v, dict):
                    continue
                readings.append(RSIReading(
                    timeframe=tf,
                    value=safe_float(v.get("rsi") or v.get("value")),
                    divergence=_norm_div(v.get("div") or v.get("divergence")),
                ))
            return readings

        seen: set[Timeframe] = set()
        for k, v in raw.items():
            if not isinstance(k, str):
                continue
            kl = k.lower()
            if not (kl.startswith("rsi") or kl.startswith("rsi_")):
                continue
            tf = parse_timeframe(kl.replace("rsi", "").lstrip("_"))
            if tf is Timeframe.UNKNOWN or tf in seen:
                continue
            seen.add(tf)
            div_key = next(
                (kk for kk in raw if isinstance(kk, str)
                 and kk.lower().startswith("div")
                 and parse_timeframe(kk) is tf),
                None,
            )
            readings.append(RSIReading(
                timeframe=tf,
                value=safe_float(v),
                divergence=_norm_div(raw.get(div_key) if div_key else None),
            ))
        return readings


# ---- S/R adapter ------------------------------------------------------------

_SUP_RE: Final[re.Pattern[str]] = re.compile(
    r"(SUR\s+support|S\s+proche|support)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)\s*%\)",
    re.I,
)
_RES_RE: Final[re.Pattern[str]] = re.compile(
    r"(SUR\s+resistance|R\s+proche|resistance)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)\s*%\)",
    re.I,
)
_INTER_RE: Final[re.Pattern[str]] = re.compile(
    r"(intermediaire|intermediate|unavailable|indisponible|n/a)", re.I
)
_STATUS_COEFF: Final[dict[str, float]] = {
    "vierge": 1.0,
    "testee": 0.8,
    "tested": 0.8,
    "role reverse": 0.6,
}


class SRAdapter(ScannerAdapter):
    name = "sr"
    priority = 8

    def detect(self, payload: Any) -> AdapterMatch:
        if not isinstance(payload, dict):
            return AdapterMatch(0.0, "not dict")
        assets = payload.get("assets")
        if not isinstance(assets, list) or not assets:
            return AdapterMatch(0.0, "no assets list")
        sample = assets[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict asset")
        keys = set(sample.keys())
        score = 0.0
        if "symbol" in keys:
            score += 0.3
        if "zones" in keys:
            score += 0.45
        if "price_context" in keys or "trends" in keys:
            score += 0.2
        return AdapterMatch(score, "sr signature")

    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, dict):
            res.add(Diagnostic("sr", Severity.ERROR, "bad_root", "expected dict"))
            return res
        assets = payload.get("assets", [])
        if not isinstance(assets, list):
            res.add(Diagnostic("sr", Severity.ERROR, "bad_assets", "assets not list"))
            return res

        for idx, raw in enumerate(assets):
            if len(out) >= MAX_ASSETS:
                res.add(Diagnostic("sr", Severity.WARNING, "cap_reached",
                                   f"MAX_ASSETS={MAX_ASSETS}"))
                break
            asset = self._build_asset(raw, idx, res)
            if asset is not None:
                out.append(asset)
        return res

    @staticmethod
    def _build_asset(
        raw: Any, idx: int, res: Result[list[CanonicalAsset]]
    ) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            return None
        sym_raw = raw.get("symbol") or raw.get("pair") or raw.get("Paire")
        if not sym_raw:
            res.add(Diagnostic("sr", Severity.WARNING, "no_symbol",
                               "missing", {"i": idx}))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        asset.price_context = SRAdapter._parse_ctx(raw.get("price_context", ""))

        zones_raw = raw.get("zones", [])
        zones: list[SRZone] = []
        if isinstance(zones_raw, list):
            for z in zones_raw:
                if len(zones) >= MAX_ZONES_PER_ASSET:
                    break
                if not isinstance(z, dict):
                    continue
                parsed = SRAdapter._parse_zone(z)
                if parsed is not None:
                    zones.append(parsed)
        zones.sort(key=lambda z: z.distance_pct)
        asset.zones = zones
        asset.add_provenance("sr", f"{len(zones)}zones")
        return asset

    @staticmethod
    def _parse_ctx(raw: Any) -> PriceContext:
        ctx = PriceContext(raw=safe_str(raw, max_len=512))
        s = ctx.raw
        if not s or _INTER_RE.search(s):
            ctx.is_intermediate = True
            return ctx
        m = _SUP_RE.search(s)
        if m:
            ctx.support_tag = m.group(1).strip()
            ctx.support_level = safe_float(m.group(2))
            ctx.support_dist_pct = safe_float(m.group(3))
        m = _RES_RE.search(s)
        if m:
            ctx.resistance_tag = m.group(1).strip()
            ctx.resistance_level = safe_float(m.group(2))
            ctx.resistance_dist_pct = safe_float(m.group(3))
        return ctx

    @staticmethod
    def _parse_zone(z: dict[str, Any]) -> SRZone | None:
        level = safe_float(z.get("level"))
        if level is None or level <= 0:
            return None
        score = safe_float(z.get("score")) or 0.0
        dist = safe_float(z.get("distance_pct"))
        dist = 999.0 if dist is None else dist
        status = safe_str(z.get("status", "Unknown"), max_len=32)
        coeff = _STATUS_COEFF.get(status.lower(), 0.8)
        tf_list = SRAdapter._parse_tf_list(z.get("timeframes", ""))
        side = SRAdapter._parse_side(z.get("signal", ""))
        alert = SRAdapter._parse_alert(z.get("alert", ""))

        return SRZone(
            side=side,
            level=round(level, 5),
            score=round(score, 2),
            weighted_score=round(score * coeff, 2),
            status=status,
            distance_pct=round(dist, 3),
            alert=alert,
            timeframes=tf_list,
            has_weekly=Timeframe.W1 in tf_list,
            has_daily=Timeframe.D1 in tf_list,
            has_h4=Timeframe.H4 in tf_list,
        )

    @staticmethod
    def _parse_tf_list(tf_raw: Any) -> list[Timeframe]:
        tf_list: list[Timeframe] = []
        if isinstance(tf_raw, list):
            iterable: Iterable[Any] = tf_raw
        else:
            iterable = re.split(r"[+,/]", str(tf_raw))
        for tok in iterable:
            tf = parse_timeframe(str(tok).strip())
            if tf is not Timeframe.UNKNOWN:
                tf_list.append(tf)
        return tf_list

    @staticmethod
    def _parse_side(raw: Any) -> Literal["BUY", "SELL", "UNKNOWN"]:
        sig = str(raw).upper()
        if "BUY" in sig:
            return "BUY"
        if "SELL" in sig:
            return "SELL"
        return "UNKNOWN"

    @staticmethod
    def _parse_alert(raw: Any) -> str:
        alert = str(raw or "").upper()
        if "CHAUDE" in alert or "HOT" in alert:
            return "ZONE CHAUDE"
        if "PROCHE" in alert or "NEAR" in alert:
            return "Proche"
        return ""


# ---- CHoCH adapter ----------------------------------------------------------

def _parse_iso_datetime(raw: Any) -> datetime | None:
    """Tolerant ISO-8601 parser; returns None on failure."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _parse_direction_text(raw: Any) -> Direction:
    s = str(raw or "").lower()
    if "bull" in s:
        return Direction.BULLISH
    if "bear" in s:
        return Direction.BEARISH
    return Direction.NEUTRAL


class CHoCHAdapter(ScannerAdapter):
    name = "choch"
    priority = 7

    def detect(self, payload: Any) -> AdapterMatch:
        if not isinstance(payload, dict):
            return AdapterMatch(0.0, "not dict")
        sigs = payload.get("signals")
        if not isinstance(sigs, list) or not sigs:
            return AdapterMatch(0.0, "no signals list")
        sample = sigs[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict signal")
        keys = set(sample.keys())
        score = 0.3
        if "type" in keys or "is_choch" in keys or "kind" in keys:
            score += 0.3
        if "direction" in keys:
            score += 0.2
        if "confluence_score" in keys:
            score += 0.2
        return AdapterMatch(min(score, 1.0), "choch signature")

    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, dict):
            res.add(Diagnostic("choch", Severity.ERROR, "bad_root", "expected dict"))
            return res
        sigs = payload.get("signals", [])
        if not isinstance(sigs, list):
            res.add(Diagnostic("choch", Severity.ERROR, "bad_signals", "not list"))
            return res

        by_sym: dict[str, CanonicalAsset] = {}
        for idx, raw in enumerate(sigs):
            self._ingest_signal(raw, idx, by_sym, res)
            if len(by_sym) >= MAX_ASSETS:
                res.add(Diagnostic("choch", Severity.WARNING, "cap_reached",
                                   f"MAX_ASSETS={MAX_ASSETS}"))
                break
        out.extend(by_sym.values())
        return res

    @staticmethod
    def _ingest_signal(
        raw: Any,
        idx: int,
        by_sym: dict[str, CanonicalAsset],
        res: Result[list[CanonicalAsset]],
    ) -> None:
        if not isinstance(raw, dict):
            return
        sym_raw = (raw.get("pair") or raw.get("symbol")
                   or raw.get("pair_oanda") or raw.get("Paire"))
        if not sym_raw:
            res.add(Diagnostic("choch", Severity.WARNING, "no_symbol",
                               "missing", {"i": idx}))
            return
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return
        asset = by_sym.setdefault(sym.canonical, CanonicalAsset.from_symbol(sym))
        if len(asset.structure_events) >= MAX_EVENTS_PER_ASSET:
            return

        event = CHoCHAdapter._build_event(raw, idx, sym.canonical, res)
        if event is None:
            return
        asset.structure_events.append(event)
        asset.add_provenance("choch", event.signal_id)

    @staticmethod
    def _build_event(
        raw: dict[str, Any],
        idx: int,
        symbol: str,
        res: Result[list[CanonicalAsset]],
    ) -> StructureEvent | None:
        ts_raw = raw.get("signal_time") or raw.get("timestamp")
        ts = _parse_iso_datetime(ts_raw)
        if ts_raw and ts is None:
            res.add(Diagnostic("choch", Severity.DEBUG, "bad_time",
                               "unparseable signal_time",
                               {"v": safe_str(ts_raw, max_len=40)}))
        try:
            return StructureEvent(
                signal_id=safe_str(
                    raw.get("signal_id") or raw.get("id") or f"auto_{idx}_{symbol}",
                    max_len=128,
                ),
                kind=safe_str(raw.get("type") or raw.get("kind") or "CHoCH", max_len=32),
                direction=_parse_direction_text(raw.get("direction")),
                timeframe=parse_timeframe(raw.get("timeframe") or raw.get("tf")),
                level=safe_float(raw.get("level")),
                close_price=safe_float(raw.get("close_price")),
                current_price=safe_float(raw.get("current_price")),
                confluence_score=safe_float(raw.get("confluence_score")),
                status=safe_str(raw.get("status") or "Unknown", max_len=32),
                signal_time=ts,
                distance_pct=safe_float(raw.get("distance_pct")),
                distance_atr_multiple=safe_float(raw.get("distance_atr_multiple")),
                volatility=safe_str(raw["volatility"], max_len=32) if raw.get("volatility") else None,
                force=safe_str(raw["force"], max_len=32) if raw.get("force") else None,
                bb_regime=safe_str(raw["bb_regime"], max_len=32) if raw.get("bb_regime") else None,
                session=safe_str(raw["session"], max_len=32) if raw.get("session") else None,
                candles_elapsed=safe_int(raw.get("candles_elapsed")),
            )
        except Exception as e:  # noqa: BLE001
            res.add(Diagnostic("choch", Severity.WARNING, "event_invalid",
                               f"{type(e).__name__}: {e}",
                               {"i": idx, "sym": symbol}))
            return None


# ---- Heuristic fallback ----------------------------------------------------

_SYMBOL_HINTS: Final[tuple[str, ...]] = (
    "pair", "symbol", "instrument", "ticker", "devises", "paire", "asset",
)


def _fuzzy_score(a: str, b: str) -> int:
    if _HAS_RAPIDFUZZ and _rf_fuzz is not None:
        return int(_rf_fuzz.partial_ratio(a.lower(), b.lower()))
    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return 100
    if a_l in b_l or b_l in a_l:
        return 85
    set_a, set_b = set(a_l), set(b_l)
    union = set_a | set_b
    if not union:
        return 0
    return int(100 * len(set_a & set_b) / len(union))


def _best_fuzzy_key(
    keys: Iterable[str], target: str, threshold: int = 75
) -> str | None:
    best_k: str | None = None
    best_s = 0
    for k in keys:
        if not isinstance(k, str):
            continue
        s = _fuzzy_score(target, k)
        if s > best_s:
            best_k, best_s = k, s
    return best_k if best_s >= threshold else None


class HeuristicAdapter(ScannerAdapter):
    name = "heuristic"
    priority = 1

    def detect(self, payload: Any) -> AdapterMatch:
        items = self._items(payload)
        if not items:
            return AdapterMatch(0.0, "no items")
        sample = items[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict items")
        keys = list(sample.keys())
        if any(_best_fuzzy_key(keys, h, 80) for h in _SYMBOL_HINTS):
            return AdapterMatch(0.45, "fuzzy symbol key found")
        return AdapterMatch(0.0, "no recognizable symbol")

    @staticmethod
    def _items(payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for v in payload.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v
        return []

    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        items = self._items(payload)
        if not items:
            res.add(Diagnostic("heuristic", Severity.ERROR, "empty", "no items"))
            return res

        for raw in items:
            if len(out) >= MAX_ASSETS:
                break
            asset = self._build_asset(raw)
            if asset is not None:
                out.append(asset)
        res.add(Diagnostic("heuristic", Severity.INFO, "introspected",
                           f"extracted {len(out)} assets"))
        return res

    @staticmethod
    def _build_asset(raw: Any) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            return None
        keys = list(raw.keys())
        sym_key: str | None = None
        for hint in _SYMBOL_HINTS:
            k = _best_fuzzy_key(keys, hint, 80)
            if k is not None:
                sym_key = k
                break
        if sym_key is None:
            return None
        sym = normalize_symbol(raw[sym_key])
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        asset.rsi = HeuristicAdapter._extract_rsi(raw)[:MAX_RSI_READINGS_PER_ASSET]
        asset.add_provenance("heuristic", "introspected")
        return asset

    @staticmethod
    def _extract_rsi(raw: dict[str, Any]) -> list[RSIReading]:
        readings: list[RSIReading] = []
        seen: set[Timeframe] = set()
        for k, v in raw.items():
            if not isinstance(k, str) or "rsi" not in k.lower():
                continue
            if isinstance(v, dict):
                val = safe_float(v.get("rsi") or v.get("value"))
                tf = parse_timeframe(k)
                if tf is Timeframe.UNKNOWN:
                    tf = parse_timeframe(v.get("tf") or v.get("timeframe"))
            else:
                val = safe_float(v)
                tf = parse_timeframe(k)
            if val is None or val < 0 or val > 100:
                continue
            if tf is Timeframe.UNKNOWN or tf in seen:
                continue
            seen.add(tf)
            readings.append(RSIReading(timeframe=tf, value=val))
        return readings


# ════════════════════════════════════════════════════════════════════════════
# REGISTRY
# ════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class DetectionResult:
    adapter: ScannerAdapter | None
    score: float
    reason: str


class ScannerRegistry:
    """Deterministic adapter selection with score + priority tie-break."""

    _FALLBACK_THRESHOLD: Final[float] = 0.5

    def __init__(
        self,
        adapters: Sequence[ScannerAdapter],
        fallback: ScannerAdapter | None = None,
    ) -> None:
        if not adapters:
            raise ValueError("ScannerRegistry requires at least one adapter")
        self._adapters: tuple[ScannerAdapter, ...] = tuple(adapters)
        self._fallback = fallback

    def detect(self, payload: Any) -> DetectionResult:
        best = DetectionResult(None, 0.0, "no match")
        for a in self._adapters:
            m, diag = _safe_call(
                f"registry.detect.{a.name}", "detect_crash",
                lambda adapter=a: adapter.detect(payload),
                AdapterMatch(0.0, "crash"),
                severity=Severity.WARNING,
            )
            if diag is not None:
                _LOG.warning("adapter %s.detect crashed", a.name)
                continue
            # Stable selection: strictly greater score wins;
            # on equal score, higher priority wins (deterministic).
            if m.score > best.score or (
                m.score == best.score
                and best.adapter is not None
                and a.priority > best.adapter.priority
            ):
                best = DetectionResult(a, m.score, m.reason)

        if best.score < self._FALLBACK_THRESHOLD and self._fallback is not None:
            fb, _ = _safe_call(
                "registry.detect.fallback", "fallback_crash",
                lambda: self._fallback.detect(payload) if self._fallback else AdapterMatch(0.0, ""),
                AdapterMatch(0.4, "fallback default"),
                severity=Severity.WARNING,
            )
            if fb.score > best.score:
                return DetectionResult(self._fallback, fb.score, "fallback")
        return best

    def adapt(self, payload: Any) -> tuple[str, Result[list[CanonicalAsset]]]:
        det = self.detect(payload)
        if det.adapter is None:
            r: Result[list[CanonicalAsset]] = Result(value=[])
            r.add(Diagnostic("registry", Severity.ERROR, "no_adapter",
                             "no adapter matched", {"reason": det.reason}))
            return "unknown", r

        adapter = det.adapter
        result, crash_diag = _safe_call(
            f"registry.adapt.{adapter.name}", "adapter_crash",
            lambda: adapter.adapt(payload),
            Result(value=cast(list[CanonicalAsset], [])),
            severity=Severity.ERROR,
        )
        if crash_diag is not None:
            result.add(crash_diag)
            return adapter.name, result

        result.add(Diagnostic(
            "registry", Severity.INFO, "selected",
            f"{adapter.name} (score={det.score:.2f})",
            {"reason": det.reason},
        ))
        return adapter.name, result


# ════════════════════════════════════════════════════════════════════════════
# MERGE ENGINE — decomposed for clarity and safety
# ════════════════════════════════════════════════════════════════════════════

class MergeEngine:
    def merge(
        self, partial_groups: list[list[CanonicalAsset]]
    ) -> Result[dict[str, CanonicalAsset]]:
        merged: dict[str, CanonicalAsset] = {}
        res: Result[dict[str, CanonicalAsset]] = Result(value=merged)
        collisions: dict[str, int] = defaultdict(int)

        for group in partial_groups:
            for asset in group:
                if len(merged) >= MAX_ASSETS and asset.symbol not in merged:
                    res.add(Diagnostic("merge", Severity.WARNING, "cap_reached",
                                       f"MAX_ASSETS={MAX_ASSETS}"))
                    return res
                key = asset.symbol
                if not key:
                    res.add(Diagnostic("merge", Severity.WARNING, "empty_symbol", "dropped"))
                    continue
                existing = merged.get(key)
                if existing is None:
                    merged[key] = asset.model_copy(deep=True)
                else:
                    _, diag = _safe_call(
                        "merge", "fold_crash",
                        lambda t=existing, s=asset: self._fold_into(t, s, res),
                        None,
                        severity=Severity.WARNING,
                    )
                    if diag is not None:
                        res.add(diag)
                    collisions[key] += 1

        res.add(Diagnostic(
            "merge", Severity.INFO, "summary",
            f"merged {len(merged)} assets",
            {"collisions_top": dict(sorted(collisions.items(),
                                           key=lambda kv: -kv[1])[:10])},
        ))
        return res

    @staticmethod
    def _fold_into(
        target: CanonicalAsset,
        source: CanonicalAsset,
        res: Result[dict[str, CanonicalAsset]],
    ) -> None:
        MergeEngine._fold_identity(target, source)
        MergeEngine._fold_rsi(target, source, res)
        MergeEngine._fold_biases(target, source)
        MergeEngine._fold_mtf(target, source)
        MergeEngine._fold_price_context(target, source)
        MergeEngine._fold_zones(target, source)
        MergeEngine._fold_events(target, source)
        MergeEngine._fold_provenance(target, source)

    @staticmethod
    def _fold_identity(target: CanonicalAsset, source: CanonicalAsset) -> None:
        if target.asset_class is AssetClass.UNKNOWN and source.asset_class is not AssetClass.UNKNOWN:
            target.asset_class = source.asset_class
        if not target.base and source.base:
            target.base = source.base
        if not target.quote and source.quote:
            target.quote = source.quote

    @staticmethod
    def _fold_rsi(
        target: CanonicalAsset,
        source: CanonicalAsset,
        res: Result[dict[str, CanonicalAsset]],
    ) -> None:
        existing_tfs = {r.timeframe for r in target.rsi}
        for r in source.rsi:
            if len(target.rsi) >= MAX_RSI_READINGS_PER_ASSET:
                break
            if r.timeframe in existing_tfs:
                res.add(Diagnostic("merge", Severity.DEBUG, "rsi_conflict",
                                   f"{r.timeframe.value} duplicate",
                                   {"sym": target.symbol}))
                continue
            target.rsi.append(r)
            existing_tfs.add(r.timeframe)

    @staticmethod
    def _fold_biases(target: CanonicalAsset, source: CanonicalAsset) -> None:
        existing = {b.timeframe for b in target.biases}
        for b in source.biases:
            if len(target.biases) >= MAX_BIASES_PER_ASSET:
                break
            if b.timeframe not in existing:
                target.biases.append(b)
                existing.add(b.timeframe)

    @staticmethod
    def _fold_mtf(target: CanonicalAsset, source: CanonicalAsset) -> None:
        if source.mtf is None:
            return
        if target.mtf is None or source.mtf.pct > target.mtf.pct:
            target.mtf = source.mtf

    @staticmethod
    def _fold_price_context(target: CanonicalAsset, source: CanonicalAsset) -> None:
        if source.price_context is None:
            return
        if target.price_context is None:
            target.price_context = source.price_context
            return
        if not source.price_context.is_intermediate and target.price_context.is_intermediate:
            target.price_context = source.price_context

    @staticmethod
    def _fold_zones(target: CanonicalAsset, source: CanonicalAsset) -> None:
        existing = {(z.side, round(z.level, 5)) for z in target.zones}
        for z in source.zones:
            if len(target.zones) >= MAX_ZONES_PER_ASSET:
                break
            key = (z.side, round(z.level, 5))
            if key not in existing:
                target.zones.append(z)
                existing.add(key)
        target.zones.sort(key=lambda z: z.distance_pct)

    @staticmethod
    def _fold_events(target: CanonicalAsset, source: CanonicalAsset) -> None:
        existing = {e.signal_id for e in target.structure_events}
        for e in source.structure_events:
            if len(target.structure_events) >= MAX_EVENTS_PER_ASSET:
                break
            if e.signal_id not in existing:
                target.structure_events.append(e)
                existing.add(e.signal_id)

    @staticmethod
    def _fold_provenance(target: CanonicalAsset, source: CanonicalAsset) -> None:
        for k, v in source.provenance.items():
            bucket = target.provenance.setdefault(k, [])
            remaining = MAX_PROVENANCE_ENTRIES - len(bucket)
            if remaining <= 0:
                continue
            bucket.extend(v[:remaining])


# ════════════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ════════════════════════════════════════════════════════════════════════════

_DIR_TOKENS: Final[dict[Direction, tuple[str, ...]]] = {
    Direction.BULLISH: ("bullish", "bull", "haussier", "hausse", "long"),
    Direction.BEARISH: ("bearish", "bear", "baissier", "baisse", "short"),
}


def _direction_from_text(text: str) -> Direction:
    s = (text or "").lower()
    for d, tokens in _DIR_TOKENS.items():
        if any(t in s for t in tokens):
            return d
    return Direction.NEUTRAL


class EnrichmentEngine:
    def enrich(self, assets: dict[str, CanonicalAsset]) -> Result[list[EnrichedSignal]]:
        signals: list[EnrichedSignal] = []
        res: Result[list[EnrichedSignal]] = Result(value=signals)

        for asset in assets.values():
            for event in asset.structure_events:
                if len(signals) >= MAX_SIGNALS_OUT:
                    res.add(Diagnostic("enrich", Severity.WARNING, "cap_reached",
                                       f"MAX_SIGNALS_OUT={MAX_SIGNALS_OUT}"))
                    return res
                signal, diag = _safe_call(
                    "enrich", "signal_build_crash",
                    lambda a=asset, e=event: self._build_signal(a, e),
                    None,
                    severity=Severity.WARNING,
                )
                if diag is not None:
                    res.add(diag)
                if signal is not None:
                    signals.append(signal)

        res.add(Diagnostic("enrich", Severity.INFO, "summary",
                           f"enriched {len(signals)} signals from {len(assets)} assets"))
        return res

    def _build_signal(self, asset: CanonicalAsset, event: StructureEvent) -> EnrichedSignal:
        aligned_zones: list[SRZone] = []
        opposite_zones: list[SRZone] = []
        for z in asset.zones:
            if event.direction is Direction.BULLISH and z.side == "BUY":
                aligned_zones.append(z)
            elif event.direction is Direction.BULLISH and z.side == "SELL":
                opposite_zones.append(z)
            elif event.direction is Direction.BEARISH and z.side == "SELL":
                aligned_zones.append(z)
            elif event.direction is Direction.BEARISH and z.side == "BUY":
                opposite_zones.append(z)

        return EnrichedSignal(
            event=event,
            asset=asset,
            htf_aligned=self._htf_aligned(asset, event),
            nearest_aligned_zone=aligned_zones[0] if aligned_zones else None,
            tp_zones=opposite_zones[:3],
            confluence_total=self._confluence(asset, event),
            enrichment=self._enrichment_quality(asset),
            warnings=self._warnings(asset, event),
        )

    @staticmethod
    def _htf_aligned(asset: CanonicalAsset, event: StructureEvent) -> bool:
        if asset.mtf is None:
            return False
        # Compare against the most relevant HTF bias for the event timeframe.
        candidate_tfs: tuple[Timeframe, ...]
        if event.timeframe in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.M30):
            candidate_tfs = (Timeframe.H4, Timeframe.H1, Timeframe.D1)
        elif event.timeframe is Timeframe.H1:
            candidate_tfs = (Timeframe.H4, Timeframe.D1)
        elif event.timeframe is Timeframe.H4:
            candidate_tfs = (Timeframe.D1, Timeframe.W1)
        else:
            candidate_tfs = (Timeframe.D1, Timeframe.W1)
        for tf in candidate_tfs:
            bias = asset.mtf.biases.get(tf.value)
            if bias and _direction_from_text(bias) == event.direction:
                return True
        return False

    @staticmethod
    def _confluence(asset: CanonicalAsset, event: StructureEvent) -> float:
        total = event.confluence_score or 0.0
        if asset.mtf is not None:
            total += asset.mtf.pct * 0.5
        for z in asset.zones[:3]:
            total += z.weighted_score * 0.1
        return round(total, 2)

    @staticmethod
    def _enrichment_quality(asset: CanonicalAsset) -> EnrichmentQuality:
        sources = {k for k in asset.provenance if k != "heuristic"}
        n = len(sources)
        if n >= 3:
            status: Literal["complete", "partial", "minimal", "empty"] = "complete"
        elif n == 2:
            status = "partial"
        elif n == 1:
            status = "minimal"
        else:
            status = "empty"
        return EnrichmentQuality(status=status, scanners_matched=n, scanners_total=n)

    @staticmethod
    def _warnings(asset: CanonicalAsset, event: StructureEvent) -> list[str]:
        w: list[str] = []
        if event.direction is Direction.NEUTRAL:
            w.append("neutral direction")
        if event.level is not None and event.level <= 0:
            w.append(f"non-positive level: {event.level}")
        if asset.mtf and not 0 <= asset.mtf.pct <= 100:
            w.append(f"mtf_pct out of range: {asset.mtf.pct}")
        for r in asset.rsi:
            if r.value is not None and not 0 <= r.value <= 100:
                w.append(f"rsi {r.timeframe.value} out of range")
        return w


# ════════════════════════════════════════════════════════════════════════════
# CORRELATION
# ════════════════════════════════════════════════════════════════════════════

_QUALITY_RANK: Final[dict[str, int]] = {"A+": 4, "A": 3, "B+": 2, "B": 1}


class CorrelationEngine:
    def build(
        self, signals: list[EnrichedSignal]
    ) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for s in signals:
            asset = s.asset
            for leg in (asset.base, asset.quote):
                if not leg:
                    continue
                bucket = groups[leg]
                if len(bucket) >= MAX_CORRELATION_GROUP_SIZE:
                    continue
                bucket.append({
                    "symbol": asset.symbol,
                    "direction": s.event.direction.value,
                    "kind": s.event.kind,
                    "timeframe": s.event.timeframe.value,
                    "mtf_pct": asset.mtf.pct if asset.mtf else 0,
                    "quality": asset.mtf.quality if asset.mtf else None,
                    "confluence": s.confluence_total,
                })
        return {
            leg: sorted(
                items,
                key=lambda x: (
                    _QUALITY_RANK.get(str(x.get("quality")), 0),
                    safe_float(x.get("confluence")) or 0.0,
                ),
                reverse=True,
            )
            for leg, items in sorted(groups.items())
            if len(items) >= 2
        }


# ════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True, frozen=True)
class IngestedFile:
    name: str
    payload: Any


def _zone_dict(z: SRZone) -> dict[str, Any]:
    return {
        "side": z.side,
        "level": z.level,
        "score": z.score,
        "weighted_score": z.weighted_score,
        "status": z.status,
        "distance_pct": z.distance_pct,
        "alert": z.alert,
        "timeframes": [t.value for t in z.timeframes],
        "has_weekly": z.has_weekly,
        "has_daily": z.has_daily,
        "has_h4": z.has_h4,
    }


class MergePipeline:
    """Orchestrates: detect → adapt → merge → enrich → correlate."""

    def __init__(
        self,
        registry: ScannerRegistry,
        merger: MergeEngine | None = None,
        enricher: EnrichmentEngine | None = None,
        correlator: CorrelationEngine | None = None,
    ) -> None:
        self._registry = registry
        self._merger = merger or MergeEngine()
        self._enricher = enricher or EnrichmentEngine()
        self._correlator = correlator or CorrelationEngine()

    def run(self, files: list[IngestedFile]) -> Result[MergeOutput]:
        t0 = time.perf_counter()
        diags: list[Diagnostic] = []

        if not files:
            res: Result[MergeOutput] = Result(value=None)
            res.add(Diagnostic("pipeline", Severity.ERROR, "no_input", "no files provided"))
            return res

        partials, scanners_detected, unknown_count = self._adapt_phase(files, diags)
        merged_r = self._merger.merge(partials)
        diags.extend(merged_r.diagnostics)
        assets = merged_r.value or {}

        enriched_r = self._enricher.enrich(assets)
        diags.extend(enriched_r.diagnostics)
        signals = enriched_r.value or []

        groups, _ = _safe_call(
            "pipeline.correlate", "correlate_crash",
            lambda: self._correlator.build(signals),
            cast(dict[str, list[dict[str, Any]]], {}),
        )
        hot, _ = _safe_call(
            "pipeline.hot_zones", "hot_zones_crash",
            lambda: self._hot_zones(assets),
            cast(list[dict[str, Any]], []),
        )
        top, _ = _safe_call(
            "pipeline.top_consensus", "top_consensus_crash",
            lambda: self._top_consensus(assets),
            cast(dict[str, list[dict[str, Any]]], {}),
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        out = MergeOutput(
            meta=MergeMeta(
                generated_at=datetime.now(timezone.utc),
                scanners_detected=scanners_detected,
                scanners_unknown=unknown_count,
                assets_count=len(assets),
                signals_count=len(signals),
                elapsed_ms=round(elapsed_ms, 2),
            ),
            assets=assets,
            signals=signals,
            correlation_groups=groups,
            hot_zones=hot,
            top_consensus=top,
            diagnostics=[d.to_dict() for d in diags],
        )
        return Result(value=out, diagnostics=diags)

    def _adapt_phase(
        self,
        files: list[IngestedFile],
        diags: list[Diagnostic],
    ) -> tuple[list[list[CanonicalAsset]], list[str], int]:
        partials: list[list[CanonicalAsset]] = []
        scanners_detected: list[str] = []
        unknown_count = 0
        for f in files:
            name, r = self._registry.adapt(f.payload)
            diags.extend(r.diagnostics)
            if name == "unknown" or not r.value:
                unknown_count += 1
                continue
            scanners_detected.append(f"{f.name}:{name}")
            partials.append(r.value)
        return partials, scanners_detected, unknown_count

    @staticmethod
    def _hot_zones(assets: dict[str, CanonicalAsset]) -> list[dict[str, Any]]:
        zones: list[dict[str, Any]] = []
        for sym, asset in assets.items():
            for z in asset.zones:
                if z.distance_pct < 2.0:
                    zones.append({"symbol": sym, **_zone_dict(z)})
                    if len(zones) >= MAX_HOT_ZONES_OUT * 2:
                        break
        zones.sort(key=lambda x: safe_float(x["distance_pct"]) or 999.0)
        return zones[:MAX_HOT_ZONES_OUT]

    @staticmethod
    def _top_consensus(
        assets: dict[str, CanonicalAsset],
        min_pct: int = 85,
        top_n: int = 5,
    ) -> dict[str, list[dict[str, Any]]]:
        bull: list[dict[str, Any]] = []
        bear: list[dict[str, Any]] = []
        for sym, asset in assets.items():
            if asset.mtf is None or asset.mtf.pct < min_pct:
                continue
            entry = {
                "symbol": sym,
                "mtf_pct": asset.mtf.pct,
                "quality": asset.mtf.quality,
                "nc": asset.mtf.nc,
                "age_d1": asset.mtf.age_d1,
            }
            if asset.mtf.direction is Direction.BULLISH:
                bull.append(entry)
            elif asset.mtf.direction is Direction.BEARISH:
                bear.append(entry)

        def _rank(e: dict[str, Any]) -> tuple[int, int, int]:
            q = _QUALITY_RANK.get(str(e.get("quality")), 0)
            return q, safe_int(e.get("nc")), safe_int(e.get("mtf_pct"))

        bull.sort(key=_rank, reverse=True)
        bear.sort(key=_rank, reverse=True)
        return {"top_bullish": bull[:top_n], "top_bearish": bear[:top_n]}


# ════════════════════════════════════════════════════════════════════════════
# EXPORT
# ════════════════════════════════════════════════════════════════════════════

def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Enum):
        return o.value
    if hasattr(o, "model_dump"):
        return o.model_dump(mode="json")
    raise TypeError(f"Type {type(o).__name__} not serializable")


def export_json(output: MergeOutput, *, indent: int = 2) -> str:
    payload = output.model_dump(mode="json")
    return json.dumps(payload, indent=indent, ensure_ascii=False, default=_json_default)


# ════════════════════════════════════════════════════════════════════════════
# STREAMLIT — caching & UI
# ════════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def get_pipeline() -> MergePipeline:
    """Stateless pipeline — safe singleton across reruns."""
    adapters: list[ScannerAdapter] = [
        GPSAdapter(),
        RSIAdapter(),
        SRAdapter(),
        CHoCHAdapter(),
    ]
    registry = ScannerRegistry(adapters, fallback=HeuristicAdapter())
    return MergePipeline(registry=registry)


@st.cache_data(show_spinner=False, max_entries=128, ttl=3600, persist=False)
def parse_json_bytes(data: bytes, name: str) -> tuple[Any | None, str | None]:
    """Cache-friendly JSON parsing keyed on raw bytes + name."""
    if not data:
        return None, f"{name}: empty file"
    if len(data) > MAX_FILE_SIZE_BYTES:
        return None, f"{name}: file too large ({len(data)} > {MAX_FILE_SIZE_BYTES} bytes)"
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as e:
            return None, f"{name}: encoding error ({e})"
    try:
        return json.loads(text), None
    except json.JSONDecodeError as e:
        return None, f"{name}: invalid JSON at line {e.lineno} col {e.colno} ({e.msg})"
    except (RecursionError, MemoryError) as e:
        return None, f"{name}: resource error ({type(e).__name__})"
    except ValueError as e:
        return None, f"{name}: {type(e).__name__}: {e}"


def _files_fingerprint(files: list[tuple[str, bytes]]) -> str:
    h = hashlib.sha256()
    for name, data in sorted(files, key=lambda x: x[0]):
        h.update(name.encode("utf-8"))
        h.update(b"\x00")
        h.update(len(data).to_bytes(8, "big"))
        h.update(hashlib.sha256(data).digest())
    return h.hexdigest()


@st.cache_data(show_spinner=False, max_entries=16, ttl=1800, persist=False)
def run_pipeline_cached(
    fingerprint: str,
    files_raw: list[tuple[str, bytes]],
) -> dict[str, Any]:
    """
    Cached pipeline run. Key = fingerprint (deterministic, content-derived).
    Returns serializable dict to avoid keeping pydantic objects in cache.
    """
    _ = fingerprint  # explicit cache key for clarity
    pipeline = get_pipeline()
    ingested: list[IngestedFile] = []
    parse_errors: list[str] = []
    for name, data in files_raw:
        payload, err = parse_json_bytes(data, name)
        if err is not None:
            parse_errors.append(err)
            continue
        ingested.append(IngestedFile(name=name, payload=payload))

    result, crash_diag = _safe_call(
        "pipeline", "pipeline_crash",
        lambda: pipeline.run(ingested),
        Result(value=None),
        severity=Severity.CRITICAL,
    )
    if crash_diag is not None:
        result.add(crash_diag)
        _LOG.error("pipeline crashed; degraded result returned")

    return {
        "ok": result.ok,
        "parse_errors": parse_errors,
        "output": result.value.model_dump(mode="json") if result.value else None,
        "diagnostics": [d.to_dict() for d in result.diagnostics],
        "schema_version": SCHEMA_VERSION,
    }


# ════════════════════════════════════════════════════════════════════════════
# UI RENDERING
# ════════════════════════════════════════════════════════════════════════════

_SEV_ICON: Final[dict[str, str]] = {
    "critical": "🔴", "error": "🔴", "warning": "🟡",
    "info": "🔵", "debug": "⚪",
}
_STATUS_BADGE: Final[dict[str, str]] = {
    "complete": "🟢", "partial": "🟡", "minimal": "🟠", "empty": "🔴",
}


def _render_header() -> None:
    st.markdown(
        f"""
        <div style="background:linear-gradient(135deg,#1B45B4 0%,#0f2d8a 100%);
             color:white;padding:18px 24px;border-radius:10px;margin-bottom:18px">
          <div style="font-family:monospace;font-size:10px;opacity:.65;letter-spacing:2px">
            BLUESTAR SYSTEM · GENERIC MULTI-SCANNER MERGE
          </div>
          <div style="font-family:monospace;font-size:22px;font-weight:700">
            BLUESTAR MERGE <span style="opacity:.6;font-size:14px">v{SCHEMA_VERSION}</span>
          </div>
          <div style="font-family:monospace;font-size:11px;opacity:.85">
            Auto-detection · Canonical pivot · Format-agnostic · Heuristic fallback
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metrics(meta: dict[str, Any], hot_count: int) -> None:
    cols = st.columns(6)
    cols[0].metric("Scanners détectés", len(meta.get("scanners_detected", [])))
    cols[1].metric("Inconnus", safe_int(meta.get("scanners_unknown")))
    cols[2].metric("Actifs", safe_int(meta.get("assets_count")))
    cols[3].metric("Signaux", safe_int(meta.get("signals_count")))
    cols[4].metric("Zones chaudes", hot_count)
    cols[5].metric("Latence", f"{safe_float(meta.get('elapsed_ms')) or 0.0:.0f} ms")


def _render_signals(signals: list[dict[str, Any]]) -> None:
    if not signals:
        st.info("Aucun signal de structure trouvé dans les fichiers fournis.")
        return
    st.subheader(f"📊 Signaux enrichis ({len(signals)})")
    for s in signals[:200]:  # UI cap, prevents browser stall
        ev = s.get("event", {}) or {}
        asset = s.get("asset", {}) or {}
        enr = s.get("enrichment", {}) or {}
        status = str(enr.get("status", "empty"))
        badge = _STATUS_BADGE.get(status, "⚪")
        htf = "✅" if s.get("htf_aligned") else "⚠️"
        nz = s.get("nearest_aligned_zone")
        if nz and isinstance(nz, dict):
            zone_txt = (
                f"@ `{nz.get('level')}` "
                f"(d={safe_float(nz.get('distance_pct')) or 0.0:.2f}%, "
                f"sc={nz.get('score')})"
            )
        else:
            zone_txt = "_no aligned zone_"
        warns = s.get("warnings") or []
        warn_txt = f" ⚡{len(warns)}w" if warns else ""
        st.markdown(
            f"- {badge} `{asset.get('symbol', '?')}` "
            f"[{ev.get('timeframe', '?')}] **{ev.get('direction', '?')}** · "
            f"HTF {htf} · {zone_txt} · "
            f"confluence={s.get('confluence_total', 0)}{warn_txt}"
        )
    if len(signals) > 200:
        st.caption(f"_({len(signals) - 200} signaux supplémentaires masqués — exporter le JSON pour la liste complète)_")


def _render_top_consensus(top: dict[str, Any]) -> None:
    bull = top.get("top_bullish") or []
    bear = top.get("top_bearish") or []
    if not bull and not bear:
        return
    st.subheader("🏆 Top consensus MTF (≥85%)")
    col1, col2 = st.columns(2)
    with col1:
        _render_consensus_column("🟢 Bullish", bull)
    with col2:
        _render_consensus_column("🔴 Bearish", bear)


def _render_consensus_column(label: str, entries: list[dict[str, Any]]) -> None:
    st.markdown(f"**{label}**")
    if not entries:
        st.markdown("_aucun_")
        return
    for e in entries:
        symbol = e.get("symbol", "?")
        pct = e.get("mtf_pct", "?")
        quality = e.get("quality") or "?"
        nc = e.get("nc")
        st.markdown(f"- `{symbol}` · {pct}% · Q={quality} · NC={nc}")


def _render_hot_zones(hot: list[dict[str, Any]]) -> None:
    if not hot:
        return
    with st.expander(f"🔥 Zones chaudes ({len(hot)})"):
        for z in hot[:50]:
            tags_parts = [
                t for t in (
                    "W" if z.get("has_weekly") else "",
                    "D" if z.get("has_daily") else "",
                    "H4" if z.get("has_h4") else "",
                ) if t
            ]
            tags = " ".join(tags_parts) or "—"
            dist = safe_float(z.get("distance_pct")) or 0.0
            st.markdown(
                f"- `{z.get('symbol', '?')}` {z.get('side', '?')} @ `{z.get('level')}` "
                f"(d={dist:.2f}%, sc={z.get('weighted_score')}, "
                f"TF={tags}, {z.get('status')}) {z.get('alert') or ''}"
            )


def _render_correlations(groups: dict[str, list[dict[str, Any]]]) -> None:
    if not groups:
        return
    with st.expander(f"🔗 Clusters de corrélation ({len(groups)})"):
        for leg, entries in groups.items():
            dirs = {e.get("direction") for e in entries}
            flag = "✅" if len(dirs) == 1 else "⚠️"
            members = " · ".join(
                f"`{e.get('symbol', '?')}` {e.get('direction', '?')}"
                for e in entries[:20]
            )
            extra = "" if len(entries) <= 20 else f" _(+{len(entries) - 20})_"
            st.markdown(f"**{leg}** {flag} · {members}{extra}")


def _render_diagnostics(diags: list[dict[str, Any]]) -> None:
    if not diags:
        return
    severities = [d.get("severity", "info") for d in diags]
    err = sum(1 for s in severities if s in ("error", "critical"))
    warn = sum(1 for s in severities if s == "warning")
    info = sum(1 for s in severities if s == "info")
    label = f"🔧 Diagnostics · {len(diags)} total · {err}🔴 · {warn}🟡 · {info}🔵"
    with st.expander(label):
        priority = {"critical": 0, "error": 1, "warning": 2, "info": 3, "debug": 4}
        ordered = sorted(diags, key=lambda x: priority.get(x.get("severity", "info"), 9))
        for d in ordered[:500]:
            icon = _SEV_ICON.get(d.get("severity", "info"), "•")
            ctx = d.get("context") or {}
            ctx_txt = ""
            if ctx:
                try:
                    ctx_str = json.dumps(ctx, ensure_ascii=False, default=str)
                    ctx_txt = f" · `{ctx_str[:120]}`"
                except (TypeError, ValueError):
                    ctx_txt = ""
            st.markdown(
                f"{icon} `[{d.get('stage')}/{d.get('code')}]` "
                f"{d.get('message')}{ctx_txt}"
            )
        if len(ordered) > 500:
            st.caption(f"_({len(ordered) - 500} diagnostics supplémentaires masqués)_")


def _render_export(payload_dict: dict[str, Any]) -> None:
    payload, diag = _safe_call(
        "ui.export", "serialize_crash",
        lambda: json.dumps(payload_dict, indent=2, ensure_ascii=False, default=_json_default),
        None,
    )
    if payload is None:
        st.error(f"Export JSON impossible: {diag.message if diag else 'unknown'}")
        return
    fname = f"bluestar_merged_{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json"
    st.download_button(
        "📥 Télécharger merged_pipeline.json",
        data=payload.encode("utf-8"),
        file_name=fname,
        mime="application/json",
        use_container_width=True,
        type="primary",
    )
    with st.expander("Prévisualiser JSON (4000 premiers caractères)"):
        st.code(payload[:4000] + ("\n…" if len(payload) > 4000 else ""), language="json")


def _render_asset_browser(assets: dict[str, Any]) -> None:
    if not assets:
        return
    with st.expander(f"🔍 Explorer actifs canoniques ({len(assets)})"):
        symbols = sorted(assets.keys())
        selected = st.selectbox("Symbole", symbols, key="asset_browser")
        if selected:
            st.json(assets[selected], expanded=False)


# ---- Upload handling -------------------------------------------------------

def _read_uploads(uploads: list[Any]) -> tuple[list[tuple[str, bytes]], list[str]]:
    """Read uploaded files defensively with size/count limits."""
    files_raw: list[tuple[str, bytes]] = []
    errors: list[str] = []
    total_size = 0

    if len(uploads) > MAX_FILES:
        errors.append(f"Trop de fichiers ({len(uploads)} > MAX_FILES={MAX_FILES}); seuls les {MAX_FILES} premiers seront traités.")
        uploads = uploads[:MAX_FILES]

    for f in uploads:
        try:
            f.seek(0)
            data = f.read()
            if not isinstance(data, (bytes, bytearray)):
                data = str(data).encode("utf-8")
            data = bytes(data)
        except Exception as e:  # noqa: BLE001 — IO boundary
            errors.append(f"Lecture impossible de `{getattr(f, 'name', '?')}`: {type(e).__name__}: {e}")
            continue

        size = len(data)
        if size == 0:
            errors.append(f"`{f.name}`: fichier vide ignoré")
            continue
        if size > MAX_FILE_SIZE_BYTES:
            errors.append(
                f"`{f.name}`: fichier trop volumineux ({size} > {MAX_FILE_SIZE_BYTES} octets), ignoré"
            )
            continue
        if total_size + size > MAX_TOTAL_SIZE_BYTES:
            errors.append(
                f"`{f.name}`: dépasserait la limite globale "
                f"({MAX_TOTAL_SIZE_BYTES} octets), ignoré"
            )
            continue

        total_size += size
        files_raw.append((f.name, data))
    return files_raw, errors


def _render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### ⚙️ Pipeline")
        st.caption("Adapters actifs:")
        st.markdown(
            "- `gps` · MTF consensus\n"
            "- `rsi` · flat & nested\n"
            "- `sr` · zones + price context\n"
            "- `choch` · structure events\n"
            "- `heuristic` · fuzzy fallback"
        )
        st.caption(f"RapidFuzz: {'✅ natif' if _HAS_RAPIDFUZZ else '⚠️ fallback Python'}")
        st.caption(f"Schema: `v{SCHEMA_VERSION}`")
        st.caption(
            f"Limits: {MAX_FILES} fichiers · "
            f"{MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB/fichier · "
            f"{MAX_ASSETS} actifs"
        )
        if st.button("🧹 Vider le cache", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("Cache vidé. Rerun…")
            st.rerun()


def _render_results(result: dict[str, Any]) -> None:
    parse_errors = result.get("parse_errors") or []
    if parse_errors:
        st.error("Fichiers JSON invalides :\n" + "\n".join(f"- {e}" for e in parse_errors))

    output = result.get("output")
    diagnostics = result.get("diagnostics") or []

    if output is None:
        st.error("Pipeline en erreur — aucun résultat exploitable.")
        _render_diagnostics(diagnostics)
        return

    meta = output.get("meta", {}) or {}
    hot = output.get("hot_zones") or []
    _render_metrics(meta, len(hot))
    st.divider()
    _render_signals(output.get("signals") or [])
    _render_top_consensus(output.get("top_consensus") or {})
    _render_hot_zones(hot)
    _render_correlations(output.get("correlation_groups") or {})
    _render_asset_browser(output.get("assets") or {})
    _render_diagnostics(diagnostics)
    st.divider()
    _render_export(output)


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    st.set_page_config(
        page_title=f"BLUESTAR MERGE v{SCHEMA_VERSION}",
        page_icon="🔷",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    _render_header()
    _render_sidebar()

    uploads = st.file_uploader(
        "Déposez vos scanners JSON (détection automatique)",
        type=["json"],
        accept_multiple_files=True,
        help=(
            f"Aucun mapping manuel — chaque fichier est identifié par "
            f"introspection. Limite: {MAX_FILES} fichiers, "
            f"{MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB par fichier."
        ),
    )
    if not uploads:
        st.info("⬆️ Déposez 1 à N fichiers JSON pour démarrer.")
        return

    files_raw, read_errors = _read_uploads(uploads)
    for err in read_errors:
        st.warning(err)
    if not files_raw:
        st.error("Aucun fichier lisible.")
        return

    run_btn = st.button("🚀 Exécuter le pipeline", type="primary", use_container_width=True)
    if not run_btn:
        st.caption(f"{len(files_raw)} fichier(s) prêt(s).")
        return

    fingerprint = _files_fingerprint(files_raw)
    with st.spinner("Pipeline en cours…"):
        result, diag = _safe_call(
            "ui.run", "ui_pipeline_crash",
            lambda: run_pipeline_cached(fingerprint, files_raw),
            None,
            severity=Severity.CRITICAL,
        )
    if result is None:
        st.error(f"Erreur fatale du pipeline: {diag.message if diag else 'unknown'}")
        return

    _render_results(result)


if __name__ == "__main__":
    main()
