"""
BLUESTAR MERGE v3.0 — Single-file production-grade Streamlit application.

Multi-scanner JSON merge engine with auto-detection, canonical pivot model,
heuristic fallback, and full pipeline diagnostics. Zero hardcoded assets.

Deploy: place this file as `app.py` at repo root and point Streamlit Cloud to it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import time
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Generic, Iterable, Literal, Sequence, TypeVar

import streamlit as st
from pydantic import BaseModel, ConfigDict, Field, field_validator

try:
    from rapidfuzz import fuzz as _rf_fuzz  # type: ignore
    _HAS_RAPIDFUZZ = True
except Exception:  # pragma: no cover
    _HAS_RAPIDFUZZ = False

T = TypeVar("T")

# ════════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════════

_LOG = logging.getLogger("bluestar_merge")
if not _LOG.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    ))
    _LOG.addHandler(_h)
_LOG.setLevel(logging.INFO)


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


def _safe_call(stage: str, code: str, fn: Callable[[], T], default: T) -> tuple[T, Diagnostic | None]:
    """Defensive wrapper — turns any exception into a diagnostic."""
    try:
        return fn(), None
    except Exception as e:
        tb = traceback.format_exc(limit=2)
        return default, Diagnostic(
            stage=stage, severity=Severity.WARNING, code=code,
            message=f"{type(e).__name__}: {e}",
            context={"trace": tb.splitlines()[-3:]},
        )


# ════════════════════════════════════════════════════════════════════════════
# SYMBOL NORMALIZATION — pure heuristics, no asset list
# ════════════════════════════════════════════════════════════════════════════

class AssetClass(str, Enum):
    FOREX = "forex"
    METAL = "metal"
    INDEX = "index"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    UNKNOWN = "unknown"


_FIAT_ISO = frozenset({
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY", "ZAR",
    "MXN", "SGD", "HKD", "CNH", "CNY", "INR", "BRL", "RUB", "ILS", "KRW",
})
_STABLE_QUOTES = frozenset({"USDT", "USDC", "BUSD", "DAI", "TUSD"})

_METAL_HINT = re.compile(r"^(XAU|XAG|XPT|XPD|GOLD|SILVER|PLAT)", re.I)
_INDEX_HINT = re.compile(r"^(US\d+|SPX|NDX|DAX|FTSE|NIKKEI|HSI|ASX|UK\d+|GER\d+|JP\d+|FRA\d+|EUSTX|VIX|NAS|DOW)", re.I)
_CRYPTO_HINT = re.compile(r"^(BTC|ETH|XRP|LTC|BCH|ADA|SOL|DOT|DOGE|AVAX|MATIC|LINK|UNI|ATOM|BNB|TRX|SHIB)", re.I)
_SEP_RE = re.compile(r"[\s/_\-.:|]+")


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


def normalize_symbol(raw: Any) -> CanonicalSymbol:
    if raw is None:
        return CanonicalSymbol("", "", "", None, AssetClass.UNKNOWN)
    s = str(raw).strip().upper()
    if not s:
        return CanonicalSymbol("", "", "", None, AssetClass.UNKNOWN)

    parts = [p for p in _SEP_RE.split(s) if p]
    if len(parts) >= 2:
        base, quote = parts[0], parts[1]
        return CanonicalSymbol(s, f"{base}/{quote}", base, quote, _classify(base, quote))

    token = parts[0] if parts else s
    # Try stable / crypto quote suffix
    for q in _STABLE_QUOTES:
        if token.endswith(q) and len(token) > len(q):
            base = token[: -len(q)]
            return CanonicalSymbol(s, f"{base}/{q}", base, q, AssetClass.CRYPTO)
    # Try fiat suffix split
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


_TF_ALIAS: dict[str, Timeframe] = {
    "1m": Timeframe.M1, "m1": Timeframe.M1,
    "5m": Timeframe.M5, "m5": Timeframe.M5,
    "15m": Timeframe.M15, "m15": Timeframe.M15,
    "30m": Timeframe.M30, "m30": Timeframe.M30,
    "1h": Timeframe.H1, "h1": Timeframe.H1, "60m": Timeframe.H1, "hourly": Timeframe.H1,
    "4h": Timeframe.H4, "h4": Timeframe.H4, "240m": Timeframe.H4,
    "d": Timeframe.D1, "d1": Timeframe.D1, "daily": Timeframe.D1, "day": Timeframe.D1, "1d": Timeframe.D1,
    "w": Timeframe.W1, "w1": Timeframe.W1, "weekly": Timeframe.W1, "week": Timeframe.W1, "1w": Timeframe.W1,
    "mn": Timeframe.MN, "monthly": Timeframe.MN, "month": Timeframe.MN, "1mn": Timeframe.MN,
}
_TF_EXTRACT_RE = re.compile(
    r"(1m|5m|15m|30m|1h|4h|1d|1w|h1|h4|d1|w1|mn|daily|weekly|monthly|hourly)",
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
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def safe_int(value: Any) -> int:
    f = safe_float(value)
    return int(f) if f is not None else 0


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


_BASE_CFG = ConfigDict(
    extra="ignore",
    validate_assignment=False,
    arbitrary_types_allowed=True,
)


class RSIReading(BaseModel):
    model_config = _BASE_CFG
    timeframe: Timeframe
    value: float | None = None
    divergence: DivergenceKind = DivergenceKind.NONE

    @field_validator("value")
    @classmethod
    def _clip(cls, v: float | None) -> float | None:
        if v is None:
            return None
        if v < 0 or v > 100:
            return None
        return v


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
    biases: dict[str, str] = Field(default_factory=dict)  # tf.value -> bias

    @field_validator("pct")
    @classmethod
    def _clamp(cls, v: int) -> int:
        return max(0, min(100, int(v)))


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
    def from_symbol(cls, sym: CanonicalSymbol) -> "CanonicalAsset":
        return cls(
            symbol=sym.canonical,
            base=sym.base,
            quote=sym.quote,
            asset_class=sym.asset_class,
        )


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
    version: str = "3.0.0"
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
# ADAPTERS
# ════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class AdapterMatch:
    score: float
    reason: str


class ScannerAdapter(ABC):
    name: str = "unknown"

    @abstractmethod
    def detect(self, payload: Any) -> AdapterMatch: ...

    @abstractmethod
    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]: ...


# ---- GPS adapter ------------------------------------------------------------

_MTF_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_MTF_DIR_RE = re.compile(r"\b(bullish|bearish|neutral|range)\b", re.I)

_GPS_TF_KEYS: dict[str, Timeframe] = {
    "M": Timeframe.MN, "Monthly": Timeframe.MN, "MN": Timeframe.MN,
    "W": Timeframe.W1, "Weekly": Timeframe.W1, "W1": Timeframe.W1,
    "D": Timeframe.D1, "Daily": Timeframe.D1, "D1": Timeframe.D1,
    "4H": Timeframe.H4, "H4": Timeframe.H4,
    "1H": Timeframe.H1, "H1": Timeframe.H1,
    "15m": Timeframe.M15, "M15": Timeframe.M15,
}


class GPSAdapter(ScannerAdapter):
    name = "gps"

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

        for i, raw in enumerate(payload):
            if not isinstance(raw, dict):
                res.add(Diagnostic("gps", Severity.WARNING, "skip", "non-dict", {"i": i}))
                continue
            sym_raw = raw.get("Paire") or raw.get("pair") or raw.get("symbol")
            if not sym_raw:
                res.add(Diagnostic("gps", Severity.WARNING, "no_symbol", "missing pair", {"i": i}))
                continue
            sym = normalize_symbol(sym_raw)
            asset = CanonicalAsset.from_symbol(sym)

            pct, direction = self._parse_mtf(raw.get("MTF", ""))
            biases: dict[str, str] = {}
            for k, tf in _GPS_TF_KEYS.items():
                if k in raw and raw[k] is not None:
                    biases[tf.value] = str(raw[k])

            asset.mtf = MTFConsensus(
                pct=pct,
                direction=direction,
                quality=str(raw["Quality"]) if raw.get("Quality") else None,
                nc=safe_int(raw.get("NC")),
                age_d1=safe_int(raw.get("Age D1") or raw.get("AgeD1")),
                atr_h1=safe_float(raw.get("ATR H1")),
                atr_h4=safe_float(raw.get("ATR H4")),
                atr_daily=safe_float(raw.get("ATR Daily") or raw.get("ATR D1")),
                biases=biases,
            )
            asset.provenance.setdefault("gps", []).append("mtf")
            out.append(asset)
        return res

    @staticmethod
    def _parse_mtf(raw: Any) -> tuple[int, Direction]:
        if raw is None:
            return (0, Direction.NEUTRAL)
        s = str(raw)
        pct = 0
        m = _MTF_PCT_RE.search(s)
        if m:
            try:
                pct = max(0, min(100, int(float(m.group(1)))))
            except ValueError:
                pct = 0
        d = _MTF_DIR_RE.search(s)
        direction = Direction.NEUTRAL
        if d:
            t = d.group(1).lower()
            if t == "bullish":
                direction = Direction.BULLISH
            elif t == "bearish":
                direction = Direction.BEARISH
        return (pct, direction)


# ---- RSI adapter ------------------------------------------------------------

_DIV_MAP: dict[str, DivergenceKind] = {
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

        for i, raw in enumerate(items):
            if not isinstance(raw, dict):
                continue
            sym_raw = (raw.get("pair") or raw.get("Devises") or raw.get("Paire")
                       or raw.get("symbol") or raw.get("instrument"))
            if not sym_raw:
                res.add(Diagnostic("rsi", Severity.WARNING, "no_symbol", "missing pair", {"i": i}))
                continue
            sym = normalize_symbol(sym_raw)
            asset = CanonicalAsset.from_symbol(sym)
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
            else:
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

            asset.rsi = readings
            asset.provenance.setdefault("rsi", []).append(f"{len(readings)}tf")
            out.append(asset)
        return res


# ---- S/R adapter ------------------------------------------------------------

_SUP_RE = re.compile(
    r"(SUR\s+support|S\s+proche|support)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)\s*%\)",
    re.I,
)
_RES_RE = re.compile(
    r"(SUR\s+resistance|R\s+proche|resistance)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)\s*%\)",
    re.I,
)
_INTER_RE = re.compile(r"(intermediaire|intermediate|unavailable|indisponible|n/a)", re.I)
_STATUS_COEFF = {"vierge": 1.0, "testee": 0.8, "tested": 0.8, "role reverse": 0.6}


class SRAdapter(ScannerAdapter):
    name = "sr"

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

        for i, raw in enumerate(assets):
            if not isinstance(raw, dict):
                continue
            sym_raw = raw.get("symbol") or raw.get("pair") or raw.get("Paire")
            if not sym_raw:
                res.add(Diagnostic("sr", Severity.WARNING, "no_symbol", "missing", {"i": i}))
                continue
            sym = normalize_symbol(sym_raw)
            asset = CanonicalAsset.from_symbol(sym)
            asset.price_context = self._parse_ctx(raw.get("price_context", ""))
            zones_raw = raw.get("zones", [])
            zones: list[SRZone] = []
            if isinstance(zones_raw, list):
                for z in zones_raw:
                    if not isinstance(z, dict):
                        continue
                    parsed = self._parse_zone(z)
                    if parsed is not None:
                        zones.append(parsed)
            zones.sort(key=lambda z: z.distance_pct)
            asset.zones = zones
            asset.provenance.setdefault("sr", []).append(f"{len(zones)}zones")
            out.append(asset)
        return res

    @staticmethod
    def _parse_ctx(raw: Any) -> PriceContext:
        ctx = PriceContext(raw=str(raw or ""))
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
        status = str(z.get("status", "Unknown"))
        coeff = _STATUS_COEFF.get(status.lower(), 0.8)

        tf_raw = z.get("timeframes", "")
        tf_list: list[Timeframe] = []
        if isinstance(tf_raw, list):
            tf_iter: Iterable[Any] = tf_raw
        else:
            tf_iter = re.split(r"[+,/]", str(tf_raw))
        for tok in tf_iter:
            tf = parse_timeframe(str(tok).strip())
            if tf is not Timeframe.UNKNOWN:
                tf_list.append(tf)

        sig_raw = str(z.get("signal", "")).upper()
        side: Literal["BUY", "SELL", "UNKNOWN"]
        if "BUY" in sig_raw:
            side = "BUY"
        elif "SELL" in sig_raw:
            side = "SELL"
        else:
            side = "UNKNOWN"

        alert_raw = str(z.get("alert", "") or "").upper()
        if "CHAUDE" in alert_raw or "HOT" in alert_raw:
            alert = "ZONE CHAUDE"
        elif "PROCHE" in alert_raw or "NEAR" in alert_raw:
            alert = "Proche"
        else:
            alert = ""

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


# ---- CHoCH adapter ----------------------------------------------------------

class CHoCHAdapter(ScannerAdapter):
    name = "choch"

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
        for i, raw in enumerate(sigs):
            if not isinstance(raw, dict):
                continue
            sym_raw = (raw.get("pair") or raw.get("symbol")
                       or raw.get("pair_oanda") or raw.get("Paire"))
            if not sym_raw:
                res.add(Diagnostic("choch", Severity.WARNING, "no_symbol", "missing", {"i": i}))
                continue
            sym = normalize_symbol(sym_raw)
            asset = by_sym.setdefault(sym.canonical, CanonicalAsset.from_symbol(sym))

            dr = str(raw.get("direction", "")).lower()
            if "bull" in dr:
                direction = Direction.BULLISH
            elif "bear" in dr:
                direction = Direction.BEARISH
            else:
                direction = Direction.NEUTRAL

            ts: datetime | None = None
            ts_raw = raw.get("signal_time") or raw.get("timestamp")
            if ts_raw:
                try:
                    ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    res.add(Diagnostic("choch", Severity.DEBUG, "bad_time",
                                       "unparseable signal_time", {"v": str(ts_raw)[:40]}))

            event = StructureEvent(
                signal_id=str(raw.get("signal_id") or raw.get("id") or f"auto_{i}_{sym.canonical}"),
                kind=str(raw.get("type") or raw.get("kind") or "CHoCH"),
                direction=direction,
                timeframe=parse_timeframe(raw.get("timeframe") or raw.get("tf")),
                level=safe_float(raw.get("level")),
                close_price=safe_float(raw.get("close_price")),
                current_price=safe_float(raw.get("current_price")),
                confluence_score=safe_float(raw.get("confluence_score")),
                status=str(raw.get("status") or "Unknown"),
                signal_time=ts,
                distance_pct=safe_float(raw.get("distance_pct")),
                distance_atr_multiple=safe_float(raw.get("distance_atr_multiple")),
                volatility=str(raw["volatility"]) if raw.get("volatility") else None,
                force=str(raw["force"]) if raw.get("force") else None,
                bb_regime=str(raw["bb_regime"]) if raw.get("bb_regime") else None,
                session=str(raw["session"]) if raw.get("session") else None,
                candles_elapsed=safe_int(raw.get("candles_elapsed")),
            )
            asset.structure_events.append(event)
            asset.provenance.setdefault("choch", []).append(event.signal_id)

        out.extend(by_sym.values())
        return res


# ---- Heuristic fallback ----------------------------------------------------

_SYMBOL_HINTS = ("pair", "symbol", "instrument", "ticker", "devises", "paire", "asset")


def _fuzzy_score(a: str, b: str) -> int:
    if _HAS_RAPIDFUZZ:
        return int(_rf_fuzz.partial_ratio(a.lower(), b.lower()))
    # Fallback: substring containment heuristic
    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return 100
    if a_l in b_l or b_l in a_l:
        return 85
    common = len(set(a_l) & set(b_l))
    return int(100 * common / max(len(set(a_l) | set(b_l)), 1))


def _best_fuzzy_key(keys: Iterable[str], target: str, threshold: int = 75) -> str | None:
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

        for i, raw in enumerate(items):
            if not isinstance(raw, dict):
                continue
            keys = list(raw.keys())
            sym_key: str | None = None
            for hint in _SYMBOL_HINTS:
                k = _best_fuzzy_key(keys, hint, 80)
                if k is not None:
                    sym_key = k
                    break
            if sym_key is None:
                continue
            sym = normalize_symbol(raw[sym_key])
            if not sym.canonical:
                continue
            asset = CanonicalAsset.from_symbol(sym)

            readings: list[RSIReading] = []
            seen: set[Timeframe] = set()
            for k, v in raw.items():
                if not isinstance(k, str) or "rsi" not in k.lower():
                    continue
                if isinstance(v, dict):
                    val = safe_float(v.get("rsi") or v.get("value"))
                    tf = parse_timeframe(k) if parse_timeframe(k) is not Timeframe.UNKNOWN \
                        else parse_timeframe(v.get("tf") or v.get("timeframe"))
                else:
                    val = safe_float(v)
                    tf = parse_timeframe(k)
                if val is None or val < 0 or val > 100 or tf is Timeframe.UNKNOWN or tf in seen:
                    continue
                seen.add(tf)
                readings.append(RSIReading(timeframe=tf, value=val))
            asset.rsi = readings
            asset.provenance.setdefault("heuristic", []).append("introspected")
            out.append(asset)

        res.add(Diagnostic("heuristic", Severity.INFO, "introspected",
                           f"extracted {len(out)} assets"))
        return res


# ════════════════════════════════════════════════════════════════════════════
# REGISTRY
# ════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class DetectionResult:
    adapter: ScannerAdapter | None
    score: float
    reason: str


class ScannerRegistry:
    def __init__(self, adapters: Sequence[ScannerAdapter], fallback: ScannerAdapter | None = None):
        if not adapters:
            raise ValueError("ScannerRegistry requires at least one adapter")
        self._adapters = list(adapters)
        self._fallback = fallback

    def detect(self, payload: Any) -> DetectionResult:
        best = DetectionResult(None, 0.0, "no match")
        for a in self._adapters:
            try:
                m = a.detect(payload)
            except Exception as e:
                _LOG.warning("adapter %s detect raised: %s", a.name, e)
                continue
            if m.score > best.score:
                best = DetectionResult(a, m.score, m.reason)
        if best.score < 0.5 and self._fallback is not None:
            try:
                fb = self._fallback.detect(payload)
            except Exception:
                fb = AdapterMatch(0.4, "fallback")
            if fb.score > best.score:
                return DetectionResult(self._fallback, fb.score, "fallback")
        return best

    def adapt(self, payload: Any) -> tuple[str, Result[list[CanonicalAsset]]]:
        det = self.detect(payload)
        if det.adapter is None:
            r: Result[list[CanonicalAsset]] = Result(value=[])
            r.add(Diagnostic("registry", Severity.ERROR, "no_adapter",
                             "no adapter matched", {"reason": det.reason}))
            return ("unknown", r)
        try:
            result = det.adapter.adapt(payload)
        except Exception as e:
            r = Result(value=[])
            r.add(Diagnostic("registry", Severity.ERROR, "adapter_crash",
                             f"{det.adapter.name} raised: {e}"))
            return (det.adapter.name, r)
        result.add(Diagnostic(
            "registry", Severity.INFO, "selected",
            f"{det.adapter.name} (score={det.score:.2f})",
            {"reason": det.reason},
        ))
        return (det.adapter.name, result)


# ════════════════════════════════════════════════════════════════════════════
# MERGE ENGINE
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
                key = asset.symbol
                if not key:
                    res.add(Diagnostic("merge", Severity.WARNING, "empty_symbol", "dropped"))
                    continue
                if key not in merged:
                    merged[key] = asset.model_copy(deep=True)
                else:
                    self._fold_into(merged[key], asset, res)
                    collisions[key] += 1

        res.add(Diagnostic(
            "merge", Severity.INFO, "summary",
            f"merged {len(merged)} assets",
            {"collisions": dict(collisions)},
        ))
        return res

    @staticmethod
    def _fold_into(
        target: CanonicalAsset,
        source: CanonicalAsset,
        res: Result[dict[str, CanonicalAsset]],
    ) -> None:
        if target.asset_class is AssetClass.UNKNOWN and source.asset_class is not AssetClass.UNKNOWN:
            target.asset_class = source.asset_class
        if not target.base and source.base:
            target.base = source.base
        if not target.quote and source.quote:
            target.quote = source.quote

        existing_tfs = {r.timeframe for r in target.rsi}
        for r in source.rsi:
            if r.timeframe in existing_tfs:
                res.add(Diagnostic("merge", Severity.DEBUG, "rsi_conflict",
                                   f"{r.timeframe.value} duplicate", {"sym": target.symbol}))
                continue
            target.rsi.append(r)
            existing_tfs.add(r.timeframe)

        existing_b = {b.timeframe for b in target.biases}
        for b in source.biases:
            if b.timeframe not in existing_b:
                target.biases.append(b)
                existing_b.add(b.timeframe)

        if target.mtf is None and source.mtf is not None:
            target.mtf = source.mtf
        elif target.mtf is not None and source.mtf is not None and source.mtf.pct > target.mtf.pct:
            target.mtf = source.mtf

        if target.price_context is None:
            target.price_context = source.price_context
        elif (source.price_context and not source.price_context.is_intermediate
              and target.price_context.is_intermediate):
            target.price_context = source.price_context

        existing_z = {(z.side, round(z.level, 5)) for z in target.zones}
        for z in source.zones:
            key = (z.side, round(z.level, 5))
            if key not in existing_z:
                target.zones.append(z)
                existing_z.add(key)
        target.zones.sort(key=lambda z: z.distance_pct)

        existing_e = {e.signal_id for e in target.structure_events}
        for e in source.structure_events:
            if e.signal_id not in existing_e:
                target.structure_events.append(e)
                existing_e.add(e.signal_id)

        for k, v in source.provenance.items():
            target.provenance.setdefault(k, []).extend(v)


# ════════════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ════════════════════════════════════════════════════════════════════════════

_DIR_TOKENS: dict[Direction, tuple[str, ...]] = {
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
                eq = self._enrichment_quality(asset)
                aligned_zones = [
                    z for z in asset.zones
                    if (event.direction is Direction.BULLISH and z.side == "BUY")
                    or (event.direction is Direction.BEARISH and z.side == "SELL")
                ]
                opposite_zones = [
                    z for z in asset.zones
                    if (event.direction is Direction.BULLISH and z.side == "SELL")
                    or (event.direction is Direction.BEARISH and z.side == "BUY")
                ]
                signals.append(EnrichedSignal(
                    event=event,
                    asset=asset,
                    htf_aligned=self._htf_aligned(asset, event),
                    nearest_aligned_zone=aligned_zones[0] if aligned_zones else None,
                    tp_zones=opposite_zones[:3],
                    confluence_total=self._confluence(asset, event),
                    enrichment=eq,
                    warnings=self._warnings(asset, event),
                ))

        res.add(Diagnostic("enrich", Severity.INFO, "summary",
                           f"enriched {len(signals)} signals from {len(assets)} assets"))
        return res

    @staticmethod
    def _htf_aligned(asset: CanonicalAsset, event: StructureEvent) -> bool:
        if asset.mtf is None:
            return False
        bias_h4 = asset.mtf.biases.get(Timeframe.H4.value)
        if not bias_h4:
            return False
        return _direction_from_text(bias_h4) == event.direction

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
        sources = {k for k in asset.provenance.keys() if k != "heuristic"}
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
        if asset.mtf and not (0 <= asset.mtf.pct <= 100):
            w.append(f"mtf_pct out of range: {asset.mtf.pct}")
        for r in asset.rsi:
            if r.value is not None and not (0 <= r.value <= 100):
                w.append(f"rsi {r.timeframe.value} out of range")
        return w


# ════════════════════════════════════════════════════════════════════════════
# CORRELATION
# ════════════════════════════════════════════════════════════════════════════

_QUALITY_RANK = {"A+": 4, "A": 3, "B+": 2, "B": 1}


class CorrelationEngine:
    def build(self, signals: list[EnrichedSignal]) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for s in signals:
            asset = s.asset
            for leg in (asset.base, asset.quote):
                if not leg:
                    continue
                groups[leg].append({
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
                    float(x.get("confluence") or 0.0),
                ),
                reverse=True,
            )
            for leg, items in sorted(groups.items())
            if len(items) >= 2
        }


# ════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
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
    def __init__(
        self,
        registry: ScannerRegistry,
        merger: MergeEngine | None = None,
        enricher: EnrichmentEngine | None = None,
        correlator: CorrelationEngine | None = None,
    ):
        self._registry = registry
        self._merger = merger or MergeEngine()
        self._enricher = enricher or EnrichmentEngine()
        self._correlator = correlator or CorrelationEngine()

    def run(self, files: list[IngestedFile]) -> Result[MergeOutput]:
        t0 = time.perf_counter()
        diags: list[Diagnostic] = []
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

        merged_r = self._merger.merge(partials)
        diags.extend(merged_r.diagnostics)
        assets = merged_r.value or {}

        enriched_r = self._enricher.enrich(assets)
        diags.extend(enriched_r.diagnostics)
        signals = enriched_r.value or []

        groups = self._correlator.build(signals)
        hot = self._hot_zones(assets)
        top = self._top_consensus(assets)

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
        result: Result[MergeOutput] = Result(value=out, diagnostics=diags)
        if not files:
            result.add(Diagnostic("pipeline", Severity.ERROR, "no_input", "no files provided"))
        return result

    @staticmethod
    def _hot_zones(assets: dict[str, CanonicalAsset]) -> list[dict[str, Any]]:
        zones: list[dict[str, Any]] = []
        for sym, asset in assets.items():
            for z in asset.zones:
                if z.distance_pct < 2.0:
                    zones.append({"symbol": sym, **_zone_dict(z)})
        zones.sort(key=lambda x: float(x["distance_pct"]))
        return zones

    @staticmethod
    def _top_consensus(
        assets: dict[str, CanonicalAsset], min_pct: int = 85, top_n: int = 5
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
            return (q, int(e.get("nc") or 0), int(e.get("mtf_pct") or 0))

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
# STREAMLIT INTEGRATION — caching & UI
# ════════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def get_pipeline() -> MergePipeline:
    """Pipeline is stateless & immutable — safe singleton."""
    adapters: list[ScannerAdapter] = [
        GPSAdapter(),
        RSIAdapter(),
        SRAdapter(),
        CHoCHAdapter(),
    ]
    registry = ScannerRegistry(adapters, fallback=HeuristicAdapter())
    return MergePipeline(registry=registry)


@st.cache_data(show_spinner=False, max_entries=64, ttl=3600)
def parse_json_bytes(data: bytes, name: str) -> tuple[Any | None, str | None]:
    """Cache-friendly JSON parsing keyed on raw bytes + name."""
    try:
        payload = json.loads(data.decode("utf-8"))
        return payload, None
    except UnicodeDecodeError:
        try:
            payload = json.loads(data.decode("utf-8-sig"))
            return payload, None
        except Exception as e:
            return None, f"{name}: encoding error ({e})"
    except json.JSONDecodeError as e:
        return None, f"{name}: invalid JSON at line {e.lineno} col {e.colno} ({e.msg})"
    except Exception as e:
        return None, f"{name}: {type(e).__name__}: {e}"


def _files_fingerprint(files: list[tuple[str, bytes]]) -> str:
    h = hashlib.sha256()
    for name, data in sorted(files, key=lambda x: x[0]):
        h.update(name.encode("utf-8"))
        h.update(b"\x00")
        h.update(hashlib.sha256(data).digest())
    return h.hexdigest()


@st.cache_data(show_spinner=False, max_entries=16, ttl=1800)
def run_pipeline_cached(fingerprint: str, files_raw: list[tuple[str, bytes]]) -> dict[str, Any]:
    """
    Cached pipeline run. Key = (fingerprint, files). Returns serializable dict
    to avoid keeping pydantic objects in cache (memory + hashing concerns).
    """
    _ = fingerprint  # used purely for cache key stability
    pipeline = get_pipeline()
    ingested: list[IngestedFile] = []
    parse_errors: list[str] = []
    for name, data in files_raw:
        payload, err = parse_json_bytes(data, name)
        if err is not None:
            parse_errors.append(err)
            continue
        ingested.append(IngestedFile(name=name, payload=payload))

    result = pipeline.run(ingested)
    out_dict: dict[str, Any] = {
        "ok": result.ok,
        "parse_errors": parse_errors,
        "output": result.value.model_dump(mode="json") if result.value else None,
        "diagnostics": [d.to_dict() for d in result.diagnostics],
    }
    return out_dict


# ════════════════════════════════════════════════════════════════════════════
# UI RENDERING
# ════════════════════════════════════════════════════════════════════════════

_SEV_ICON = {
    "critical": "🔴", "error": "🔴", "warning": "🟡", "info": "🔵", "debug": "⚪",
}


def _render_header() -> None:
    st.markdown(
        """
        <div style="background:linear-gradient(135deg,#1B45B4 0%,#0f2d8a 100%);
             color:white;padding:18px 24px;border-radius:10px;margin-bottom:18px">
          <div style="font-family:monospace;font-size:10px;opacity:.65;letter-spacing:2px">
            BLUESTAR SYSTEM · GENERIC MULTI-SCANNER MERGE
          </div>
          <div style="font-family:monospace;font-size:22px;font-weight:700">
            BLUESTAR MERGE <span style="opacity:.6;font-size:14px">v3.0</span>
          </div>
          <div style="font-family:monospace;font-size:11px;opacity:.85">
            Auto-detection · Canonical pivot · Format-agnostic · Heuristic fallback
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metrics(meta: dict[str, Any], hot_count: int) -> None:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Scanners détectés", len(meta.get("scanners_detected", [])))
    c2.metric("Inconnus", meta.get("scanners_unknown", 0))
    c3.metric("Actifs", meta.get("assets_count", 0))
    c4.metric("Signaux", meta.get("signals_count", 0))
    c5.metric("Zones chaudes", hot_count)
    c6.metric("Latence", f"{meta.get('elapsed_ms', 0):.0f} ms")


def _render_signals(signals: list[dict[str, Any]]) -> None:
    if not signals:
        st.info("Aucun signal de structure trouvé dans les fichiers fournis.")
        return
    st.subheader(f"📊 Signaux enrichis ({len(signals)})")
    for s in signals:
        ev = s.get("event", {})
        asset = s.get("asset", {})
        enr = s.get("enrichment", {})
        status = enr.get("status", "empty")
        badge = {"complete": "🟢", "partial": "🟡", "minimal": "🟠", "empty": "🔴"}.get(status, "⚪")
        htf = "✅" if s.get("htf_aligned") else "⚠️"
        nz = s.get("nearest_aligned_zone")
        zone_txt = (f"@ `{nz['level']}` (d={nz['distance_pct']:.2f}%, sc={nz['score']})"
                    if nz else "_no aligned zone_")
        warns = s.get("warnings") or []
        warn_txt = f" ⚡{len(warns)}w" if warns else ""
        st.markdown(
            f"- {badge} `{asset.get('symbol', '?')}` "
            f"[{ev.get('timeframe', '?')}] **{ev.get('direction', '?')}** · "
            f"HTF {htf} · {zone_txt} · "
            f"confluence={s.get('confluence_total', 0)}{warn_txt}"
        )


def _render_top_consensus(top: dict[str, Any]) -> None:
    bull = top.get("top_bullish") or []
    bear = top.get("top_bearish") or []
    if not bull and not bear:
        return
    st.subheader("🏆 Top consensus MTF (≥85%)")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🟢 Bullish**")
        if bull:
            for e in bull:
                st.markdown(f"- `{e['symbol']}` · {e['mtf_pct']}% · Q={e.get('quality') or '?'} · NC={e.get('nc')}")
        else:
            st.markdown("_aucun_")
    with col2:
        st.markdown("**🔴 Bearish**")
        if bear:
            for e in bear:
                st.markdown(f"- `{e['symbol']}` · {e['mtf_pct']}% · Q={e.get('quality') or '?'} · NC={e.get('nc')}")
        else:
            st.markdown("_aucun_")


def _render_hot_zones(hot: list[dict[str, Any]]) -> None:
    if not hot:
        return
    with st.expander(f"🔥 Zones chaudes ({len(hot)})"):
        for z in hot[:50]:
            tags = " ".join(t for t in [
                "W" if z.get("has_weekly") else "",
                "D" if z.get("has_daily") else "",
                "H4" if z.get("has_h4") else "",
            ] if t) or "—"
            st.markdown(
                f"- `{z['symbol']}` {z.get('side', '?')} @ `{z['level']}` "
                f"(d={z['distance_pct']:.2f}%, sc={z.get('weighted_score')}, "
                f"TF={tags}, {z.get('status')}) {z.get('alert') or ''}"
            )


def _render_correlations(groups: dict[str, list[dict[str, Any]]]) -> None:
    if not groups:
        return
    with st.expander(f"🔗 Clusters de corrélation ({len(groups)})"):
        for leg, entries in groups.items():
            dirs = {e["direction"] for e in entries}
            flag = "✅" if len(dirs) == 1 else "⚠️"
            members = " · ".join(f"`{e['symbol']}` {e['direction']}" for e in entries)
            st.markdown(f"**{leg}** {flag} · {members}")


def _render_diagnostics(diags: list[dict[str, Any]]) -> None:
    if not diags:
        return
    err = sum(1 for d in diags if d.get("severity") in ("error", "critical"))
    warn = sum(1 for d in diags if d.get("severity") == "warning")
    info = sum(1 for d in diags if d.get("severity") == "info")
    label = f"🔧 Diagnostics · {len(diags)} total · {err}🔴 · {warn}🟡 · {info}🔵"
    with st.expander(label):
        # Show errors/warnings first
        priority = {"critical": 0, "error": 1, "warning": 2, "info": 3, "debug": 4}
        for d in sorted(diags, key=lambda x: priority.get(x.get("severity", "info"), 9)):
            icon = _SEV_ICON.get(d.get("severity", "info"), "•")
            ctx = d.get("context") or {}
            ctx_txt = f" · `{json.dumps(ctx, ensure_ascii=False)[:120]}`" if ctx else ""
            st.markdown(f"{icon} `[{d.get('stage')}/{d.get('code')}]` {d.get('message')}{ctx_txt}")


def _render_export(payload_dict: dict[str, Any]) -> None:
    try:
        payload = json.dumps(payload_dict, indent=2, ensure_ascii=False, default=_json_default)
    except Exception as e:
        st.error(f"Export JSON impossible: {e}")
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


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    st.set_page_config(
        page_title="BLUESTAR MERGE v3.0",
        page_icon="🔷",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    _render_header()

    with st.sidebar:
        st.markdown("### ⚙️ Pipeline")
        st.caption("Adapters actifs:")
        st.markdown("- `gps` · MTF consensus\n- `rsi` · flat & nested\n"
                    "- `sr` · zones + price context\n- `choch` · structure events\n"
                    "- `heuristic` · fuzzy fallback")
        st.caption(f"RapidFuzz: {'✅ natif' if _HAS_RAPIDFUZZ else '⚠️ fallback Python'}")
        if st.button("🧹 Vider le cache", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("Cache vidé. Rerun…")
            st.rerun()

    uploads = st.file_uploader(
        "Déposez vos scanners JSON (détection automatique)",
        type=["json"],
        accept_multiple_files=True,
        help="Aucun mapping manuel — chaque fichier est identifié par introspection.",
    )

    if not uploads:
        st.info("⬆️ Déposez 1 à N fichiers JSON pour démarrer.")
        return

    files_raw: list[tuple[str, bytes]] = []
    for f in uploads:
        try:
            f.seek(0)
            data = f.read()
            if not isinstance(data, (bytes, bytearray)):
                data = str(data).encode("utf-8")
            files_raw.append((f.name, bytes(data)))
        except Exception as e:
            st.error(f"Lecture impossible de `{f.name}`: {e}")

    if not files_raw:
        return

    run_btn = st.button("🚀 Exécuter le pipeline", type="primary", use_container_width=True)
    if not run_btn:
        st.caption(f"{len(files_raw)} fichier(s) prêt(s).")
        return

    fingerprint = _files_fingerprint(files_raw)
    with st.spinner("Pipeline en cours…"):
        try:
            result = run_pipeline_cached(fingerprint, files_raw)
        except Exception as e:
            st.error(f"Erreur fatale du pipeline: {type(e).__name__}: {e}")
            st.code(traceback.format_exc(), language="python")
            return

    parse_errors = result.get("parse_errors") or []
    if parse_errors:
        st.error("Fichiers JSON invalides :\n" + "\n".join(f"- {e}" for e in parse_errors))

    output = result.get("output")
    diagnostics = result.get("diagnostics") or []

    if output is None:
        st.error("Pipeline en erreur — aucun résultat exploitable.")
        _render_diagnostics(diagnostics)
        return

    meta = output.get("meta", {})
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


if __name__ == "__main__":
    main()
