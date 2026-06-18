# -*- coding: utf-8 -*-
"""
BLUESTAR MERGE v3.5.2 — Production-grade Streamlit application.
Multi-scanner JSON merge engine with auto-detection, canonical pivot model,
heuristic fallback, full pipeline diagnostics, and hardened against malformed
input, DoS, and partial failures.

v3.5.2 — Lint fix (no behaviour change):
    Removed redundant @staticmethod decorator on _fold_current_price()
    (double @staticmethod is a no-op in Python ≥ 3.10 but is misleading).
    Zero regression: AST-verified, no logic modified.

v3.5.1 — Market Context Layer — schema completion (additive, zero regression):
    Completes the market_context schema introduced in v3.5.0 with 5 fields
    that were present in the architectural audit but missing from the initial
    implementation. All additions are pure, derived from already-computed data.
    No existing field is modified. No scoring/conviction/SL/TP impact.

      • New fields in market_context:
          - counter_trend_classification : structured bloc (present, class,
            dominant_tf, aligns_with_divergence, age_d1_modifier_applied,
            description). Derived from events_summary + divergence_ctx.
          - momentum_context.rsi_gradient : "rising"|"falling"|"neutral"|
            "unknown". H1 vs H4 RSI differential (threshold 3 pts).
          - sr_context.key_level_type     : "W1"|"D1"|None. Precision on
            at_key_level when True.
          - sr_context.sr_confluence_with_counter : bool. True if a counter-
            side zone ≤ 2% exists AND counter structure events are present.
          - transition_signals.weakening_trend    : bool. mtf_pct < 70 AND
            no aligned Fresh AND counter present.
          - transition_signals.compression_detected : bool. Projection of
            market_state == RANGE_COMPRESSION.

      • _mc_sr_context signature gains optional counter_present: bool = False.
        Call sites updated. Existing behaviour preserved when False (default).

      • _mc_divergence_context return dict gains rsi_gradient key.

v3.5.0 — Market Context Layer (passive, additive, zero scoring impact):
    Adds a `market_context` block to every CanonicalAsset in the merge output.
    Computed after all existing pre-computations. Strictly read-only: no effect
    on scoring, conviction, SL/TP, rankings, or rendering.

      • CanonicalAsset gains:
          - market_context : dict | None  (None only on internal crash)

      • New pure functions (module-level, no side effects):
          - _build_market_context()
          - _mc_classify_structure_events()
          - _mc_classify_market_state()
          - _mc_build_confidence_drivers()
          - _mc_divergence_context()
          - _mc_sr_context()
          - _mc_age_category()

      • market_context schema:
          - market_state        : 8-state enum (CLEAN_CONTINUATION …
                                  REVERSAL_RISK / RANGE_COMPRESSION /
                                  DATA_INCOMPLETE)
          - structural_risk     : Low | Low-Moderate | Moderate |
                                  Moderate-High | High | Critical | Undefined
          - mtf_alignment       : HTF anchor, aligned TFs, conflict TFs
          - structure_events_summary : aligned/counter counts, escalation
          - momentum_context    : RSI H4 status + divergence confirmed TFs
          - sr_context          : nearest zone proximity (≤ 2%)
          - transition_signals  : age_d1 category, distribution_phase_risk
          - confidence_drivers  : ordered list of explicit string drivers
          - structural_risk_drivers : risk factors list

      • ENGINE_V9 is unmodified. market_context is silently ignored by its
        own CanonicalAsset model (extra="ignore"). Zero regression guaranteed.

      • meta.version bumped to "3.5.0".

v3.4.2 — Directional inference patch (fixed: current_price from asset) (S/R side fix):
    When the scanner SR produces zones with side="UNKNOWN" (e.g. pivot zones
    without explicit BUY/SELL signal), the merger now infers the direction
    from the relative position of the level vs current_price:
      • level < current_price → BUY  (Support)
      • level > current_price → SELL (Resistance)
      • level == current_price → UNKNOWN (zone touched, ambiguous)
    This fixes 9+ assets that had unusable UNKNOWN zones in production.

v3.4.0 — Pre-computation layer for prompt v9.0 (BLUESTAR DIRECT):
    The LLM downstream now receives ALL deterministic arithmetic pre-computed,
    eliminating ~40% of arithmetic ops on the model side and stabilising
    cross-model behaviour. Specifically:

      • CanonicalAsset gains:
          - atr_effective   : float | None  (ATR cascade output)
          - atr_source      : Literal[h4, h1_proxy, d1_proxy, synthetic]
          - conviction_cap  : Literal[A, BBB] | None
          - nearest_aligned_zone : SRZone | None  (real SR preferred)
          - hot_zone_primary     : SRZone | None  (incl. UNKNOWN pivots)

      • CanonicalAsset.rsi_h4_status now uses the 7-level v9.0 scale:
            extreme_overbought | overbought | grey_high | favorable |
            grey_low | oversold | extreme_oversold
        (previously: 5-level). Same scale applied per-TF in rsi_by_tf.

      • EnrichedSignal.precomputed gains a typed sub-model carrying:
          - atr_effective, atr_source
          - bb_mult            (Squeeze=1.0 / Normal=1.5 / Expansion=2.0)
          - sl_distance_min    (= atr_effective × 0.8 — SL floor)
          - sl_distance_raw    (= atr_effective × bb_mult)
          - rsi_h4_value, rsi_h4_status
          - candles_elapsed
          - sig_fresh_aligned  (Fresh + direction match + ≤2 candles)

      • SL / TP1 / RR now use atr_effective (with cascade fallback) instead of
        the raw atr_h1, so signals on assets missing atr_h4 still get usable
        levels with a conviction_cap flagged.

      • meta.version bumped to "3.4.0".

v3.3.1 fixes inherited and preserved:
    - P0: _parse_price_context() regex fallback restored on dict["raw"]
    - P1: synthetic nearest zones tagged "SR_nearest" + abs(distance)
    - P2: _select_nearest_aligned_zone() prefers real SR over synthetic
    - P3: _hot_zones() excludes synthetic/invalid zones

v3.3 fixes inherited and preserved:
    - BUG 1: htf_aligned requires BOTH D1 AND H4 aligned
    - GAP 3: current_price promoted to CanonicalAsset level
    - GAP 4: rsi_by_tf dict + rsi_h4_status pre-computed on CanonicalAsset
    - GAP 6: sl_price / tp1_price / rr_estimated pre-computed on EnrichedSignal
    - GAP 7: nearest_aligned_zone uses 5% threshold + price_context fallback

Deploy: place this file as `app.py` and run `streamlit run app.py`.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import math
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
except ImportError:  # pragma: no cover - optional dep
    _rf_fuzz = None
    _HAS_RAPIDFUZZ = False

T = TypeVar("T")


# ════════════════════════════════════════════════════════════════════════════
# PRODUCTION LIMITS — hard caps to prevent DoS and runaway memory
# ════════════════════════════════════════════════════════════════════════════
MAX_FILES: Final[int] = 32
MAX_FILE_SIZE_BYTES: Final[int] = 25 * 1024 * 1024          # 25 MB / file
MAX_TOTAL_SIZE_BYTES: Final[int] = 100 * 1024 * 1024        # 100 MB combined
MAX_ASSETS: Final[int] = 5_000
MAX_ZONES_PER_ASSET: Final[int] = 64
MAX_EVENTS_PER_ASSET: Final[int] = 128
MAX_RSI_READINGS_PER_ASSET: Final[int] = 16
MAX_BIASES_PER_ASSET: Final[int] = 16
MAX_SIGNALS_OUT: Final[int] = 10_000
MAX_HOT_ZONES_OUT: Final[int] = 500
MAX_CORRELATION_GROUP_SIZE: Final[int] = 50
MAX_PROVENANCE_ENTRIES: Final[int] = 32
MAX_DIAGNOSTICS: Final[int] = 5_000
MAX_TP_ZONES: Final[int] = 3

SCHEMA_VERSION: Final[str] = "3.5.0"

# ── MERGE-2: HTF alignment thresholds (configurable via these constants) ──
# Timeframes considered "high timeframe" for bias alignment.
_HTF_BIAS_TFS: Final[frozenset[str]] = frozenset({"MN", "W1", "D1"})
# Minimum number of HTF timeframes that must agree to declare htf=True.
_HTF_MIN_AGREEMENT: Final[int] = 2

# ── MERGE-6: top_consensus minimum MTF % per direction ────────────────────
# Bullish setups require strong consensus (typically 85%+).
_TOP_CONSENSUS_MIN_PCT_BULL: Final[int] = 85
# Bearish setups in a USD-driven market fragment consensus — lower threshold.
_TOP_CONSENSUS_MIN_PCT_BEAR: Final[int] = 50

# Status values identifying synthetic zones built from price_context fallback.
_SR_NEAREST_STATUS: Final[str] = "SR_nearest"
_INVALID_ZONE_STATUSES: Final[frozenset[str]] = frozenset({
    "Unknown", _SR_NEAREST_STATUS,
})


# ════════════════════════════════════════════════════════════════════════════
# LOGGING — structured, production-ready
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
# DIAGNOSTICS — Result[T] carrier; never raise from pipeline stages
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
        return self.value is not None and not self.has(
            Severity.ERROR, Severity.CRITICAL
        )

    def has(self, *sev: Severity) -> bool:
        s = set(sev)
        return any(d.severity in s for d in self.diagnostics)

    def add(self, d: Diagnostic) -> None:
        if len(self.diagnostics) < MAX_DIAGNOSTICS:
            self.diagnostics.append(d)

    def extend(self, diags: Iterable[Diagnostic]) -> None:
        room = MAX_DIAGNOSTICS - len(self.diagnostics)
        if room <= 0:
            return
        self.diagnostics.extend(list(diags)[:room])


def _safe_call(
    stage: str,
    code: str,
    fn: Callable[[], T],
    default: T,
    severity: Severity = Severity.ERROR,
) -> tuple[T, Diagnostic | None]:
    """Defensive wrapper — converts ANY exception into a structured diagnostic."""
    try:
        return fn(), None
    except Exception as exc:
        tb_lines = traceback.format_exc(limit=4).splitlines()
        _LOG.warning(
            "safe_call boundary: %s in %s/%s: %s",
            type(exc).__name__, stage, code, exc,
        )
        diag = Diagnostic(
            stage=stage,
            severity=severity,
            code=code,
            message=f"{type(exc).__name__}: {exc}",
            context={
                "exception_type": type(exc).__name__,
                "trace_tail": tb_lines[-4:],
            },
        )
        return default, diag


def _is_finite_number(value: Any) -> bool:
    """True iff value is a finite (non-NaN, non-inf) real number."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


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
    "MXN", "SGD", "HKD", "CNH", "CNY", "INR", "BRL", "RUB",
    "ILS", "KRW",
})
_STABLE_QUOTES: Final[frozenset[str]] = frozenset({
    "USDT", "USDC", "BUSD", "DAI", "TUSD",
})
_METAL_HINT: Final[re.Pattern[str]] = re.compile(
    r"^(XAU|XAG|XPT|XPD|GOLD|SILVER|PLAT)", re.I
)
_INDEX_HINT: Final[re.Pattern[str]] = re.compile(
    r"^(US\d+|SPX|NDX|DAX|FTSE|NIKKEI|HSI|ASX|UK\d+|GER\d+|"
    r"JP\d+|FRA\d+|EUSTX|VIX|NAS|DOW|DE\d+)",
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
    if _CRYPTO_HINT.match(b) or (q is not None and q in _STABLE_QUOTES):
        return AssetClass.CRYPTO
    if _INDEX_HINT.match(b):
        return AssetClass.INDEX
    if b in _FIAT_ISO and (q is None or q in _FIAT_ISO):
        return AssetClass.FOREX
    if q is not None and q in _FIAT_ISO and len(b) == 3 and b.isalpha():
        return AssetClass.FOREX
    return AssetClass.UNKNOWN


_EMPTY_SYMBOL: Final[CanonicalSymbol] = CanonicalSymbol(
    "", "", "", None, AssetClass.UNKNOWN
)


def _split_concatenated(token: str) -> tuple[str, str | None]:
    """Best-effort split of a glued symbol like `EURUSD` or `BTCUSDT`."""
    for q in _STABLE_QUOTES:
        if token.endswith(q) and len(token) > len(q):
            return token[: -len(q)], q
    for q in _FIAT_ISO:
        if token.endswith(q) and len(token) > len(q):
            return token[: -len(q)], q
    return token, None


def normalize_symbol(raw: Any) -> CanonicalSymbol:
    if raw is None:
        return _EMPTY_SYMBOL
    s = str(raw).strip().upper()[:_MAX_SYMBOL_LEN]
    if not s:
        return _EMPTY_SYMBOL
    parts = [p for p in _SEP_RE.split(s) if p]
    if len(parts) >= 2:
        base, quote = parts[0], parts[1]
        return CanonicalSymbol(
            s, f"{base}/{quote}", base, quote, _classify(base, quote)
        )
    token = parts[0] if parts else s
    base, quote = _split_concatenated(token)
    if quote is not None:
        return CanonicalSymbol(
            s, f"{base}/{quote}", base, quote, _classify(base, quote)
        )
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
    "1h": Timeframe.H1, "h1": Timeframe.H1,
    "60m": Timeframe.H1, "hourly": Timeframe.H1,
    "4h": Timeframe.H4, "h4": Timeframe.H4, "240m": Timeframe.H4,
    "d": Timeframe.D1, "d1": Timeframe.D1, "daily": Timeframe.D1,
    "day": Timeframe.D1, "1d": Timeframe.D1,
    "w": Timeframe.W1, "w1": Timeframe.W1, "weekly": Timeframe.W1,
    "week": Timeframe.W1, "1w": Timeframe.W1,
    "mn": Timeframe.MN, "monthly": Timeframe.MN, "month": Timeframe.MN,
    "1mn": Timeframe.MN,
}

# FIX-001: _TF_EXTRACT_RE supprimé — regex ReDoS remplacé par split+set (CWE-1333)
_TF_SET: Final[frozenset[str]] = frozenset({
    "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w",
    "h1", "h4", "d1", "w1", "mn", "daily", "weekly", "monthly", "hourly",
})


def parse_timeframe(raw: Any) -> Timeframe:
    if raw is None:
        return Timeframe.UNKNOWN
    s = str(raw).strip().lower()
    if not s:
        return Timeframe.UNKNOWN
    # 1. Direct lookup O(1)
    if s in _TF_ALIAS:
        return _TF_ALIAS[s]
    # 2. Token split — linéaire, sans backtracking (FIX-001)
    for part in re.split(r'[^a-z0-9]+', s):
        if part in _TF_SET:
            return _TF_ALIAS.get(part, Timeframe.UNKNOWN)
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


def _parse_iso_datetime(raw: Any) -> datetime | None:
    """Tolerant datetime parser: ISO-8601, Unix epoch (s & ms). UTC-naive."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if not _is_finite_number(raw):
            return None
        ts = float(raw)
        if abs(ts) > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    s = str(raw).strip()
    if not s:
        return None
    # Normalise trailing Z to +00:00 for fromisoformat compatibility.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


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


BaseCfg: Final[ConfigDict] = ConfigDict(
    extra="ignore",
    validate_assignment=False,
    arbitrary_types_allowed=True,
    str_strip_whitespace=True,
)


# ── RSI status mapping — v9.0 prompt-compatible 7-level scale ─────────────
# Thresholds (inclusive lower bound, ordered DESC):
#   80     → extreme_overbought
#   72     → overbought
#   68     → grey_high
#   32     → favorable    (the "neutral" healthy band)
#   28     → grey_low
#   20     → oversold
#   <20    → extreme_oversold
_RSI_STATUS_THRESHOLDS: Final[tuple[tuple[float, str], ...]] = (
    (80.0, "extreme_overbought"),
    (72.0, "overbought"),
    (68.0, "grey_high"),
    (32.0, "favorable"),
    (28.0, "grey_low"),
    (20.0, "oversold"),
)

# ── Hiérarchie de préférence des sources de prix : live > stale > None ────
# Utilisée par _fold_current_price pour upgrader la source lors d'une
# collision de merge, indépendamment de l'ordre d'arrivée des groupes.
_PRICE_SOURCE_RANK: Final[dict[str | None, int]] = {
    "live": 2,
    "stale": 1,
    None: 0,
}



def _rsi_status_from_value(v: float | None) -> str | None:
    """Map RSI value to categorical status using the v9.0 7-level scale.
    Returns None if value missing. Anything < 20 → 'extreme_oversold'."""
    if v is None or not _is_finite_number(v):
        return None
    for threshold, status in _RSI_STATUS_THRESHOLDS:
        if v >= threshold:
            return status
    return "extreme_oversold"


# Conviction cap mapping driven by ATR cascade fallback level (v9.0 §5.1).
# None = no cap. The downstream LLM is responsible for actually applying
# the cap; the merger only annotates the source + cap deterministically.
_ATR_CONVICTION_CAP: Final[dict[str, str | None]] = {
    "h4":        None,
    "h1_proxy":  "A",
    "d1_proxy":  "BBB",
    "synthetic": "BBB",
}

# Multiplier applied to atr_h1 when used as proxy for ATR_H4.
_ATR_H1_PROXY_MULT: Final[float] = 1.8
# Multiplier applied to atr_daily when used as proxy for ATR_H4.
_ATR_D1_PROXY_MULT: Final[float] = 0.25
# Synthetic ATR = current_price × this (0.5% of price).
_ATR_SYNTHETIC_PCT: Final[float] = 0.005
# SL floor distance multiplier (v9.0 §8.2).
_SL_FLOOR_MULT: Final[float] = 0.8
# SL raw distance multiplier (default = Normal regime, v9.0 §8.2).
_SL_RAW_DEFAULT_MULT: Final[float] = 1.1

# BB-regime → SL multiplier (v9.0 §8.2).
_BB_REGIME_SL_MULT: Final[dict[str, float]] = {
    "Squeeze":   1.0,
    "Normal":    1.5,
    "Expansion": 2.0,
}

# Fresh signal alignment thresholds (v9.0 §6.1).
_FRESH_CANDLES_MAX: Final[int] = 2


class RSIReading(BaseModel):
    model_config = BaseCfg
    timeframe: Timeframe
    value: float | None = None
    divergence: DivergenceKind = DivergenceKind.NONE
    # v3.4.3 — Option A: enrichissement des métadonnées de divergence RSI.
    # Capturés depuis le scanner RSI natif (champs strength_score,
    # confidence_score, div_kind, confirmed). Tous optionnels pour
    # rétrocompatibilité avec les sources qui ne les produisent pas.
    div_strength_score: float | None = None    # force du signal [0..1]
    div_confidence_score: float | None = None  # confiance confirmation [0..1]
    div_kind: str | None = None                # "REGULAR" | "HIDDEN" | None
    div_confirmed: bool = False                # True = pivot confirmé

    @field_validator("value")
    @classmethod
    def _clip(cls, v: float | None) -> float | None:
        if v is None or not _is_finite_number(v):
            return None
        if v < 0.0 or v > 100.0:
            return None
        return float(v)


class TrendBias(BaseModel):
    model_config = BaseCfg
    timeframe: Timeframe
    bias: str
    direction: Direction = Direction.NEUTRAL


class SRZone(BaseModel):
    model_config = BaseCfg
    side: Literal["BUY", "SELL", "PIVOT", "UNKNOWN"] = "UNKNOWN"  # MERGE-3: added PIVOT
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
    type: str | None = None      # MERGE-3: "Support" | "Resistance" | "Pivot"
    strength: float | None = None  # MERGE-3: SR "Force Totale"

    def is_real_sr(self) -> bool:
        """True iff this zone comes from a real SR scanner (not a synthetic
        nearest_support/nearest_resistance built from price_context text)."""
        return self.score > 0.0 and self.status not in _INVALID_ZONE_STATUSES


class PriceContext(BaseModel):
    model_config = BaseCfg
    raw: str = ""
    support_level: float | None = None
    support_dist_pct: float | None = None
    support_tag: str | None = None
    resistance_level: float | None = None
    resistance_dist_pct: float | None = None
    resistance_tag: str | None = None
    is_intermediate: bool = False
    # MERGE-4: derived fields (populated by _enrich_asset_precompute)
    trend: str | None = None             # D1 bias direction ("Bullish"/"Bearish"/"Range")
    near_zone: dict[str, Any] | None = None  # closest zone summary


class StructureEvent(BaseModel):
    model_config = BaseCfg
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
    # DIR-1: trend du contexte CHoCH (Bullish|Bearish) — informatif uniquement.
    # Permet de distinguer "CHoCH bearish dans trend bullish" vs inverse.
    # None si la source ne fournit pas ce champ (backward-compatible).
    choch_trend: Direction | None = None


class MTFConsensus(BaseModel):
    model_config = BaseCfg
    pct: int = 0
    direction: Direction = Direction.NEUTRAL
    quality: str | None = None
    nc: int = 0
    age_d1: int | None = None  # MERGE-5: None when GPS returns "N/A"
    atr_h1: float | None = None
    atr_h4: float | None = None
    atr_daily: float | None = None
    biases: dict[str, str] = Field(default_factory=dict)
    # MERGE-2: computed fields (null until _enrich_asset_precompute runs)
    htf: bool | None = None
    score: int | None = None
    grade: str | None = None

    @field_validator("pct", mode="before")
    @classmethod
    def _clamp_pct(cls, v: Any) -> int:
        return max(0, min(100, safe_int(v, default=0)))

    @field_validator("nc", mode="before")
    @classmethod
    def _coerce_nc(cls, v: Any) -> int:
        return max(0, safe_int(v, default=0))

    @field_validator("age_d1", mode="before")
    @classmethod
    def _coerce_age_d1(cls, v: Any) -> int | None:
        # MERGE-5: "N/A" / None / empty → None instead of 0
        if v is None:
            return None
        s = str(v).strip().upper()
        if s in ("N/A", "NA", ""):
            return None
        f = safe_float(v)
        if f is None:
            return None
        try:
            return max(0, int(f))
        except (OverflowError, ValueError):
            return None


class CanonicalAsset(BaseModel):
    """Canonical asset pivot.

    v3.4 adds (pre-computation for prompt v9.0):
      - atr_effective       : float | None     (ATR cascade output)
      - atr_source          : str | None       (h4 | h1_proxy | d1_proxy | synthetic)
      - conviction_cap      : str | None       (A | BBB | None)
      - nearest_aligned_zone: SRZone | None    (real SR preferred over synth)
      - hot_zone_primary    : SRZone | None    (incl. UNKNOWN pivots by sign)

    v3.3 added:
      - current_price (GAP 3)
      - rsi_by_tf dict (GAP 4)
      - rsi_h4_status pre-computed (GAP 5)
    """
    model_config = BaseCfg
    symbol: str
    base: str = ""
    quote: str | None = None
    asset_class: AssetClass = AssetClass.UNKNOWN
    current_price: float | None = None
    current_price_source: Literal["live", "stale"] | None = None  # SR-1: propagé depuis scanner S/R
    rsi: list[RSIReading] = Field(default_factory=list)
    rsi_by_tf: dict[str, dict[str, Any]] = Field(default_factory=dict)
    rsi_h4_status: str | None = None
    biases: list[TrendBias] = Field(default_factory=list)
    mtf: MTFConsensus | None = None
    price_context: PriceContext | None = None
    zones: list[SRZone] = Field(default_factory=list)
    structure_events: list[StructureEvent] = Field(default_factory=list)
    provenance: dict[str, list[str]] = Field(default_factory=dict)

    # ── v3.4 pre-computation layer ────────────────────────────────────────
    atr_effective: float | None = None
    atr_source: Literal["h4", "h1_proxy", "d1_proxy", "synthetic"] | None = None
    conviction_cap: Literal["A", "BBB"] | None = None
    nearest_aligned_zone: SRZone | None = None
    hot_zone_primary: SRZone | None = None
    # v3.4.3: direction dénormalisée au top-level (évite asset.mtf.direction dans le LLM)
    direction: Direction = Direction.NEUTRAL
    # v3.5.0: market_context — bloc passif, observabilité uniquement.
    # Calculé dans MergeEngine._enrich_asset_precompute après les autres pre-computations.
    # IGNORÉ par ENGINE_V9 (extra="ignore" dans son propre CanonicalAsset).
    # Aucun impact sur scoring/conviction/SL/TP/ranking.
    market_context: dict[str, Any] | None = None

    @classmethod
    def from_symbol(cls, sym: CanonicalSymbol) -> CanonicalAsset:
        return cls(
            symbol=sym.canonical,
            base=sym.base,
            quote=sym.quote,
            asset_class=sym.asset_class,
        )

    def add_provenance(self, source: str, tag: str) -> None:
        # cast: provenance is dict[str, list[str]] at runtime; Pydantic's
        # FieldInfo annotation misleads pylint into thinking it has no
        # .setdefault(). The cast silences the false E1101 without any
        # runtime cost.
        prov: dict[str, list[str]] = cast(dict[str, list[str]], self.provenance)
        bucket = prov.setdefault(source, [])
        if len(bucket) < MAX_PROVENANCE_ENTRIES:
            bucket.append(tag)

    def recompute_rsi_views(self) -> None:
        """Build rsi_by_tf dict + rsi_h4_status from rsi list.
        Called after each fold so views stay in sync. (GAP 4 + 5)
        v3.4: uses the 7-level v9.0 scale."""
        by_tf: dict[str, dict[str, Any]] = {}
        for r in self.rsi:
            key = r.timeframe.value
            by_tf[key] = {
                "value": r.value,
                "divergence": r.divergence.value,
                "status": _rsi_status_from_value(r.value),
                # v3.4.3 — Option A: métadonnées de divergence enrichies.
                # None si la source ne les fournit pas (rétrocompat totale).
                "div_strength_score": r.div_strength_score,
                "div_confidence_score": r.div_confidence_score,
                "div_kind": r.div_kind,
                "div_confirmed": r.div_confirmed,
            }
        self.rsi_by_tf = by_tf
        h4 = by_tf.get(Timeframe.H4.value)
        self.rsi_h4_status = h4.get("status") if h4 else None

    def recompute_current_price(self) -> None:
        """Promote current_price from first structure_event if missing. (GAP 3)"""
        if self.current_price is not None:
            return
        for ev in self.structure_events:
            if ev.current_price is not None and _is_finite_number(ev.current_price):
                self.current_price = ev.current_price
                return


class EnrichmentQuality(BaseModel):
    model_config = BaseCfg
    status: Literal["complete", "partial", "minimal", "empty"] = "empty"
    scanners_matched: int = 0
    scanners_total: int = 0


# ── v3.4: typed pre-computation block embedded in every EnrichedSignal ────
class SignalPrecomputed(BaseModel):
    """Deterministic pre-computations for the v9.0 DAG.
    All fields are derived ONLY from the merged asset + event — no I/O.
    """
    model_config = BaseCfg
    atr_effective: float | None = None
    atr_source: Literal["h4", "h1_proxy", "d1_proxy", "synthetic"] | None = None
    bb_mult: float = _SL_RAW_DEFAULT_MULT
    sl_distance_min: float | None = None
    sl_distance_raw: float | None = None
    rsi_h4_value: float | None = None
    rsi_h4_status: str | None = None
    candles_elapsed: int = 999
    sig_fresh_aligned: bool = False
    htf_aligned: bool = False
    conviction_cap: Literal["A", "BBB"] | None = None
    # DIR-1: contexte directionnel GPS vs CHoCH — purement informatif.
    # Aucun filtre, aucune modification de score/SL/TP/ranking.
    # L'engine consomme ces champs pour ses propres décisions.
    gps_direction: Direction | None = None        # = asset.mtf.direction
    choch_direction: Direction | None = None      # = event.direction
    direction_aligned: bool | None = None         # True si gps == choch
    counter_trend_signal: bool | None = None      # True si gps != choch
    alignment_score: int | None = None            # 100 si aligné, 0 sinon


class EnrichedSignal(BaseModel):
    """v3.4 adds the typed `precomputed` sub-model (SignalPrecomputed)."""
    model_config = BaseCfg
    event: StructureEvent
    asset: CanonicalAsset
    htf_aligned: bool = False
    nearest_aligned_zone: SRZone | None = None
    tp_zones: list[SRZone] = Field(default_factory=list)
    confluence_total: float = 0.0
    sl_price: float | None = None
    sl_atr_multiple: float = _SL_RAW_DEFAULT_MULT
    tp1_price: float | None = None
    tp1_atr_multiple: float | None = None
    rr_estimated: float | None = None
    enrichment: EnrichmentQuality = Field(default_factory=EnrichmentQuality)
    warnings: list[str] = Field(default_factory=list)
    precomputed: SignalPrecomputed = Field(default_factory=SignalPrecomputed)


class MergeMeta(BaseModel):
    model_config = BaseCfg
    generated_at: datetime
    version: str = SCHEMA_VERSION
    scanners_detected: list[str] = Field(default_factory=list)
    scanners_unknown: int = 0
    assets_count: int = 0
    signals_count: int = 0
    elapsed_ms: float = 0.0


class MergeOutput(BaseModel):
    model_config = BaseCfg
    meta: MergeMeta
    assets: dict[str, CanonicalAsset]
    signals: list[EnrichedSignal]
    correlation_groups: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    hot_zones: list[dict[str, Any]] = Field(default_factory=dict)
    top_consensus: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = Field(default_factory=dict)


# ════════════════════════════════════════════════════════════════════════════
# v3.4 — ATR CASCADE & ZONE PRE-COMPUTATION HELPERS
# ════════════════════════════════════════════════════════════════════════════
def compute_atr_effective(
    mtf: MTFConsensus | None,
    current_price: float | None,
) -> tuple[float | None, Literal["h4", "h1_proxy", "d1_proxy", "synthetic"] | None]:
    """ATR cascade per v9.0 §5.1. First level available wins.

    Returns (atr_effective, atr_source). (None, None) only if all levels
    are unusable — which gates G5 rejects the asset downstream (LLM side).
    """
    if mtf is not None:
        h4 = mtf.atr_h4
        if h4 is not None and _is_finite_number(h4) and h4 > 0:
            return float(h4), "h4"
        h1 = mtf.atr_h1
        if h1 is not None and _is_finite_number(h1) and h1 > 0:
            return round(float(h1) * _ATR_H1_PROXY_MULT, 8), "h1_proxy"
        d1 = mtf.atr_daily
        if d1 is not None and _is_finite_number(d1) and d1 > 0:
            return round(float(d1) * _ATR_D1_PROXY_MULT, 8), "d1_proxy"
    if current_price is not None and _is_finite_number(current_price) and current_price > 0:
        return round(float(current_price) * _ATR_SYNTHETIC_PCT, 8), "synthetic"
    return None, None


def _select_hot_zone_primary(
    asset: CanonicalAsset, direction: Direction
) -> SRZone | None:
    """Pick the most relevant 'ZONE CHAUDE' for the asset's direction.
    Includes UNKNOWN-side pivot zones if their distance sign is coherent with
    the direction (per v9.0 §6.2). Real SR zones are preferred over synthetic
    nearest zones — the latter rarely carry an 'alert' flag anyway."""
    if direction is Direction.NEUTRAL:
        return None
    wanted = "BUY" if direction is Direction.BULLISH else "SELL"

    def _alignment_ok(z: SRZone) -> bool:
        if z.alert != "ZONE CHAUDE" and "ZONE CHAUDE" not in z.alert.upper():
            return False
        if z.side == wanted:
            return True
        if z.side == "UNKNOWN":
            # Pivots: below price for bullish, above for bearish.
            # Note: distance_pct is stored absolute since v3.3.1 (P1 fix),
            # so we cannot use its sign. Fall back to the raw level.
            # The hot zone must be on the "right" side of current_price.
            cp = asset.current_price
            if cp is None or not _is_finite_number(cp) or cp <= 0:
                # Can't disambiguate → conservative: include UNKNOWN.
                return True
            if direction is Direction.BULLISH:
                return z.level <= cp
            return z.level >= cp
        return False

    aligned_hot = [z for z in asset.zones if _alignment_ok(z)]
    if not aligned_hot:
        return None
    # Prefer real SR zones; among those, the closest one wins.
    real_hot = [z for z in aligned_hot if z.is_real_sr()]
    pool = real_hot if real_hot else aligned_hot
    pool.sort(key=lambda z: z.distance_pct)
    return pool[0]


def _select_nearest_aligned_for_asset(
    asset: CanonicalAsset, direction: Direction
) -> SRZone | None:
    """Pick the closest aligned zone for the asset's MTF direction.
    Mirrors the enrichment-stage selector but operates at asset level so the
    LLM sees a pre-computed `asset.nearest_aligned_zone`.
    v3.4.1: inclut les zones UNKNOWN dont la position relative au prix
    est cohérente avec la direction (aligné avec _select_hot_zone_primary)."""
    if direction is Direction.NEUTRAL:
        return None
    wanted = "BUY" if direction is Direction.BULLISH else "SELL"

    def _is_aligned(z: SRZone) -> bool:
        if z.side == wanted:
            return True
        if z.side == "UNKNOWN" and z.level > 0:
            cp = asset.current_price
            if cp is not None and _is_finite_number(cp) and cp > 0:
                if direction is Direction.BULLISH:
                    return z.level <= cp  # Support = sous le prix
                return z.level >= cp      # Résistance = au-dessus du prix
        return False

    aligned = [z for z in asset.zones if _is_aligned(z)]
    if not aligned:
        return None
    real = [z for z in aligned if z.is_real_sr()]
    pool = real if real else aligned
    pool.sort(key=lambda z: z.distance_pct)
    return pool[0]


# ════════════════════════════════════════════════════════════════════════════
# ADAPTERS — abstract base + concrete implementations
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True, slots=True)
class AdapterMatch:
    score: float
    reason: str


class ScannerAdapter(ABC):
    name: str = "unknown"
    priority: int = 0

    @abstractmethod
    def detect(self, payload: Any) -> AdapterMatch: ...

    @abstractmethod
    def adapt(self, payload: Any) -> Result[list[CanonicalAsset]]: ...


# ──── GPS adapter ─────────────────────────────────────────────────────────
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
    if d is None:
        return pct, Direction.NEUTRAL
    t = d.group(1).lower()
    if t == "bullish":
        return pct, Direction.BULLISH
    if t == "bearish":
        return pct, Direction.BEARISH
    return pct, Direction.NEUTRAL


def _extract_gps_biases(raw: dict[str, Any]) -> dict[str, str]:
    biases: dict[str, str] = {}
    for k, tf in _GPS_TF_KEYS.items():
        v = raw.get(k)
        if v is not None:
            biases[tf.value] = safe_str(v, max_len=64)
    nested = raw.get("biases")
    if isinstance(nested, dict):
        for kk, vv in nested.items():
            tf = parse_timeframe(kk)
            if tf is not Timeframe.UNKNOWN and vv is not None:
                biases[tf.value] = safe_str(vv, max_len=64)
    return biases


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
                res.add(Diagnostic(
                    "gps", Severity.WARNING, "cap_reached",
                    f"MAX_ASSETS={MAX_ASSETS}",
                ))
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
            res.add(Diagnostic(
                "gps", Severity.DEBUG, "skip", "non-dict", {"i": idx}
            ))
            return None
        sym_raw = raw.get("Paire") or raw.get("pair") or raw.get("symbol")
        if not sym_raw:
            res.add(Diagnostic(
                "gps", Severity.WARNING, "no_symbol",
                "missing pair", {"i": idx},
            ))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        mtf, mtf_diag = GPSAdapter._build_mtf(raw, idx)
        if mtf_diag is not None:
            res.add(mtf_diag)
        if mtf is None:
            return None
        asset.mtf = mtf
        asset.add_provenance("gps", "mtf")
        return asset

    @staticmethod
    def _build_mtf(
        raw: dict[str, Any], idx: int
    ) -> tuple[MTFConsensus | None, Diagnostic | None]:
        pct, direction = _parse_mtf_string(raw.get("MTF", ""))
        biases = _extract_gps_biases(raw)
        quality_raw = raw.get("Quality")
        try:
            # MERGE-5: pass raw Age D1 to MTFConsensus — the _coerce_age_d1 validator
            # handles "N/A" / None → None (instead of silently coercing to 0).
            raw_age_d1 = raw.get("Age D1") or raw.get("AgeD1")
            mtf = MTFConsensus(
                pct=pct,
                direction=direction,
                quality=safe_str(quality_raw, max_len=8) if quality_raw else None,
                nc=safe_int(raw.get("NC")),
                age_d1=raw_age_d1,
                atr_h1=safe_float(raw.get("ATR H1")),
                atr_h4=safe_float(raw.get("ATR H4")),
                atr_daily=safe_float(raw.get("ATR Daily") or raw.get("ATR D1")),
                biases=biases,
            )
        except Exception as exc:
            return None, Diagnostic(
                "gps", Severity.WARNING, "mtf_invalid",
                f"{type(exc).__name__}: {exc}", {"i": idx},
            )
        return mtf, None


# ──── RSI adapter ─────────────────────────────────────────────────────────
_DIV_MAP: Final[dict[str, DivergenceKind]] = {
    "none": DivergenceKind.NONE,
    "aucune": DivergenceKind.NONE,
    "no": DivergenceKind.NONE,
    "bull": DivergenceKind.BULL,
    "bullish": DivergenceKind.BULL,
    "haussiere": DivergenceKind.BULL,
    "haussière": DivergenceKind.BULL,
    "bear": DivergenceKind.BEAR,
    "bearish": DivergenceKind.BEAR,
    "baissiere": DivergenceKind.BEAR,
    "baissière": DivergenceKind.BEAR,
}


def _norm_div(v: Any) -> DivergenceKind:
    if v is None:
        return DivergenceKind.NONE
    return _DIV_MAP.get(str(v).strip().lower(), DivergenceKind.NONE)


_TF_REMAP_LOG: Final[dict[str, str]] = {"d": "D1", "w": "W1"}


def _extract_nested_rsi(tfs: dict[str, Any]) -> list[RSIReading]:
    readings: list[RSIReading] = []
    for k, v in tfs.items():
        # MERGE-7: log TF label remapping for observability (D→D1, W→W1)
        k_lc = k.strip().lower()
        if k_lc in _TF_REMAP_LOG:
            _LOG.debug(
                "rsi_tf_remap: '%s' → '%s' (pair context unknown at this level)",
                k, _TF_REMAP_LOG[k_lc],
            )
        tf = parse_timeframe(k)
        if tf is Timeframe.UNKNOWN or not isinstance(v, dict):
            continue
        # Champ div simple (label court : "BULL", "BEAR", "NONE", "STALE"…)
        div_raw = v.get("div") or v.get("divergence")
        # Sous-objet divergence structuré (format scanner RSI natif v3+)
        div_obj = v.get("divergence") if isinstance(v.get("divergence"), dict) else {}
        # strength_score : présent à la racine du TF ET dans le sous-objet
        strength = safe_float(
            v.get("strength_score")
            or div_obj.get("strength_score")
        )
        confidence = safe_float(div_obj.get("confidence_score"))
        kind = safe_str(div_obj.get("kind") or div_obj.get("div_kind"), max_len=16) or None
        confirmed = bool(div_obj.get("confirmed", False))
        # Normaliser la divergence : si div_raw est un dict (champ "divergence"
        # était le sous-objet), on extrait le label textuel depuis div_obj.
        if isinstance(div_raw, dict):
            div_label = div_raw.get("label") or div_raw.get("code") or ""
        else:
            div_label = div_raw
        readings.append(RSIReading(
            timeframe=tf,
            value=safe_float(v.get("rsi") or v.get("value")),
            divergence=_norm_div(div_label),
            div_strength_score=strength,
            div_confidence_score=confidence,
            div_kind=kind if kind else None,
            div_confirmed=confirmed,
        ))
    return readings


def _extract_flat_rsi(raw: dict[str, Any]) -> list[RSIReading]:
    readings: list[RSIReading] = []
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
        div_val = _find_matching_div(raw, tf)
        readings.append(RSIReading(
            timeframe=tf,
            value=safe_float(v),
            divergence=_norm_div(div_val),
        ))
    return readings


def _find_matching_div(raw: dict[str, Any], tf: Timeframe) -> Any:
    for kk, vv in raw.items():
        if (
            isinstance(kk, str)
            and kk.lower().startswith("div")
            and parse_timeframe(kk) is tf
        ):
            return vv
    return None


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
        symbol_keys = {"pair", "devises", "symbol", "instrument", "paire"}
        if not symbol_keys & keys_lc:
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
                res.add(Diagnostic(
                    "rsi", Severity.WARNING, "cap_reached",
                    f"MAX_ASSETS={MAX_ASSETS}",
                ))
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
        sym_raw = (
            raw.get("pair")
            or raw.get("Devises")
            or raw.get("Paire")
            or raw.get("symbol")
            or raw.get("instrument")
        )
        if not sym_raw:
            res.add(Diagnostic(
                "rsi", Severity.WARNING, "no_symbol",
                "missing pair", {"i": idx},
            ))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        readings = RSIAdapter._extract_readings(raw)
        asset.rsi = readings[:MAX_RSI_READINGS_PER_ASSET]
        asset.recompute_rsi_views()
        asset.add_provenance("rsi", f"{len(asset.rsi)}tf")
        return asset

    @staticmethod
    def _extract_readings(raw: dict[str, Any]) -> list[RSIReading]:
        tfs = raw.get("timeframes")
        if isinstance(tfs, dict):
            return _extract_nested_rsi(tfs)
        if isinstance(tfs, list):
            return _extract_rsi_list(tfs)
        return _extract_flat_rsi(raw)


def _extract_rsi_list(items: list[Any]) -> list[RSIReading]:
    out: list[RSIReading] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        tf = parse_timeframe(it.get("timeframe") or it.get("tf"))
        if tf is Timeframe.UNKNOWN:
            continue
        out.append(RSIReading(
            timeframe=tf,
            value=safe_float(it.get("value") or it.get("rsi")),
            divergence=_norm_div(it.get("divergence") or it.get("div")),
        ))
    return out


# ──── S/R adapter ─────────────────────────────────────────────────────────
_SUP_RE: Final[re.Pattern[str]] = re.compile(
    r"(SUR\s+support|S\s+proche|support)[:\s]+([\d.]+)\s*(([-+]?[\d.]+)\s*%)",
    re.I,
)
_RES_RE: Final[re.Pattern[str]] = re.compile(
    r"(SUR\s+resistance|R\s+proche|resistance)[:\s]+([\d.]+)\s*(([-+]?[\d.]+)\s*%)",
    re.I,
)
_INTER_RE: Final[re.Pattern[str]] = re.compile(
    r"(intermediaire|intermediate|unavailable|indisponible|n/a)", re.I
)
_STATUS_COEFF: Final[dict[str, float]] = {
    "vierge": 1.0,
    "virgin": 1.0,
    "testee": 0.8,
    "tested": 0.8,
    "role reverse": 0.6,
}


def _parse_side(raw: Any) -> Literal["BUY", "SELL", "PIVOT", "UNKNOWN"]:
    sig = str(raw or "").upper()
    if "BUY" in sig:
        return "BUY"
    if "SELL" in sig:
        return "SELL"
    if "PIVOT" in sig:
        return "PIVOT"
    return "UNKNOWN"


def _parse_alert(raw: Any) -> str:
    """Conserves the original raw string to preserve visual emojis if found."""
    s = str(raw or "").strip()
    if not s:
        return ""
    upper_s = s.upper()
    if "CHAUDE" in upper_s or "HOT" in upper_s:
        return s
    if "PROCHE" in upper_s or "NEAR" in upper_s:
        return s
    return ""


def _parse_tf_list(tf_raw: Any) -> list[Timeframe]:
    tf_list: list[Timeframe] = []
    if isinstance(tf_raw, list):
        iterable: Iterable[Any] = tf_raw
    else:
        iterable = re.split(r"[+,/]", str(tf_raw or ""))
    for tok in iterable:
        tf = parse_timeframe(str(tok).strip())
        if tf is not Timeframe.UNKNOWN:
            tf_list.append(tf)
    return tf_list


def _resolve_zone_side(
    z: dict[str, Any],
    current_price: float | None,
    level: float,
) -> Literal["BUY", "SELL", "PIVOT", "UNKNOWN"]:
    """Determine the side of an SR zone in two steps:
    1. Parse explicit side/direction keys in order of priority.
    2. Only if NO explicit key was found, infer from price position.
    """
    _SIDE_KEYS = (
        "signal", "Signal", "side", "Side",
        "direction", "Direction", "sens", "Sens",
        "position", "Position", "role", "Role",
    )
    _TYPE_KEYS = ("type", "Type")

    # Check whether any explicit side key exists in the dict at all.
    has_explicit_key = any(z.get(k) is not None for k in _SIDE_KEYS + _TYPE_KEYS)

    side_src = (
        z.get("signal") or z.get("Signal")
        or z.get("side") or z.get("Side")
        or z.get("direction") or z.get("Direction")
        or z.get("type") or z.get("Type")
        or z.get("sens") or z.get("Sens")
        or z.get("position") or z.get("Position")
        or z.get("role") or z.get("Role")
    )
    side = _parse_side(side_src)
    if side != "UNKNOWN":
        return side

    # MERGE-3: Pivots are bidirectional — return "PIVOT" not "UNKNOWN".
    original_signal = str(z.get("Signal") or z.get("signal") or "")
    original_type = str(z.get("Type") or z.get("type") or "")
    if "PIVOT" in original_signal.upper() or "PIVOT" in original_type.upper():
        return "PIVOT"

    # Positional inference only when NO explicit side key was present.
    # If an explicit key exists but was unrecognized (e.g. "UNKNOWN", "N/A"),
    # honour it and return UNKNOWN rather than silently overriding.
    if has_explicit_key:
        return "UNKNOWN"

    # v3.4.4 (FIX-003): robust float inference with isfinite + isclose
    if level > 0 and current_price is not None and current_price > 0:
        if not math.isfinite(level) or not math.isfinite(current_price):
            return "UNKNOWN"
        if math.isclose(level, current_price, rel_tol=1e-9, abs_tol=1e-12):
            return "UNKNOWN"  # zone touched, ambiguous
        if level < current_price:
            return "BUY"   # Support: price is above the level
        if level > current_price:
            return "SELL"  # Resistance: price is below the level
    return "UNKNOWN"


def _build_zone_from_raw(z: dict[str, Any], current_price: float | None = None) -> SRZone | None:
    # Clé principale snake_case (formats tiers génériques) puis alias
    # PascalCase/FR produits par le scanner BLUESTAR natif.
    # Ordre : clé standard d'abord → rétrocompatibilité garantie.
    level = safe_float(z.get("level") or z.get("Niveau"))
    if level is None or level <= 0:
        return None
    score = safe_float(z.get("score") or z.get("Score")) or 0.0
    dist = safe_float(z.get("distance_pct") or z.get("Distance %"))
    if dist is None:
        dist = 999.0
    status = safe_str(
        z.get("status") or z.get("Statut") or "Unknown", max_len=32
    )
    coeff = _STATUS_COEFF.get(status.lower(), 0.8)
    tf_list = _parse_tf_list(
        z.get("timeframes") or z.get("Timeframes") or ""
    )
    # Side resolution: explicit keys first, then positional inference.
    side = _resolve_zone_side(z, current_price, level)
    # MERGE-3: preserve zone type (Support / Resistance / Pivot)
    zone_type = safe_str(
        z.get("zone_type") or z.get("Type") or z.get("type") or "", max_len=32
    ) or None
    # MERGE-3: preserve raw strength score from SR scanner
    zone_strength = safe_float(
        z.get("zone_strength") or z.get("Force Totale") or z.get("force_totale")
    )
    return SRZone(
        side=side,
        level=round(level, 5),
        score=round(score, 2),
        weighted_score=round(score * coeff, 2),
        status=status,
        distance_pct=round(abs(dist), 3),
        alert=_parse_alert(z.get("alert") or z.get("Alerte") or ""),
        timeframes=tf_list,
        has_weekly=Timeframe.W1 in tf_list,
        has_daily=Timeframe.D1 in tf_list,
        has_h4=Timeframe.H4 in tf_list,
        type=zone_type,
        strength=zone_strength,
    )


def _parse_price_context_from_text(s: str) -> PriceContext:
    """Regex-based parser for the legacy textual price_context format."""
    ctx = PriceContext(raw=s)
    if not s or _INTER_RE.search(s):
        ctx.is_intermediate = True
        return ctx
    m = _SUP_RE.search(s)
    if m:
        ctx.support_tag = m.group(1).strip()
        ctx.support_level = safe_float(m.group(2))
        ctx.support_dist_pct = safe_float(m.group(3).strip().rstrip("%"))
    m = _RES_RE.search(s)
    if m:
        ctx.resistance_tag = m.group(1).strip()
        ctx.resistance_level = safe_float(m.group(2))
        ctx.resistance_dist_pct = safe_float(m.group(3).strip().rstrip("%"))
    if ctx.support_level is None and ctx.resistance_level is None:
        ctx.is_intermediate = True
    return ctx


def _parse_price_context(raw: Any) -> PriceContext:
    """v3.4.4 (FIX-002): _apply_nearest_fallback fusionné inline.
    Robust parser; falls back to regex on dict['raw']."""
    if raw is None:
        return PriceContext(raw="")
    if isinstance(raw, dict):
        raw_text = safe_str(raw.get("raw") or "", max_len=512)
        sup_level = safe_float(raw.get("support_level"))
        sup_dist = safe_float(raw.get("support_dist_pct"))
        sup_tag = raw.get("support_tag")
        res_level = safe_float(raw.get("resistance_level"))
        res_dist = safe_float(raw.get("resistance_dist_pct"))
        res_tag = raw.get("resistance_tag")
        # FIX-002: inline nearest_support / nearest_resistance (was _apply_nearest_fallback)
        if sup_level is None or res_level is None:
            ns = raw.get("nearest_support")
            nr = raw.get("nearest_resistance")
            if isinstance(ns, dict) and sup_level is None:
                sup_level = safe_float(ns.get("level"))
                sup_dist = safe_float(ns.get("distance_pct"))
                sup_tag = ns.get("tag") or ns.get("status")
            if isinstance(nr, dict) and res_level is None:
                res_level = safe_float(nr.get("level"))
                res_dist = safe_float(nr.get("distance_pct"))
                res_tag = nr.get("tag") or nr.get("status")
        if (
            sup_level is None
            and res_level is None
            and raw_text
            and not _INTER_RE.search(raw_text)
        ):
            fallback = _parse_price_context_from_text(raw_text)
            sup_level = fallback.support_level
            sup_dist = fallback.support_dist_pct
            sup_tag = fallback.support_tag or sup_tag
            res_level = fallback.resistance_level
            res_dist = fallback.resistance_dist_pct
            res_tag = fallback.resistance_tag or res_tag
        ctx = PriceContext(raw=raw_text)
        ctx.support_level = sup_level
        ctx.support_dist_pct = sup_dist
        ctx.support_tag = safe_str(sup_tag, max_len=64) if sup_tag else None
        ctx.resistance_level = res_level
        ctx.resistance_dist_pct = res_dist
        ctx.resistance_tag = safe_str(res_tag, max_len=64) if res_tag else None
        is_inter = raw.get("is_intermediate")
        if isinstance(is_inter, bool):
            ctx.is_intermediate = is_inter
        elif raw_text and _INTER_RE.search(raw_text):
            ctx.is_intermediate = True
        elif sup_level is None and res_level is None:
            ctx.is_intermediate = True
        return ctx
    s = safe_str(raw, max_len=512)
    return _parse_price_context_from_text(s)


def _build_zone_from_nearest(
    obj: Any, side: Literal["BUY", "SELL"]
) -> SRZone | None:
    """Build a synthetic SRZone from nearest_support / nearest_resistance.
    v3.3.1 (P1): distance_pct forced positive, status tagged 'SR_nearest'."""
    if not isinstance(obj, dict):
        return None
    level = safe_float(obj.get("level"))
    if level is None or level <= 0:
        return None
    dist = safe_float(obj.get("distance_pct"))
    if dist is None:
        dist = 999.0
    status = _SR_NEAREST_STATUS
    score = 0.0
    tf_list = _parse_tf_list(obj.get("timeframes") or obj.get("tf") or "")
    return SRZone(
        side=side,
        level=round(level, 5),
        score=round(score, 2),
        weighted_score=0.0,
        status=status,
        distance_pct=round(abs(dist), 3),
        alert=_parse_alert(obj.get("alert", "")),
        timeframes=tf_list,
        has_weekly=Timeframe.W1 in tf_list,
        has_daily=Timeframe.D1 in tf_list,
        has_h4=Timeframe.H4 in tf_list,
    )


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
                res.add(Diagnostic(
                    "sr", Severity.WARNING, "cap_reached",
                    f"MAX_ASSETS={MAX_ASSETS}",
                ))
                break
            asset = self._build_asset(raw, idx, res)
            if asset is not None:
                out.append(asset)
        # SR-1: propager diagnostics.assets_with_no_zones comme INFO dans le pipeline
        diag_block = payload.get("diagnostics")
        if isinstance(diag_block, dict):
            no_zones = diag_block.get("assets_with_no_zones")
            if isinstance(no_zones, list) and no_zones:
                res.add(Diagnostic(
                    "sr", Severity.INFO, "no_zones_detected",
                    f"{len(no_zones)} assets sans zones",
                    {"assets": no_zones},
                ))
        return res

    @staticmethod
    def _build_asset(
        raw: Any, idx: int, res: Result[list[CanonicalAsset]]
    ) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            return None
        sym_raw = raw.get("symbol") or raw.get("pair") or raw.get("Paire")
        if not sym_raw:
            res.add(Diagnostic(
                "sr", Severity.WARNING, "no_symbol",
                "missing", {"i": idx},
            ))
            return None
        sym = normalize_symbol(sym_raw)
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        cp = safe_float(raw.get("current_price") or raw.get("price"))
        if cp is not None:
            asset.current_price = cp
        # SR-1: propager la source du prix (live vs stale/marché fermé)
        cp_source = raw.get("current_price_source")
        if cp_source in ("live", "stale"):
            asset.current_price_source = cp_source
        asset.price_context = _parse_price_context(raw.get("price_context", ""))
        zones = SRAdapter._collect_zones(raw.get("zones", []), asset.current_price)
        pc_raw = raw.get("price_context")
        if isinstance(pc_raw, dict):
            ns = pc_raw.get("nearest_support")
            nr = pc_raw.get("nearest_resistance")
        else:
            ns = raw.get("nearest_support")
            nr = raw.get("nearest_resistance")
        z_ns = _build_zone_from_nearest(ns, "BUY")
        z_nr = _build_zone_from_nearest(nr, "SELL")
        if z_ns is not None:
            zones.append(z_ns)
        if z_nr is not None:
            zones.append(z_nr)
        seen: set[tuple[str, float]] = set()
        dedup: list[SRZone] = []
        for z in zones:
            key = (z.side, round(z.level, 5))
            if key in seen:
                continue
            seen.add(key)
            dedup.append(z)
        # Correction minimale : On supprime le tri par distance_pct pour préserver l'ordre d'origine
        asset.zones = dedup[:MAX_ZONES_PER_ASSET]
        asset.add_provenance("sr", f"{len(asset.zones)}zones")
        return asset

    @staticmethod
    def _collect_zones(zones_raw: Any, current_price: float | None = None) -> list[SRZone]:
        zones: list[SRZone] = []
        if not isinstance(zones_raw, list):
            return zones
        for z in zones_raw:
            if len(zones) >= MAX_ZONES_PER_ASSET:
                break
            if not isinstance(z, dict):
                continue
            parsed = _build_zone_from_raw(z, current_price)
            if parsed is not None:
                zones.append(parsed)
        # Bug #1 fix: Ne pas trier. L'ordre d'insertion du scanner source est préservé.
        return zones


# ──── CHoCH adapter ───────────────────────────────────────────────────────
def _parse_direction_text(raw: Any) -> Direction:
    s = str(raw or "").lower()
    if "bull" in s:
        return Direction.BULLISH
    if "bear" in s:
        return Direction.BEARISH
    return Direction.NEUTRAL


def _maybe_str(raw: dict[str, Any], key: str, max_len: int = 32) -> str | None:
    v = raw.get(key)
    if v is None:
        return None
    s = safe_str(v, max_len=max_len)
    return s if s else None


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
        return AdapterMatch(self._signature_score(sample), "choch signature")

    @staticmethod
    def _signature_score(sample: dict[str, Any]) -> float:
        keys = set(sample.keys())
        score = 0.3
        if {"type", "is_choch", "kind"} & keys:
            score += 0.3
        if "direction" in keys:
            score += 0.2
        if "confluence_score" in keys:
            score += 0.2
        return min(score, 1.0)

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
            if len(by_sym) >= MAX_ASSETS:
                res.add(Diagnostic(
                    "choch", Severity.WARNING, "cap_reached",
                    f"MAX_ASSETS={MAX_ASSETS}",
                ))
                break
            self._ingest_signal(raw, idx, by_sym, res)
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
        sym_raw = (
            raw.get("pair") or raw.get("symbol")
            or raw.get("pair_oanda") or raw.get("Paire")
        )
        if not sym_raw:
            res.add(Diagnostic(
                "choch", Severity.WARNING, "no_symbol",
                "missing", {"i": idx},
            ))
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
        if asset.current_price is None and event.current_price is not None:
            asset.current_price = event.current_price

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
            res.add(Diagnostic(
                "choch", Severity.DEBUG, "bad_time",
                "unparseable signal_time",
                {"v": safe_str(ts_raw, max_len=40)},
            ))
        try:
            return StructureEvent(
                signal_id=safe_str(
                    raw.get("signal_id") or raw.get("id")
                    or f"auto_{idx}_{symbol}",
                    max_len=128,
                ),
                kind=safe_str(
                    raw.get("type") or raw.get("kind") or "CHoCH",
                    max_len=32,
                ),
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
                volatility=_maybe_str(raw, "volatility"),
                force=_maybe_str(raw, "force"),
                bb_regime=_maybe_str(raw, "bb_regime"),
                session=_maybe_str(raw, "session"),
                candles_elapsed=safe_int(raw.get("candles_elapsed")),
                # DIR-1: lire "trend" du scanner CHoCH (contexte directionnel)
                choch_trend=_parse_direction_text(raw["trend"])
                if raw.get("trend") else None,
            )
        except Exception as exc:
            res.add(Diagnostic(
                "choch", Severity.WARNING, "event_invalid",
                f"{type(exc).__name__}: {exc}",
                {"i": idx, "sym": symbol},
            ))
            return None


# ──── Heuristic fallback ─────────────────────────────────────────────────
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
        res.add(Diagnostic(
            "heuristic", Severity.INFO, "introspected",
            f"extracted {len(out)} assets",
        ))
        return res

    @staticmethod
    def _build_asset(raw: Any) -> CanonicalAsset | None:
        if not isinstance(raw, dict):
            return None
        sym_key = HeuristicAdapter._find_symbol_key(raw)
        if sym_key is None:
            return None
        sym = normalize_symbol(raw[sym_key])
        if not sym.canonical:
            return None
        asset = CanonicalAsset.from_symbol(sym)
        readings = HeuristicAdapter._extract_rsi(raw)
        asset.rsi = readings[:MAX_RSI_READINGS_PER_ASSET]
        asset.recompute_rsi_views()
        asset.add_provenance("heuristic", "introspected")
        return asset

    @staticmethod
    def _find_symbol_key(raw: dict[str, Any]) -> str | None:
        keys = list(raw.keys())
        for hint in _SYMBOL_HINTS:
            k = _best_fuzzy_key(keys, hint, 80)
            if k is not None:
                return k
        return None

    @staticmethod
    def _extract_rsi(raw: dict[str, Any]) -> list[RSIReading]:
        readings: list[RSIReading] = []
        seen: set[Timeframe] = set()
        for k, v in raw.items():
            if not isinstance(k, str) or "rsi" not in k.lower():
                continue
            val, tf = HeuristicAdapter._rsi_value_and_tf(k, v)
            if val is None or val < 0 or val > 100:
                continue
            if tf is Timeframe.UNKNOWN or tf in seen:
                continue
            seen.add(tf)
            readings.append(RSIReading(timeframe=tf, value=val))
        return readings

    @staticmethod
    def _rsi_value_and_tf(
        key: str, value: Any
    ) -> tuple[float | None, Timeframe]:
        if isinstance(value, dict):
            val = safe_float(value.get("rsi") or value.get("value"))
            tf = parse_timeframe(key)
            if tf is Timeframe.UNKNOWN:
                tf = parse_timeframe(value.get("tf") or value.get("timeframe"))
            return val, tf
        return safe_float(value), parse_timeframe(key)


# ════════════════════════════════════════════════════════════════════════════
# REGISTRY — deterministic adapter selection
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
        for adapter in self._adapters:
            match = self._safe_detect(adapter, payload)
            if match is None:
                continue
            if self._is_better(match, adapter, best):
                best = DetectionResult(adapter, match.score, match.reason)
        if best.score < self._FALLBACK_THRESHOLD and self._fallback is not None:
            fb_match = self._safe_detect(self._fallback, payload)
            if fb_match is not None and fb_match.score > best.score:
                return DetectionResult(
                    self._fallback, fb_match.score, "fallback"
                )
        return best

    @staticmethod
    def _safe_detect(
        adapter: ScannerAdapter, payload: Any
    ) -> AdapterMatch | None:
        match, diag = _safe_call(
            f"registry.detect.{adapter.name}", "detect_crash",
            lambda a=adapter, p=payload: a.detect(p),
            AdapterMatch(0.0, "crash"),
            severity=Severity.WARNING,
        )
        if diag is not None:
            _LOG.warning("adapter %s.detect crashed", adapter.name)
            return None
        return match

    @staticmethod
    def _is_better(
        match: AdapterMatch,
        adapter: ScannerAdapter,
        best: DetectionResult,
    ) -> bool:
        if match.score > best.score:
            return True
        if (
            match.score == best.score
            and best.adapter is not None
            and adapter.priority > best.adapter.priority
        ):
            return True
        return False

    def adapt(self, payload: Any) -> tuple[str, Result[list[CanonicalAsset]]]:
        det = self.detect(payload)
        if det.adapter is None:
            r: Result[list[CanonicalAsset]] = Result(value=[])
            r.add(Diagnostic(
                "registry", Severity.ERROR, "no_adapter",
                "no adapter matched", {"reason": det.reason},
            ))
            return "unknown", r
        adapter = det.adapter
        result, crash_diag = _safe_call(
            f"registry.adapt.{adapter.name}", "adapter_crash",
            lambda a=adapter, p=payload: a.adapt(p),
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
# MERGE ENGINE
# ════════════════════════════════════════════════════════════════════════════
class MergeEngine:
    """Deterministic, defensive merger of partial asset groups into a canon."""

    def merge(
        self, partial_groups: list[list[CanonicalAsset]]
    ) -> Result[dict[str, CanonicalAsset]]:
        merged: dict[str, CanonicalAsset] = {}
        res: Result[dict[str, CanonicalAsset]] = Result(value=merged)
        collisions: dict[str, int] = defaultdict(int)
        for group in partial_groups:
            stop = self._merge_group(group, merged, collisions, res)
            if stop:
                break

        # ── Phase 2: per-asset pre-computation (v3.4) ────────────────────
        for asset in merged.values():
            asset.recompute_rsi_views()
            asset.recompute_current_price()
            self._enrich_asset_precompute(asset, res)

        res.add(Diagnostic(
            "merge", Severity.INFO, "summary",
            f"merged {len(merged)} assets",
            {"collisions_top": dict(
                sorted(collisions.items(), key=lambda kv: -kv[1])[:10]
            )},
        ))
        return res

    # ── v3.4: per-asset pre-computation (ATR cascade, zones) ──────────────
    @staticmethod
    def _enrich_asset_precompute(
        asset: CanonicalAsset,
        res: Result[dict[str, CanonicalAsset]],
    ) -> None:
        """Compute ATR cascade output, conviction cap, nearest aligned zone
        and primary hot zone. Pure, deterministic, no I/O."""
        atr_eff, atr_src = compute_atr_effective(asset.mtf, asset.current_price)
        asset.atr_effective = atr_eff
        asset.atr_source = atr_src
        cap = _ATR_CONVICTION_CAP.get(atr_src) if atr_src is not None else None
        # SR-1: prix stale (marché fermé) → conviction cap plafonnée à BBB
        # même si l'ATR serait normalement A. Le prix de référence est figé,
        # donc nearest_aligned_zone et sl/tp sont moins fiables.
        if asset.current_price_source == "stale":
            cap = "BBB" if cap is None or cap == "A" else cap
        # cast for type-checker; values are constrained to A | BBB | None
        asset.conviction_cap = cast(
            "Literal['A', 'BBB'] | None", cap
        )

        # MERGE-1: populate asset.biases (top-level list) from mtf.biases dict.
        # ENGINE_V9 reads asset.biases for HTF alignment; mtf.biases is preserved.
        if asset.mtf is not None and asset.mtf.biases:
            asset.biases = [
                TrendBias(
                    timeframe=parse_timeframe(tf),
                    bias=direction,
                    direction=_direction_from_text(direction),
                )
                for tf, direction in asset.mtf.biases.items()
            ]

        # MERGE-2: compute mtf.htf, mtf.score, mtf.grade from biases + quality.
        if asset.mtf is not None:
            # grade = quality (already computed by GPS)
            asset.mtf.grade = asset.mtf.quality

            # score = count of TFs aligned with MTF direction
            mtf_dir = asset.mtf.direction
            if mtf_dir in (Direction.BULLISH, Direction.BEARISH):
                asset.mtf.score = sum(
                    1 for b in asset.biases
                    if b.direction == mtf_dir
                )
            else:
                asset.mtf.score = 0

            # htf = at least _HTF_MIN_AGREEMENT of _HTF_BIAS_TFS agree on same direction (excluding Range)
            htf_directions = [
                b.direction for b in asset.biases
                if b.timeframe.value in _HTF_BIAS_TFS
                and b.direction not in (Direction.NEUTRAL,)
                and "range" not in b.bias.lower()
            ]
            if not htf_directions:
                asset.mtf.htf = False
            else:
                dominant = max(set(htf_directions), key=htf_directions.count)
                asset.mtf.htf = (
                    len([d for d in htf_directions if d == dominant]) >= _HTF_MIN_AGREEMENT
                )

        direction = asset.mtf.direction if asset.mtf else Direction.NEUTRAL
        asset.direction = direction  # v3.4.3: dénormalisation top-level
        asset.nearest_aligned_zone = _select_nearest_aligned_for_asset(
            asset, direction
        )
        asset.hot_zone_primary = _select_hot_zone_primary(asset, direction)

        # MERGE-4: derive price_context.trend and near_zone
        if asset.price_context is not None:
            # trend = D1 bias
            d1_bias = next(
                (b for b in asset.biases if b.timeframe is Timeframe.D1), None
            )
            asset.price_context.trend = d1_bias.bias if d1_bias else None

            # near_zone = closest zone by distance_pct
            if asset.zones:
                nearest_z = min(
                    asset.zones,
                    key=lambda z: z.distance_pct if z.distance_pct is not None else 999.0,
                )
                asset.price_context.near_zone = {
                    "level": nearest_z.level,
                    "type": nearest_z.type,
                    "side": nearest_z.side,
                    "distance_pct": nearest_z.distance_pct,
                }
            else:
                asset.price_context.near_zone = None

        if atr_eff is None:
            res.add(Diagnostic(
                "merge.precompute", Severity.WARNING, "no_atr",
                "ATR cascade exhausted — asset will be downgraded downstream",
                {"sym": asset.symbol},
            ))
        elif atr_src != "h4":
            res.add(Diagnostic(
                "merge.precompute", Severity.INFO, "atr_fallback",
                f"using {atr_src} (cap={cap})",
                {"sym": asset.symbol, "atr_effective": atr_eff},
            ))

        # v3.5.0: market_context — passive, additive, zero scoring impact.
        # Computed last so all other pre-computations are already finalised.
        ctx, ctx_diag = _safe_call(
            "merge.market_ctx", "market_ctx_crash",
            lambda a=asset: _build_market_context(a),
            None,
            severity=Severity.WARNING,
        )
        if ctx_diag is not None:
            res.add(ctx_diag)
        asset.market_context = ctx  # None on crash — engine silently ignores it

    @staticmethod
    def _merge_group(
        group: list[CanonicalAsset],
        merged: dict[str, CanonicalAsset],
        collisions: dict[str, int],
        res: Result[dict[str, CanonicalAsset]],
    ) -> bool:
        for asset in group:
            if len(merged) >= MAX_ASSETS and asset.symbol not in merged:
                res.add(Diagnostic(
                    "merge", Severity.WARNING, "cap_reached",
                    f"MAX_ASSETS={MAX_ASSETS}",
                ))
                return True
            key = asset.symbol
            if not key:
                res.add(Diagnostic(
                    "merge", Severity.WARNING, "empty_symbol", "dropped"
                ))
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = asset.model_copy(deep=True)
                continue
            _, diag = _safe_call(
                "merge", "fold_crash",
                lambda t=existing, s=asset, r=res: MergeEngine._fold_into(t, s, r),
                None,
                severity=Severity.WARNING,
            )
            if diag is not None:
                res.add(diag)
            collisions[key] += 1
        return False

    @staticmethod
    def _fold_into(
        target: CanonicalAsset,
        source: CanonicalAsset,
        res: Result[dict[str, CanonicalAsset]],
    ) -> None:
        src = source.model_copy(deep=True)
        MergeEngine._fold_identity(target, src)
        MergeEngine._fold_rsi(target, src, res)
        MergeEngine._fold_biases(target, src)
        MergeEngine._fold_mtf(target, src)
        MergeEngine._fold_price_context(target, src)
        MergeEngine._fold_zones(target, src)
        MergeEngine._fold_events(target, src)
        MergeEngine._fold_current_price(target, src)
        MergeEngine._fold_provenance(target, src)

    @staticmethod
    def _fold_identity(target: CanonicalAsset, source: CanonicalAsset) -> None:
        if (
            target.asset_class is AssetClass.UNKNOWN
            and source.asset_class is not AssetClass.UNKNOWN
        ):
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
        for reading in source.rsi:
            if len(target.rsi) >= MAX_RSI_READINGS_PER_ASSET:
                break
            if reading.timeframe in existing_tfs:
                res.add(Diagnostic(
                    "merge", Severity.DEBUG, "rsi_conflict",
                    f"{reading.timeframe.value} duplicate",
                    {"sym": target.symbol},
                ))
                continue
            target.rsi.append(reading)
            existing_tfs.add(reading.timeframe)

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
    def _fold_price_context(
        target: CanonicalAsset, source: CanonicalAsset
    ) -> None:
        if source.price_context is None:
            return
        if target.price_context is None:
            target.price_context = source.price_context
            return
        if (
            not source.price_context.is_intermediate
            and target.price_context.is_intermediate
        ):
            target.price_context = source.price_context
            return
        tpc = target.price_context
        spc = source.price_context
        if tpc.support_level is None and spc.support_level is not None:
            tpc.support_level = spc.support_level
            tpc.support_dist_pct = spc.support_dist_pct
            tpc.support_tag = spc.support_tag
        if tpc.resistance_level is None and spc.resistance_level is not None:
            tpc.resistance_level = spc.resistance_level
            tpc.resistance_dist_pct = spc.resistance_dist_pct
            tpc.resistance_tag = spc.resistance_tag

    @staticmethod
    def _fold_zones(target: CanonicalAsset, source: CanonicalAsset) -> None:
        # FIX-004: clé enrichie round(6)+status pour éviter collisions sur niveaux très proches
        existing = {(z.side, round(z.level, 6), z.status) for z in target.zones}
        for z in source.zones:
            if len(target.zones) >= MAX_ZONES_PER_ASSET:
                break
            key = (z.side, round(z.level, 6), z.status)
            if key not in existing:
                target.zones.append(z)
                existing.add(key)
        # Bug #1 fix: Ne pas trier après fusion — ordre sémantique préservé.
        # Tri d'affichage uniquement côté rendu.

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
    def _fold_current_price(
        target: CanonicalAsset, source: CanonicalAsset
    ) -> None:
        # Préférer la source la plus fiable : live > stale > None.
        if source.current_price is None:
            return
        if target.current_price is None:
            target.current_price = source.current_price
            target.current_price_source = source.current_price_source
            return
        t_rank = _PRICE_SOURCE_RANK.get(target.current_price_source, 0)
        s_rank = _PRICE_SOURCE_RANK.get(source.current_price_source, 0)
        if s_rank > t_rank:
            # Source strictement meilleure : upgrade prix + source.
            target.current_price = source.current_price
            target.current_price_source = source.current_price_source
        # Rangs égaux ou inférieurs : aucune mutation (idempotent, stable).

    @staticmethod
    def _fold_provenance(target: CanonicalAsset, source: CanonicalAsset) -> None:
        for k, v in source.provenance.items():
            bucket = target.provenance.setdefault(k, [])
            remaining = MAX_PROVENANCE_ENTRIES - len(bucket)
            if remaining <= 0:
                continue
            bucket.extend(v[:remaining])


# ════════════════════════════════════════════════════════════════════════════
# v3.5.0 — MARKET CONTEXT (passive, read-only, zero scoring impact)
# ════════════════════════════════════════════════════════════════════════════
# Timeframe seniority ordinal: higher = structurally more significant.
_MC_TF_SENIORITY: Final[dict[str, int]] = {
    "MN": 6, "W1": 5, "D1": 4, "H4": 3, "H1": 2, "M15": 1, "M5": 0, "M1": 0,
}

# age_d1 category thresholds (inclusive lower bound).
_MC_AGE_MATURE: Final[int] = 30
_MC_AGE_YOUNG: Final[int] = 10

# S/R proximity threshold for sr_context (%).
_MC_SR_NEAR_PCT: Final[float] = 2.0

# RSI divergence TFs considered "higher-timeframe" for risk amplification.
_MC_DIV_HTF: Final[frozenset[str]] = frozenset({"D1", "W1"})


def _mc_classify_structure_events(
    events: list[StructureEvent],
    mtf_direction: Direction,
) -> dict[str, Any]:
    """Classify structure events as aligned / counter and compute escalation.

    Pure function — no side effects, no mutation.
    Returns a dict matching structure_events_summary schema.
    """
    aligned_fresh = 0
    counter_fresh = 0
    # Collect counter-fresh events with their seniority for escalation check.
    counter_seniorities: list[int] = []

    for ev in events:
        is_fresh = str(ev.status or "").strip().lower() == "fresh"
        if not is_fresh:
            continue
        if ev.direction is Direction.NEUTRAL:
            continue
        if ev.direction == mtf_direction:
            aligned_fresh += 1
        else:
            counter_fresh += 1
            sen = _MC_TF_SENIORITY.get(ev.timeframe.value, 0)
            counter_seniorities.append(sen)

    # Highest counter TF
    highest_counter_seniority = max(counter_seniorities) if counter_seniorities else 0
    highest_counter_tf: str | None = None
    if highest_counter_seniority > 0:
        for tf, sen in _MC_TF_SENIORITY.items():
            if sen == highest_counter_seniority:
                highest_counter_tf = tf
                break

    # Escalation: >= 2 distinct ascending seniority levels among counter-fresh events.
    unique_sorted = sorted(set(counter_seniorities))
    escalation_detected = len(unique_sorted) >= 2
    escalation_score = len(unique_sorted) if escalation_detected else 0

    # Build readable escalation sequence (TF names, ascending seniority)
    escalation_sequence: list[str] = []
    if escalation_detected:
        for sen in unique_sorted:
            for tf, s in _MC_TF_SENIORITY.items():
                if s == sen and sen > 0:
                    escalation_sequence.append(tf)
                    break

    # Counter classification based on highest seniority
    if highest_counter_seniority == 0:
        counter_classification = "none"
    elif highest_counter_seniority <= 2:
        counter_classification = "pullback_candidate"
    elif highest_counter_seniority == 3:
        counter_classification = "deep_pullback_watch"
    elif highest_counter_seniority == 4:
        counter_classification = "structural_watch"
    else:
        counter_classification = "reversal_candidate"

    return {
        "aligned_fresh_count": aligned_fresh,
        "counter_fresh_count": counter_fresh,
        "highest_counter_tf": highest_counter_tf,
        "highest_counter_seniority": highest_counter_seniority,
        "escalation_detected": escalation_detected,
        "escalation_sequence": escalation_sequence,
        "escalation_score": escalation_score,
        "counter_classification": counter_classification,
    }


def _mc_age_category(age_d1: int | None) -> str:
    """Map age_d1 to a categorical label. Pure."""
    if age_d1 is None:
        return "unknown"
    if age_d1 < _MC_AGE_YOUNG:
        return "young"
    if age_d1 < _MC_AGE_MATURE:
        return "standard"
    return "mature"


def _mc_divergence_context(rsi_by_tf: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Extract divergence context from rsi_by_tf. Pure."""
    confirmed_tfs: list[str] = []
    div_direction: str | None = None

    for tf, data in rsi_by_tf.items():
        if not isinstance(data, dict):
            continue
        if not data.get("div_confirmed"):
            continue
        div_dir = str(data.get("divergence") or "").strip()
        if div_dir in ("Bullish", "Bearish"):
            confirmed_tfs.append(tf)
            # First confirmed divergence direction wins (consistent per asset).
            if div_direction is None:
                div_direction = div_dir

    # rsi_gradient: H1 vs H4 momentum direction.
    # Threshold of 3 RSI points filters noise while remaining sensitive to
    # meaningful short-term vs medium-term RSI divergence.
    # Values: "rising" | "falling" | "neutral" | "unknown"
    _MC_RSI_GRADIENT_THRESHOLD: float = 3.0
    h1_val = safe_float((rsi_by_tf.get("H1") or {}).get("value"))
    h4_val = safe_float((rsi_by_tf.get("H4") or {}).get("value"))
    if h1_val is not None and h4_val is not None:
        diff = h1_val - h4_val
        if diff > _MC_RSI_GRADIENT_THRESHOLD:
            rsi_gradient = "rising"
        elif diff < -_MC_RSI_GRADIENT_THRESHOLD:
            rsi_gradient = "falling"
        else:
            rsi_gradient = "neutral"
    else:
        rsi_gradient = "unknown"

    return {
        "divergence_active": bool(confirmed_tfs),
        "divergence_confirmed_tfs": confirmed_tfs,
        "divergence_direction": div_direction,
        "rsi_gradient": rsi_gradient,
    }


def _mc_sr_context(
    zones: list[SRZone],
    mtf_direction: Direction,
    counter_present: bool = False,
) -> dict[str, Any]:
    """Summarise S/R proximity context. Pure.

    counter_present: True if at least one counter-fresh structure event exists.
    Used to compute sr_confluence_with_counter without coupling to events_summary
    inside this function.
    """
    wanted_side = "BUY" if mtf_direction is Direction.BULLISH else "SELL"
    counter_side = "SELL" if mtf_direction is Direction.BULLISH else "BUY"

    near: list[SRZone] = [
        z for z in zones
        if z.is_real_sr()
        and z.side == wanted_side
        and z.distance_pct <= _MC_SR_NEAR_PCT
    ]

    # Counter-side zones: zones aligned with the counter-trend direction.
    near_counter: list[SRZone] = [
        z for z in zones
        if z.is_real_sr()
        and z.side == counter_side
        and z.distance_pct <= _MC_SR_NEAR_PCT
    ]
    sr_confluence_with_counter = bool(counter_present and near_counter)

    if not near:
        return {
            "nearest_zone_present": False,
            "nearest_zone_distance_pct": None,
            "nearest_zone_side": None,
            "at_key_level": False,
            "key_level_type": None,
            "sr_confluence_with_counter": sr_confluence_with_counter,
        }
    best = min(near, key=lambda z: z.distance_pct)
    at_key = best.has_weekly or best.has_daily
    key_level_type: str | None = None
    if at_key:
        key_level_type = "W1" if best.has_weekly else "D1"
    return {
        "nearest_zone_present": True,
        "nearest_zone_distance_pct": round(best.distance_pct, 4),
        "nearest_zone_side": best.side,
        "at_key_level": at_key,
        "key_level_type": key_level_type,
        "sr_confluence_with_counter": sr_confluence_with_counter,
    }


def _mc_classify_market_state(
    mtf_pct: int,
    mtf_direction: Direction,
    events_summary: dict[str, Any],
    age_d1: int | None,
    divergence_ctx: dict[str, Any],
) -> tuple[str, str]:
    """Derive (market_state, structural_risk) deterministically.

    Rules applied in priority order — first match wins.
    Returns (market_state, structural_risk).
    Pure, no side effects.
    """
    # Insufficient data
    if mtf_direction is Direction.NEUTRAL or mtf_pct == 0:
        return "DATA_INCOMPLETE", "Undefined"

    counter_class = events_summary.get("counter_classification", "none")
    escalation = events_summary.get("escalation_detected", False)
    highest_sen = events_summary.get("highest_counter_seniority", 0)
    aligned_fresh = events_summary.get("aligned_fresh_count", 0)
    age_cat = _mc_age_category(age_d1)

    # Divergence amplifier: confirmed HTF divergence counter to MTF direction
    htf_div = False
    if divergence_ctx.get("divergence_active"):
        div_dir = divergence_ctx.get("divergence_direction")
        # Counter-to-MTF divergence on D1/W1
        confirmed_tfs = divergence_ctx.get("divergence_confirmed_tfs") or []
        on_htf = any(tf in _MC_DIV_HTF for tf in confirmed_tfs)
        if on_htf and div_dir is not None:
            if mtf_direction is Direction.BULLISH and div_dir == "Bearish":
                htf_div = True
            elif mtf_direction is Direction.BEARISH and div_dir == "Bullish":
                htf_div = True

    # No structure events at all — classify purely from MTF bias
    if aligned_fresh == 0 and highest_sen == 0:
        if mtf_pct < 60:
            return "RANGE_COMPRESSION", "Undefined"
        if mtf_pct >= 85:
            risk = "Low-Moderate" if htf_div else "Low"
            return "CLEAN_CONTINUATION", risk
        if mtf_pct >= 70:
            risk = "Low-Moderate"
            return "PULLBACK_CONTINUATION", risk
        if mtf_pct >= 60:
            return "TRANSITION_WATCH", "Moderate-High"
        return "RANGE_COMPRESSION", "Undefined"

    # REVERSAL_RISK — escalation full OR W1+ counter OR (D1 counter + htf div + mature)
    if escalation and highest_sen >= 4:
        return "REVERSAL_RISK", "Critical"
    if highest_sen >= 5:  # W1 or MN counter
        return "REVERSAL_RISK", "Critical"
    if highest_sen == 4 and htf_div and age_cat == "mature":
        return "REVERSAL_RISK", "Critical"

    # STRUCTURAL_CONFLICT — D1 counter without full escalation
    if highest_sen == 4:
        risk = "High"
        return "STRUCTURAL_CONFLICT", risk

    # TRANSITION_WATCH — MTF weakening + H4 counter
    if highest_sen == 3 and mtf_pct < 70:
        return "TRANSITION_WATCH", "Moderate-High"

    # DEEP_PULLBACK — H4 counter in moderate MTF
    if highest_sen == 3 and mtf_pct >= 70:
        return "DEEP_PULLBACK", "Moderate"

    # PULLBACK_CONTINUATION — H1/H4 counter in strong MTF
    if highest_sen in (2, 3) and mtf_pct >= 70:
        risk = "Low-Moderate"
        if htf_div and age_cat == "mature":
            risk = "Moderate"
        return "PULLBACK_CONTINUATION", risk

    # FIX-B: counter H1/M15 in mid MTF (50–70%) — conflicted structure, not range
    # Covers: AUD/CHF (Bear 58% + H1 Bull counter), GBP/NZD (Bull 56% + H1 Bear counter)
    if highest_sen <= 2 and aligned_fresh == 0 and 50 <= mtf_pct < 70:
        return "TRANSITION_WATCH", "Moderate-High"

    # CLEAN_CONTINUATION — no counter or only low-seniority counter in strong MTF
    if highest_sen <= 2 and mtf_pct >= 85 and aligned_fresh >= 1:
        risk = "Low"
        if htf_div:
            risk = "Low-Moderate"
        return "CLEAN_CONTINUATION", risk

    # FIX-A: aligned Fresh present but MTF too weak for CLEAN/PULLBACK
    # Covers: AUD/JPY (Bull 51% + H1 aligned Fresh, no counter)
    if aligned_fresh >= 1 and highest_sen == 0 and 40 <= mtf_pct < 60:
        return "TRANSITION_WATCH", "Moderate-High"

    # Fallback for ambiguous combinations
    if mtf_pct >= 60 and aligned_fresh >= 1:
        return "PULLBACK_CONTINUATION", "Low-Moderate"
    if mtf_pct < 60:
        return "RANGE_COMPRESSION", "Undefined"

    return "DATA_INCOMPLETE", "Undefined"


def _mc_build_confidence_drivers(
    mtf_direction: Direction,
    mtf_pct: int,
    mtf_biases: dict[str, str],
    events: list[StructureEvent],
    events_summary: dict[str, Any],
    divergence_ctx: dict[str, Any],
    sr_ctx: dict[str, Any],
    age_d1: int | None,
    age_cat: str,
) -> tuple[list[str], list[str]]:
    """Build (confidence_drivers, structural_risk_drivers) lists. Pure."""
    drivers: list[str] = []
    risk_drivers: list[str] = []

    # 1 — MTF statement (always)
    aligned_tfs = [
        tf for tf, bias in mtf_biases.items()
        if _direction_from_text(bias) == mtf_direction
    ]
    tf_list = "/".join(aligned_tfs) if aligned_tfs else "—"
    drivers.append(
        f"MTF {mtf_direction.value} {mtf_pct}% — alignement {tf_list}"
    )

    # 2 — Best aligned Fresh CHoCH (lowest candles_elapsed)
    aligned_fresh_events = [
        ev for ev in events
        if str(ev.status or "").strip().lower() == "fresh"
        and ev.direction == mtf_direction
    ]
    if aligned_fresh_events:
        best = min(aligned_fresh_events, key=lambda e: e.candles_elapsed)
        score_str = f"score={int(best.confluence_score)}" if best.confluence_score else ""
        candles_str = f"{best.candles_elapsed}c"
        drivers.append(
            f"{best.timeframe.value} CHoCH {best.direction.value} Fresh"
            f" ({score_str}, {candles_str}) — trigger aligné"
        )

    # 3 — Counter-trend statement
    counter_class = events_summary.get("counter_classification", "none")
    if counter_class != "none":
        highest_tf = events_summary.get("highest_counter_tf")
        counter_events = [
            ev for ev in events
            if str(ev.status or "").strip().lower() == "fresh"
            and ev.direction != mtf_direction
            and ev.direction is not Direction.NEUTRAL
        ]
        if counter_events and highest_tf:
            # Most recent counter event on the highest TF
            top_counter = [
                ev for ev in counter_events
                if ev.timeframe.value == highest_tf
            ]
            ev_ref = top_counter[0] if top_counter else counter_events[0]
            score_str = f"score={int(ev_ref.confluence_score)}" if ev_ref.confluence_score else ""
            drivers.append(
                f"{ev_ref.timeframe.value} CHoCH {ev_ref.direction.value} Fresh"
                f" ({score_str}) — {counter_class}"
            )
            risk_drivers.append(
                f"{ev_ref.timeframe.value} CHoCH {ev_ref.direction.value} Fresh"
                f" — séniorité {_MC_TF_SENIORITY.get(ev_ref.timeframe.value, 0)}/6"
            )

    # 4 — Escalation
    if events_summary.get("escalation_detected"):
        seq = events_summary.get("escalation_sequence") or []
        drivers.append(f"Escalade structurelle détectée : {' → '.join(seq)}")
        risk_drivers.append(f"Escalade {' → '.join(seq)}")

    # 5 — Divergence
    if divergence_ctx.get("divergence_active"):
        div_tfs = divergence_ctx.get("divergence_confirmed_tfs") or []
        div_dir = divergence_ctx.get("divergence_direction") or "—"
        drivers.append(
            f"Divergence RSI {div_dir} confirmée sur {'/'.join(div_tfs)}"
        )
        # Risk driver only if counter-to-MTF on HTF
        on_htf = any(tf in _MC_DIV_HTF for tf in div_tfs)
        is_counter = (
            (mtf_direction is Direction.BULLISH and div_dir == "Bearish")
            or (mtf_direction is Direction.BEARISH and div_dir == "Bullish")
        )
        if on_htf and is_counter:
            risk_drivers.append(
                f"Divergence RSI {div_dir} confirmée sur {'/'.join(div_tfs)} — counter-MTF"
            )

    # 6 — S/R context
    if sr_ctx.get("nearest_zone_present"):
        dist = sr_ctx.get("nearest_zone_distance_pct")
        side = sr_ctx.get("nearest_zone_side")
        key_lbl = " (niveau clé W1/D1)" if sr_ctx.get("at_key_level") else ""
        drivers.append(
            f"Zone S/R {side} proche à {dist}%{key_lbl}"
        )

    # 7 — Age context
    if age_d1 is not None:
        if age_cat == "mature":
            drivers.append(f"Actif mature {age_d1}j — surveillance distribution")
            risk_drivers.append(f"age_d1={age_d1}j — structure mature")
        elif age_cat == "young":
            drivers.append(f"Structure récente {age_d1}j — tendance en établissement")

    return drivers, risk_drivers


def _build_market_context(asset: CanonicalAsset) -> dict[str, Any]:
    """Compute the full market_context dict for a CanonicalAsset.

    Pure function — reads asset fields, returns a plain dict.
    No side effects, no mutation, no I/O.
    Identical inputs → identical outputs.
    """
    mtf = asset.mtf
    if mtf is None:
        return {"market_state": "DATA_INCOMPLETE", "structural_risk": "Undefined"}

    mtf_direction = mtf.direction
    mtf_pct = mtf.pct
    mtf_biases = mtf.biases or {}
    age_d1 = mtf.age_d1
    age_cat = _mc_age_category(age_d1)

    events = list(asset.structure_events)
    rsi_by_tf = asset.rsi_by_tf or {}
    zones = list(asset.zones)

    # HTF alignment summary
    htf_tfs_aligned = [
        tf for tf, bias in mtf_biases.items()
        if tf in _MC_TF_SENIORITY
        and _MC_TF_SENIORITY[tf] >= 4  # D1 and above
        and _direction_from_text(bias) == mtf_direction
    ]
    conflict_tfs = [
        tf for tf, bias in mtf_biases.items()
        if tf in _MC_TF_SENIORITY
        and _MC_TF_SENIORITY[tf] >= 4
        and _direction_from_text(bias) not in (mtf_direction, Direction.NEUTRAL)
    ]

    # Sub-contexts (pure)
    events_summary = _mc_classify_structure_events(events, mtf_direction)
    divergence_ctx = _mc_divergence_context(rsi_by_tf)
    counter_present = events_summary.get("counter_fresh_count", 0) > 0
    sr_ctx = _mc_sr_context(zones, mtf_direction, counter_present=counter_present)

    # Market state + risk
    market_state, structural_risk = _mc_classify_market_state(
        mtf_pct, mtf_direction, events_summary, age_d1, divergence_ctx
    )

    # Drivers
    confidence_drivers, structural_risk_drivers = _mc_build_confidence_drivers(
        mtf_direction, mtf_pct, mtf_biases, events,
        events_summary, divergence_ctx, sr_ctx, age_d1, age_cat,
    )

    # RSI H4 quick summary
    h4_rsi = rsi_by_tf.get("H4") or {}
    rsi_h4_status = h4_rsi.get("status")
    rsi_h4_value = safe_float(h4_rsi.get("value"))

    # counter_trend_classification: structured bloc derived from already-computed
    # events_summary + divergence_ctx. No new calculation.
    counter_class = events_summary.get("counter_classification", "none")
    highest_counter_tf = events_summary.get("highest_counter_tf")
    highest_counter_sen = events_summary.get("highest_counter_seniority", 0)
    # htf_div: counter-to-MTF divergence confirmed on D1/W1 — mirrors logic in
    # _mc_classify_market_state to remain consistent without duplication.
    div_dir = divergence_ctx.get("divergence_direction")
    div_confirmed_tfs = divergence_ctx.get("divergence_confirmed_tfs") or []
    on_htf_div = any(tf in _MC_DIV_HTF for tf in div_confirmed_tfs)
    aligns_with_divergence = bool(
        on_htf_div
        and div_dir is not None
        and (
            (mtf_direction is Direction.BULLISH and div_dir == "Bearish")
            or (mtf_direction is Direction.BEARISH and div_dir == "Bullish")
        )
    )
    age_d1_modifier_applied = age_cat == "mature" and counter_class != "none"

    # Build human-readable description for UI / LLM consumption.
    if counter_class == "none":
        ctc_description = "Aucun CHoCH counter-MTF détecté"
    else:
        age_suffix = f" (actif mature {age_d1}j)" if age_d1_modifier_applied else ""
        div_suffix = " + divergence HTF confirmée" if aligns_with_divergence else ""
        ctc_description = (
            f"{highest_counter_tf} CHoCH counter dans MTF"
            f" {mtf_direction.value} — {counter_class}{age_suffix}{div_suffix}"
        )

    counter_trend_classification = {
        "present": counter_class != "none",
        "class": counter_class,
        "dominant_tf": highest_counter_tf,
        "aligns_with_divergence": aligns_with_divergence,
        "age_d1_modifier_applied": age_d1_modifier_applied,
        "description": ctc_description,
    }

    # weakening_trend: MTF directional but no aligned trigger and counter pressure
    # present. Derived from raw inputs — no dependency on market_state.
    aligned_fresh = events_summary.get("aligned_fresh_count", 0)
    weakening_trend = bool(
        mtf_pct < 70
        and aligned_fresh == 0
        and counter_present
    )

    # compression_detected: direct projection of RANGE_COMPRESSION state.
    compression_detected = market_state == "RANGE_COMPRESSION"

    return {
        "market_state": market_state,
        "structural_risk": structural_risk,
        "mtf_alignment": {
            "direction": mtf_direction.value,
            "pct": mtf_pct,
            "htf_anchor": bool(len(htf_tfs_aligned) >= 2),
            "htf_tfs_aligned": htf_tfs_aligned,
            "conflict_tfs": conflict_tfs,
        },
        "structure_events_summary": events_summary,
        "counter_trend_classification": counter_trend_classification,
        "momentum_context": {
            "rsi_h4_status": rsi_h4_status,
            "rsi_h4_value": rsi_h4_value,
            **divergence_ctx,
        },
        "sr_context": sr_ctx,
        "transition_signals": {
            "age_d1": age_d1,
            "age_d1_category": age_cat,
            "distribution_phase_risk": (
                age_cat == "mature"
                and events_summary.get("counter_classification") in (
                    "structural_watch", "reversal_candidate"
                )
            ),
            "weakening_trend": weakening_trend,
            "compression_detected": compression_detected,
        },
        "confidence_drivers": confidence_drivers,
        "structural_risk_drivers": structural_risk_drivers,
    }



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


# v3.3 (GAP 7): relaxed threshold for aligned zone detection.
_ALIGNED_ZONE_MAX_DIST_PCT: Final[float] = 5.0


def _split_zones_by_alignment(
    asset: CanonicalAsset, direction: Direction
) -> tuple[list[SRZone], list[SRZone]]:
    aligned: list[SRZone] = []
    opposite: list[SRZone] = []
    if direction is Direction.NEUTRAL:
        return aligned, opposite
    for z in asset.zones:
        if z.distance_pct > _ALIGNED_ZONE_MAX_DIST_PCT:
            continue
        if direction is Direction.BULLISH:
            if z.side == "BUY":
                aligned.append(z)
            elif z.side == "SELL":
                opposite.append(z)
        else:  # BEARISH
            if z.side == "SELL":
                aligned.append(z)
            elif z.side == "BUY":
                opposite.append(z)
    return aligned, opposite


class EnrichmentEngine:
    """Computes HTF alignment, confluence, TP zones, SL/TP1/RR and the
    v3.4 typed `precomputed` block for each event."""

    def enrich(
        self, assets: dict[str, CanonicalAsset]
    ) -> Result[list[EnrichedSignal]]:
        signals: list[EnrichedSignal] = []
        res: Result[list[EnrichedSignal]] = Result(value=signals)
        for asset in assets.values():
            if self._enrich_asset(asset, signals, res):
                break
        res.add(Diagnostic(
            "enrich", Severity.INFO, "summary",
            f"enriched {len(signals)} signals from {len(assets)} assets",
        ))
        return res

    def _enrich_asset(
        self,
        asset: CanonicalAsset,
        signals: list[EnrichedSignal],
        res: Result[list[EnrichedSignal]],
    ) -> bool:
        for event in asset.structure_events:
            if len(signals) >= MAX_SIGNALS_OUT:
                res.add(Diagnostic(
                    "enrich", Severity.WARNING, "cap_reached",
                    f"MAX_SIGNALS_OUT={MAX_SIGNALS_OUT}",
                ))
                return True
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
        return False

    def _build_signal(
        self, asset: CanonicalAsset, event: StructureEvent
    ) -> EnrichedSignal:
        aligned, opposite = _split_zones_by_alignment(asset, event.direction)
        nearest = self._select_nearest_aligned_zone(asset, event, aligned)
        tp_zones = opposite[:MAX_TP_ZONES]
        htf_aligned = self._htf_aligned(asset, event)
        confluence = self._confluence(asset, event)

        # v3.4: SL/TP/RR now use atr_effective (cascade-aware)
        sl_price, tp1_price, rr, sl_mult = self._compute_sl_tp_rr(
            asset, event, nearest, tp_zones
        )

        # TP1 ATR multiple is computed using atr_effective when available.
        tp1_atr = None
        if (
            tp1_price is not None
            and event.level is not None
            and asset.atr_effective is not None
            and asset.atr_effective > 0
        ):
            tp1_atr = round(
                abs(tp1_price - event.level) / asset.atr_effective, 2
            )

        precomputed = self._build_precomputed(asset, event, htf_aligned, sl_mult)

        return EnrichedSignal(
            event=event,
            asset=asset,
            htf_aligned=htf_aligned,
            nearest_aligned_zone=nearest,
            tp_zones=tp_zones,
            confluence_total=confluence,
            sl_price=sl_price,
            sl_atr_multiple=sl_mult,
            tp1_price=tp1_price,
            tp1_atr_multiple=tp1_atr,
            rr_estimated=rr,
            enrichment=self._enrichment_quality(asset),
            warnings=self._warnings(asset, event),
            precomputed=precomputed,
        )

    # ── v3.4: typed precomputed block (consumed verbatim by prompt v9.0) ─
    @staticmethod
    def _build_precomputed(
        asset: CanonicalAsset,
        event: StructureEvent,
        htf_aligned: bool,
        _sl_mult: float,  # BB-regime mult pre-computed by caller; kept for API symmetry
    ) -> SignalPrecomputed:
        bb_mult = _BB_REGIME_SL_MULT.get(
            event.bb_regime or "", _SL_RAW_DEFAULT_MULT
        )
        atr_eff = asset.atr_effective
        sl_distance_min: float | None = None
        sl_distance_raw: float | None = None
        if atr_eff is not None and _is_finite_number(atr_eff) and atr_eff > 0:
            sl_distance_min = round(atr_eff * _SL_FLOOR_MULT, 8)
            sl_distance_raw = round(atr_eff * bb_mult, 8)

        h4_view = asset.rsi_by_tf.get(Timeframe.H4.value) or {}
        rsi_h4_value = safe_float(h4_view.get("value"))

        candles = safe_int(event.candles_elapsed, default=999)
        # sig_fresh_aligned: Fresh + direction match + ≤2 candles since signal.
        # event.status is free-form text from the scanner; we lowercase-compare.
        is_fresh = str(event.status or "").strip().lower() == "fresh"
        sig_fresh_aligned = bool(
            is_fresh
            and event.direction is not Direction.NEUTRAL
            and asset.mtf is not None
            and event.direction == asset.mtf.direction
            and candles <= _FRESH_CANDLES_MAX
        )

        return SignalPrecomputed(
            atr_effective=atr_eff,
            atr_source=asset.atr_source,
            bb_mult=bb_mult,
            sl_distance_min=sl_distance_min,
            sl_distance_raw=sl_distance_raw,
            rsi_h4_value=rsi_h4_value,
            rsi_h4_status=asset.rsi_h4_status,
            candles_elapsed=candles,
            sig_fresh_aligned=sig_fresh_aligned,
            htf_aligned=htf_aligned,
            conviction_cap=asset.conviction_cap,
            # DIR-1: enrichissement directionnel GPS vs CHoCH — purement informatif.
            # Aucun filtre, aucun impact sur SL/TP/score/ranking.
            gps_direction=asset.mtf.direction if asset.mtf is not None else None,
            choch_direction=event.direction if event.direction is not Direction.NEUTRAL else None,
            direction_aligned=(
                event.direction == asset.mtf.direction
                if asset.mtf is not None and event.direction is not Direction.NEUTRAL
                else None
            ),
            counter_trend_signal=(
                event.direction != asset.mtf.direction
                if asset.mtf is not None and event.direction is not Direction.NEUTRAL
                else None
            ),
            alignment_score=(
                100 if event.direction == asset.mtf.direction
                else 0
                if asset.mtf is not None and event.direction is not Direction.NEUTRAL
                else None
            ),
        )

    # ── BUG 1 FIX: htf_aligned requires BOTH D1 AND H4 aligned ────────────
    @staticmethod
    def _htf_aligned(asset: CanonicalAsset, event: StructureEvent) -> bool:
        if asset.mtf is None:
            return False
        if event.direction is Direction.NEUTRAL:
            return False
        d1_bias = asset.mtf.biases.get(Timeframe.D1.value)
        h4_bias = asset.mtf.biases.get(Timeframe.H4.value)
        if not d1_bias or not h4_bias:
            return False
        d1_dir = _direction_from_text(d1_bias)
        h4_dir = _direction_from_text(h4_bias)
        if d1_dir is Direction.NEUTRAL or h4_dir is Direction.NEUTRAL:
            return False
        return d1_dir == event.direction and h4_dir == event.direction

    @staticmethod
    def _select_nearest_aligned_zone(
        _asset: CanonicalAsset,  # reserved for future alignment filtering
        _event: StructureEvent,  # reserved for future timeframe filtering
        aligned_zones: list[SRZone],
    ) -> SRZone | None:
        if not aligned_zones:
            return None
        real = [z for z in aligned_zones if z.is_real_sr()]
        if real:
            real.sort(key=lambda z: z.distance_pct)
            return real[0]
        aligned_sorted = sorted(aligned_zones, key=lambda z: z.distance_pct)
        return aligned_sorted[0]

    @staticmethod
    def _confluence(asset: CanonicalAsset, event: StructureEvent) -> float:
        total = event.confluence_score or 0.0
        if asset.mtf is not None:
            total += asset.mtf.pct * 0.5
        for z in asset.zones:
            if not z.is_real_sr():
                continue
            total += z.weighted_score * 0.1
        return round(total, 2)

    @staticmethod
    def _resolve_tp1_from_zones(
        direction: Direction,
        tp_zones: list[SRZone],
        price_context: PriceContext | None,
    ) -> float | None:
        """Resolve TP1 price: real SR zones first, price_context fallback,
        then any synthetic zone. Extracted from _compute_sl_tp_rr to
        reduce cyclomatic complexity."""
        real_tp = [z for z in tp_zones if z.is_real_sr()]
        if real_tp:
            return real_tp[0].level
        if price_context is not None:
            if direction is Direction.BULLISH and price_context.resistance_level is not None:
                return price_context.resistance_level
            if direction is Direction.BEARISH and price_context.support_level is not None:
                return price_context.support_level
        if tp_zones:
            return tp_zones[0].level
        return None

    # ── GAP 6 + v3.4: SL / TP1 / RR using atr_effective ───────────────────
    @staticmethod
    def _compute_sl_tp_rr(
        asset: CanonicalAsset,
        event: StructureEvent,
        _nearest: SRZone | None,  # reserved — TP1 currently derived from tp_zones
        tp_zones: list[SRZone],
    ) -> tuple[float | None, float | None, float | None, float]:
        """Compute SL / TP1 / RR. v3.4 uses asset.atr_effective (cascade).
        Returns (sl_price, tp1_price, rr, sl_mult_actually_used).
        sl_mult is the BB-regime multiplier (or 1.1 fallback) so callers can
        persist it on EnrichedSignal.sl_atr_multiple."""
        level = event.level
        sl_mult = _BB_REGIME_SL_MULT.get(
            event.bb_regime or "", _SL_RAW_DEFAULT_MULT
        )
        if level is None or not _is_finite_number(level) or level <= 0:
            return None, None, None, sl_mult

        atr_eff = asset.atr_effective
        if atr_eff is None or not _is_finite_number(atr_eff) or atr_eff <= 0:
            sl_price: float | None = None
        else:
            if event.direction is Direction.BULLISH:
                sl_price = round(level - sl_mult * atr_eff, 5)
            elif event.direction is Direction.BEARISH:
                sl_price = round(level + sl_mult * atr_eff, 5)
            else:
                sl_price = None

        tp1_price: float | None = None
        if event.direction in (Direction.BULLISH, Direction.BEARISH):
            raw_tp1 = EnrichmentEngine._resolve_tp1_from_zones(
                event.direction, tp_zones, asset.price_context
            )
            if raw_tp1 is not None:
                tp1_price = round(raw_tp1, 5)

        rr: float | None = None
        if (
            sl_price is not None
            and tp1_price is not None
            and _is_finite_number(sl_price)
            and _is_finite_number(tp1_price)
        ):
            risk = abs(level - sl_price)
            reward = abs(tp1_price - level)
            if risk > 0:
                rr = round(reward / risk, 2)
        return sl_price, tp1_price, rr, sl_mult

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
        if asset.atr_effective is None:
            w.append("atr_cascade_exhausted")
        elif asset.atr_source and asset.atr_source != "h4":
            w.append(f"atr_source={asset.atr_source}")
        return w


# ════════════════════════════════════════════════════════════════════════════
# CORRELATION
# ════════════════════════════════════════════════════════════════════════════
_QUALITY_RANK: Final[dict[str, int]] = {"A+": 4, "A": 3, "B+": 2, "B": 1}


class CorrelationEngine:
    """Groups signals by base/quote currency for cross-pair correlation."""

    def build(
        self, signals: list[EnrichedSignal]
    ) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for s in signals:
            self._append_signal_legs(s, groups)
        return self._finalize_groups(groups)

    @staticmethod
    def _append_signal_legs(
        s: EnrichedSignal, groups: dict[str, list[dict[str, Any]]]
    ) -> None:
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

    @staticmethod
    def _finalize_groups(
        groups: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
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
        "type": z.type,          # MERGE-3: "Support" | "Resistance" | "Pivot"
        "strength": z.strength,  # MERGE-3: Force Totale from SR scanner
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
            res.add(Diagnostic(
                "pipeline", Severity.ERROR, "no_input", "no files provided"
            ))
            return res
        partials, scanners, unknown = self._adapt_phase(files, diags)
        assets = self._merge_phase(partials, diags)
        signals = self._enrich_phase(assets, diags)
        groups, hot, top = self._post_phase(assets, signals)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        out = MergeOutput(
            meta=MergeMeta(
                generated_at=datetime.now(timezone.utc),
                scanners_detected=scanners,
                scanners_unknown=unknown,
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

    def _merge_phase(
        self,
        partials: list[list[CanonicalAsset]],
        diags: list[Diagnostic],
    ) -> dict[str, CanonicalAsset]:
        merged_r = self._merger.merge(partials)
        diags.extend(merged_r.diagnostics)
        return merged_r.value or {}

    def _enrich_phase(
        self,
        assets: dict[str, CanonicalAsset],
        diags: list[Diagnostic],
    ) -> list[EnrichedSignal]:
        enriched_r = self._enricher.enrich(assets)
        diags.extend(enriched_r.diagnostics)
        return enriched_r.value or []

    def _post_phase(
        self,
        assets: dict[str, CanonicalAsset],
        signals: list[EnrichedSignal],
    ) -> tuple[
        dict[str, list[dict[str, Any]]],
        list[dict[str, Any]],
        dict[str, list[dict[str, Any]]],
    ]:
        groups, _ = _safe_call(
            "pipeline.correlate", "correlate_crash",
            lambda s=signals: self._correlator.build(s),
            cast(dict[str, list[dict[str, Any]]], {}),
        )
        hot, _ = _safe_call(
            "pipeline.hot_zones", "hot_zones_crash",
            lambda a=assets: self._hot_zones(a),
            cast(list[dict[str, Any]], []),
        )
        top, _ = _safe_call(
            "pipeline.top_consensus", "top_consensus_crash",
            lambda a=assets: self._top_consensus(a),
            cast(dict[str, list[dict[str, Any]]], {}),
        )
        return groups, hot, top

    # ── P3 FIX: hot_zones excludes synthetic / invalid zones ───────────────
    @staticmethod
    def _hot_zones(
        assets: dict[str, CanonicalAsset]
    ) -> list[dict[str, Any]]:
        zones: list[dict[str, Any]] = []
        soft_cap = MAX_HOT_ZONES_OUT * 2
        for sym, asset in assets.items():
            for z in asset.zones:
                if not z.is_real_sr():
                    continue
                if z.distance_pct >= 2.0:
                    continue
                # FIX-006: log discret si zone UNKNOWN devient hot (aide debug)
                if z.side == "UNKNOWN" and z.alert == "🔥 ZONE CHAUDE":
                    _LOG.debug("hot_zone_primary UNKNOWN pour %s (niveau %s)", sym, z.level)
                zones.append({"symbol": sym, **_zone_dict(z)})
                if len(zones) >= soft_cap:
                    break
            if len(zones) >= soft_cap:
                break
        zones.sort(key=lambda x: safe_float(x["distance_pct"]) or 999.0)
        return zones[:MAX_HOT_ZONES_OUT]

    @staticmethod
    def _top_consensus(
        assets: dict[str, CanonicalAsset],
        min_pct_bull: int = _TOP_CONSENSUS_MIN_PCT_BULL,
        min_pct_bear: int = _TOP_CONSENSUS_MIN_PCT_BEAR,
        top_n: int = 5,
    ) -> dict[str, list[dict[str, Any]]]:
        bull: list[dict[str, Any]] = []
        bear: list[dict[str, Any]] = []
        for sym, asset in assets.items():
            if asset.mtf is None:
                continue
            entry = {
                "symbol": sym,
                "mtf_pct": asset.mtf.pct,
                "quality": asset.mtf.quality,
                "nc": asset.mtf.nc,
                "age_d1": asset.mtf.age_d1,
            }
            if asset.mtf.direction is Direction.BULLISH and asset.mtf.pct >= min_pct_bull:
                bull.append(entry)
            elif asset.mtf.direction is Direction.BEARISH and asset.mtf.pct >= min_pct_bear:
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
    return json.dumps(
        payload, indent=indent, ensure_ascii=False, default=_json_default
    )


# ════════════════════════════════════════════════════════════════════════════
# STREAMLIT — caching layer (content-addressable, never hashes raw bytes)
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True, slots=True)
class FileEntry:
    """Content-addressed file. Streamlit hashes via ``__hash__``/``__eq__``,
    which use only the SHA-256 fingerprint — never the raw bytes.
    ``__hash__`` and ``__eq__`` are intentional overrides: frozen dataclasses
    generate an ``__eq__`` that compares all fields (including ``data``),
    which would be prohibitively slow and would break Streamlit's cache key.
    The ``type: ignore[override]`` annotations below suppress the mypy false
    positive that arises because frozen dataclasses generate a ``__hash__``
    whose signature is technically identical — not a real conflict.
    """
    name: str
    sha256: str
    data: bytes = field(repr=False)

    def __hash__(self) -> int:  # type: ignore[override]
        return hash((self.name, self.sha256))

    def __eq__(self, other: object) -> bool:  # type: ignore[override]
        if not isinstance(other, FileEntry):
            return NotImplemented
        return self.name == other.name and self.sha256 == other.sha256


def _make_file_entry(name: str, data: bytes) -> FileEntry:
    return FileEntry(name=name, sha256=hashlib.sha256(data).hexdigest(), data=data)


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


@st.cache_data(show_spinner=False, max_entries=64, ttl=3600, persist=False)  # FIX-005: 128→64
def parse_json_bytes(entry: FileEntry) -> tuple[Any | None, str | None]:
    """Cache-friendly JSON parsing — keyed on (name, sha256), not raw bytes."""
    data = entry.data
    name = entry.name
    if not data:
        return None, f"{name}: empty file"
    if len(data) > MAX_FILE_SIZE_BYTES:
        return None, (
            f"{name}: file too large ({len(data)} > {MAX_FILE_SIZE_BYTES} bytes)"
        )
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            return None, f"{name}: encoding error ({exc})"
    try:
        return json.loads(text), None
    except json.JSONDecodeError as exc:
        return None, (
            f"{name}: invalid JSON at line {exc.lineno} col {exc.colno} "
            f"({exc.msg})"
        )
    except (RecursionError, MemoryError) as exc:
        return None, f"{name}: resource error ({type(exc).__name__})"
    except ValueError as exc:
        return None, f"{name}: {type(exc).__name__}: {exc}"


def _files_fingerprint(entries: Sequence[FileEntry]) -> str:
    """Deterministic combined fingerprint of multiple files."""
    h = hashlib.sha256()
    for e in sorted(entries, key=lambda x: (x.name, x.sha256)):
        h.update(e.name.encode("utf-8"))
        h.update(b"\x00")
        h.update(e.sha256.encode("ascii"))
        h.update(b"\x00")
    return h.hexdigest()


@st.cache_data(show_spinner=False, max_entries=16, ttl=1800, persist=False)
def run_pipeline_cached(
    fingerprint: str, entries: tuple[FileEntry, ...]
) -> dict[str, Any]:
    """Cached pipeline run. Returns a serializable dict."""
    _ = fingerprint
    pipeline = get_pipeline()
    ingested: list[IngestedFile] = []
    parse_errors: list[str] = []
    for entry in entries:
        payload, err = parse_json_bytes(entry)
        if err is not None:
            parse_errors.append(err)
            continue
        ingested.append(IngestedFile(name=entry.name, payload=payload))

    result, crash_diag = _safe_call(
        "pipeline", "pipeline_crash",
        lambda p=pipeline, i=ingested: p.run(i),
        Result(value=cast(MergeOutput | None, None)),
        severity=Severity.CRITICAL,
    )
    if crash_diag is not None:
        result.add(crash_diag)
        _LOG.error("pipeline crashed; degraded result returned")

    output_dict: dict[str, Any] | None = None
    if result.value is not None:
        output_dict = result.value.model_dump(mode="json")

    return {
        "ok": result.ok,
        "parse_errors": parse_errors,
        "output": output_dict,
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
_ATR_SRC_BADGE: Final[dict[str, str]] = {
    "h4": "🟢H4",
    "h1_proxy": "🟡H1×1.8",
    "d1_proxy": "🟠D1×0.25",
    "synthetic": "🔴SYNTH",
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
            BLUESTAR MERGE
            <span style="opacity:.6;font-size:14px">v{SCHEMA_VERSION}</span>
          </div>
          <div style="font-family:monospace;font-size:11px;opacity:.85">
            Auto-detection · Canonical pivot · Pre-computation layer for v9.0 prompt
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
    cols[5].metric(
        "Latence", f"{safe_float(meta.get('elapsed_ms')) or 0.0:.0f} ms"
    )


def _zone_text(nz: dict[str, Any] | None) -> str:
    if not nz or not isinstance(nz, dict):
        return "no aligned zone"
    status = nz.get("status")
    synth = " ⚠️synth" if status in _INVALID_ZONE_STATUSES else ""
    return (
        f"@ `{nz.get('level')}` "
        f"(d={safe_float(nz.get('distance_pct')) or 0.0:.2f}%, "
        f"sc={nz.get('score')}, {status}){synth}"
    )


def _fmt_price(v: Any) -> str:
    f = safe_float(v)
    if f is None:
        return "—"
    return f"{f:.5f}" if f < 10 else f"{f:.2f}"


def _build_trade_txt(s: dict[str, Any]) -> str:
    """Format SL / TP1 / RR inline badges for a signal row."""
    parts: list[str] = []
    sl = s.get("sl_price")
    tp1 = s.get("tp1_price")
    rr = s.get("rr_estimated")
    if sl is not None:
        parts.append(f"SL={_fmt_price(sl)}")
    if tp1 is not None:
        parts.append(f"TP1={_fmt_price(tp1)}")
    if rr is not None:
        parts.append(f"RR={rr:.2f}")
    return (" · " + " ".join(parts)) if parts else ""


def _build_extra_txt(s: dict[str, Any]) -> str:
    """Format v3.4 pre-computed badges (ATR source, fresh, conviction cap)."""
    pre = s.get("precomputed") or {}
    asset = s.get("asset") or {}
    parts: list[str] = []
    atr_src = pre.get("atr_source") or asset.get("atr_source")
    if atr_src:
        parts.append(_ATR_SRC_BADGE.get(atr_src, atr_src))
    if pre.get("sig_fresh_aligned"):
        parts.append("🔥FRESH")
    cap = pre.get("conviction_cap") or asset.get("conviction_cap")
    if cap:
        parts.append(f"cap≤{cap}")
    return (" · " + " ".join(parts)) if parts else ""


def _render_one_signal(s: dict[str, Any]) -> None:
    ev = s.get("event") or {}
    asset = s.get("asset") or {}
    enr = s.get("enrichment") or {}
    status = str(enr.get("status", "empty"))
    badge = _STATUS_BADGE.get(status, "⚪")
    htf = "✅" if s.get("htf_aligned") else "⚠️"
    zone_txt = _zone_text(s.get("nearest_aligned_zone"))
    warns = s.get("warnings") or []
    warn_txt = f" ⚡{len(warns)}w" if warns else ""
    trade_txt = _build_trade_txt(s)
    extra_txt = _build_extra_txt(s)
    st.markdown(
        f"- {badge}  `{asset.get('symbol', '?')}` "
        f"[{ev.get('timeframe', '?')}]  {ev.get('direction', '?')}  ·  "
        f"HTF {htf} · {zone_txt} ·  "
        f"confluence={s.get('confluence_total', 0)}{warn_txt}{trade_txt}{extra_txt}"
    )


def _render_signals(signals: list[dict[str, Any]]) -> None:
    if not signals:
        st.info("Aucun signal de structure trouvé dans les fichiers fournis.")
        return
    st.subheader(f"📊 Signaux enrichis ({len(signals)})")
    ui_cap = 200
    for s in signals[:ui_cap]:
        _render_one_signal(s)
    if len(signals) > ui_cap:
        extra = len(signals) - ui_cap
        st.caption(
            f"({extra} signaux supplémentaires masqués — "
            f"exporter le JSON pour la liste complète)"
        )


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
        st.markdown("*aucun*")
        return
    for e in entries:
        symbol = e.get("symbol", "?")
        pct = e.get("mtf_pct", "?")
        quality = e.get("quality") or "?"
        nc = e.get("nc")
        st.markdown(f"-  `{symbol}`  · {pct}% · Q={quality} · NC={nc}")


def _hot_zone_tags(z: dict[str, Any]) -> str:
    parts = [
        t for t in (
            "W" if z.get("has_weekly") else "",
            "D" if z.get("has_daily") else "",
            "H4" if z.get("has_h4") else "",
        ) if t
    ]
    return " ".join(parts) or "—"


def _render_hot_zones(hot: list[dict[str, Any]]) -> None:
    if not hot:
        return
    with st.expander(f"🔥 Zones chaudes ({len(hot)})"):
        for z in hot[:50]:
            dist = safe_float(z.get("distance_pct")) or 0.0
            st.markdown(
                f"-  `{z.get('symbol', '?')}`  {z.get('side', '?')}  "
                f"@  `{z.get('level')}` "
                f"(d={dist:.2f}%, sc={z.get('weighted_score')},  "
                f"TF={_hot_zone_tags(z)}, {z.get('status')})  "
                f"{z.get('alert') or ''}"
            )


def _render_correlations(groups: dict[str, list[dict[str, Any]]]) -> None:
    if not groups:
        return
    with st.expander(f"🔗 Clusters de corrélation ({len(groups)})"):
        for leg, entries in groups.items():
            dirs = {e.get("direction") for e in entries}
            flag = "✅" if len(dirs) == 1 else "⚠️"
            members = " ·  ".join(
                f"`{e.get('symbol', '?')}`  {e.get('direction', '?')}"
                for e in entries[:20]
            )
            extra = "" if len(entries) <= 20 else f"  (+{len(entries) - 20})"
            st.markdown(f"**{leg}**  {flag} · {members}{extra}")


def _diag_context_text(ctx: Any) -> str:
    if not ctx:
        return ""
    try:
        s = json.dumps(ctx, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return ""
    return f" · `{s[:120]}`"


def _render_diagnostics(diags: list[dict[str, Any]]) -> None:
    if not diags:
        return
    severities = [d.get("severity", "info") for d in diags]
    err = sum(1 for s in severities if s in ("error", "critical"))
    warn = sum(1 for s in severities if s == "warning")
    info = sum(1 for s in severities if s == "info")
    label = (
        f"🔧 Diagnostics · {len(diags)} total ·  "
        f"{err}🔴 · {warn}🟡 · {info}🔵"
    )
    with st.expander(label):
        priority = {
            "critical": 0, "error": 1, "warning": 2, "info": 3, "debug": 4,
        }
        ordered = sorted(
            diags, key=lambda x: priority.get(x.get("severity", "info"), 9)
        )
        ui_cap = 500
        for d in ordered[:ui_cap]:
            icon = _SEV_ICON.get(d.get("severity", "info"), "•")
            ctx_txt = _diag_context_text(d.get("context") or {})
            st.markdown(
                f"{icon}  `[{d.get('stage')}/{d.get('code')}]` "
                f"{d.get('message')}{ctx_txt}"
            )
        if len(ordered) > ui_cap:
            extra = len(ordered) - ui_cap
            st.caption(f"({extra} diagnostics supplémentaires masqués)")


def _render_export(payload_dict: dict[str, Any]) -> None:
    payload, diag = _safe_call(
        "ui.export", "serialize_crash",
        lambda p=payload_dict: json.dumps(
            p, indent=2, ensure_ascii=False, default=_json_default
        ),
        None,
    )
    if payload is None:
        msg = diag.message if diag else "unknown"
        st.error(f"Export JSON impossible: {msg}")
        return
    fname = (
        f"merged_pipeline_"
        f"{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json"
    )
    st.download_button(
        "📥 Télécharger merged_pipeline.json",
        data=payload.encode("utf-8"),
        file_name=fname,
        mime="application/json",
        use_container_width=True,
        type="primary",
    )
    with st.expander("Prévisualiser JSON (4000 premiers caractères)"):
        st.code(
            payload[:4000] + ("\n…" if len(payload) > 4000 else ""),
            language="json",
        )


def _render_asset_browser(assets: dict[str, Any]) -> None:
    if not assets:
        return
    with st.expander(f"🔍 Explorer actifs canoniques ({len(assets)})"):
        symbols = sorted(assets.keys())
        selected = st.selectbox("Symbole", symbols, key="asset_browser")
        if selected:
            st.json(assets[selected], expanded=False)


# ──── Upload handling ─────────────────────────────────────────────────────
def _read_one_upload(f: Any) -> tuple[bytes | None, str | None]:
    try:
        f.seek(0)
        data = f.read()
    except (OSError, IOError, AttributeError) as exc:
        name = getattr(f, "name", "?")
        return None, (
            f"Lecture impossible de `{name}` : {type(exc).__name__}: {exc}"
        )
    if not isinstance(data, (bytes, bytearray)):
        try:
            data = str(data).encode("utf-8")
        except (UnicodeEncodeError, TypeError) as exc:
            name = getattr(f, "name", "?")
            return None, (
                f"Encodage impossible de `{name}` : {type(exc).__name__}: {exc}"
            )
    return bytes(data), None


def _read_uploads(uploads: list[Any]) -> tuple[list[FileEntry], list[str]]:
    files: list[FileEntry] = []
    errors: list[str] = []
    total_size = 0
    if len(uploads) > MAX_FILES:
        errors.append(
            f"Trop de fichiers ({len(uploads)} > MAX_FILES={MAX_FILES}); "
            f"seuls les {MAX_FILES} premiers seront traités."
        )
        uploads = uploads[:MAX_FILES]
    for f in uploads:
        data, err = _read_one_upload(f)
        if data is None:
            if err:
                errors.append(err)
            continue
        name = getattr(f, "name", "?")
        size = len(data)
        if size == 0:
            errors.append(f"`{name}`: fichier vide ignoré")
            continue
        if size > MAX_FILE_SIZE_BYTES:
            errors.append(
                f"`{name}`: fichier trop volumineux "
                f"({size} > {MAX_FILE_SIZE_BYTES} octets), ignoré"
            )
            continue
        if total_size + size > MAX_TOTAL_SIZE_BYTES:
            errors.append(
                f"`{name}`: dépasserait la limite globale "
                f"({MAX_TOTAL_SIZE_BYTES} octets), ignoré"
            )
            continue
        total_size += size
        files.append(_make_file_entry(name, data))
    return files, errors


def _render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### ⚙️ Pipeline")
        st.caption("Adapters actifs:")
        st.markdown(
            "- `gps` · MTF consensus\n"
            "- `rsi` · flat & nested (7-level v9.0 scale)\n"
            "- `sr` · zones + price context\n"
            "- `choch` · structure events\n"
            "- `heuristic` · fuzzy fallback"
        )
        st.markdown("### 🧮 Pré-calculs v3.4")
        st.markdown(
            "- ATR cascade (`h4 → h1×1.8 → d1×0.25 → synth`)\n"
            "- `nearest_aligned_zone` (réelles prioritaires)\n"
            "- `hot_zone_primary` (avec pivots UNKNOWN)\n"
            "- `sig_fresh_aligned`, `bb_mult`, `sl_distance_*`\n"
            "- `conviction_cap` selon source ATR"
        )
        fuzz_state = "✅ natif" if _HAS_RAPIDFUZZ else "⚠️ fallback Python"
        st.caption(f"RapidFuzz: {fuzz_state}")
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
        st.error(
            "Fichiers JSON invalides :\n"
            + "\n".join(f"- {e}" for e in parse_errors)
        )
    output = result.get("output")
    diagnostics = result.get("diagnostics") or []
    if output is None:
        st.error("Pipeline en erreur — aucun résultat exploitable.")
        _render_diagnostics(diagnostics)
        return
    meta = output.get("meta") or {}
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
    )
    if not uploads:
        st.info("⬆️ Déposez 1 à N fichiers JSON pour démarrer.")
        return

    entries, read_errors = _read_uploads(uploads)
    for err in read_errors:
        st.warning(err)
    if not entries:
        st.error("Aucun fichier lisible.")
        return

    run_btn = st.button(
        "🚀 Exécuter le pipeline",
        type="primary",
        use_container_width=True,
    )
    if not run_btn:
        st.caption(f"{len(entries)} fichier(s) prêt(s).")
        return

    fingerprint = _files_fingerprint(entries)
    entries_tuple = tuple(entries)
    with st.spinner("Pipeline en cours…"):
        result, diag = _safe_call(
            "ui.run", "ui_pipeline_crash",
            lambda fp=fingerprint, e=entries_tuple: run_pipeline_cached(fp, e),
            None,
            severity=Severity.CRITICAL,
        )
    if result is None:
        msg = diag.message if diag else "unknown"
        st.error(f"Erreur fatale du pipeline: {msg}")
        return
    _render_results(result)


if __name__ == "__main__":
    main()
