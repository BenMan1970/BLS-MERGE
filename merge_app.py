#!/usr/bin/env python3
"""
BLUESTAR MERGE v3.0 — Single-file production-grade trading pipeline.
Combines multi-scanner JSON merge with optional OANDA live data enrichment.
Streamlit-ready, secrets-driven, cached, defensive.

Secrets to configure in Streamlit Cloud (.streamlit/secrets.toml):
    OANDA_API_KEY = "your_oanda_api_token"
    OANDA_ACCOUNT_ID = "your_account_id"
    OANDA_ENV = "practice"   # or "live"

Usage:
    streamlit run bluestar_merge_v3_oanda.py
"""
from __future__ import annotations

import copy
import json
import logging
import math
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from enum import Enum
from typing import Any, Generic, Iterator, Literal, TypeVar
from collections import defaultdict

import requests
import streamlit as st
from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# 0. LOGGING — structuré minimal, jamais de secrets dans les logs
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stderr,
    level=logging.INFO,
)
logger = logging.getLogger("bluestar_merge")


def _sanitize(msg: str, secrets: list[str]) -> str:
    for s in secrets:
        if s and len(s) > 4:
            msg = msg.replace(s, "***")
    return msg


# ---------------------------------------------------------------------------
# 1. OANDA CLIENT — défensif, retry exponentiel, pas de secrets exposés
# ---------------------------------------------------------------------------
class OandaClient:
    """Thread-safe OANDA REST client with exponential back-off."""

    _ENV_URL = {
        "practice": "https://api-fxpractice.oanda.com",
        "live": "https://api-fxtrade.oanda.com",
    }

    def __init__(self, api_key: str, account_id: str, env: str = "practice"):
        if not api_key or not account_id:
            raise ValueError("OANDA_API_KEY and OANDA_ACCOUNT_ID are required")
        self._api_key = api_key
        self._account_id = account_id
        self._base_url = self._ENV_URL.get(env, self._ENV_URL["practice"])
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })
        self._lock = threading.Lock()
        self._secrets = [api_key, account_id]

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        url = f"{self._base_url}{endpoint}"
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with self._lock:
                    resp = self._session.request(method, url, timeout=15, **kwargs)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (429, 500, 502, 503, 504):
                    backoff = 2 ** attempt + (hash(endpoint) % 1000) / 1000.0
                    logger.warning("OANDA %s on %s, retry in %.2fs (attempt %d/%d)",
                                   resp.status_code, endpoint, backoff, attempt + 1, max_retries)
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
            except requests.RequestException as exc:
                clean_err = _sanitize(str(exc), self._secrets)
                if attempt == max_retries - 1:
                    raise RuntimeError(f"OANDA request failed after {max_retries} attempts: {clean_err}")
                time.sleep(2 ** attempt)
        raise RuntimeError(f"OANDA unreachable: {endpoint}")

    def get_candles(self, instrument: str, granularity: str = "M15", count: int = 100) -> list[dict]:
        """Fetch mid candles. Returns validated bars only."""
        endpoint = f"/v3/instruments/{instrument}/candles"
        params = {"granularity": granularity, "count": count, "price": "M"}
        data = self._request("GET", endpoint, params=params)
        bars = data.get("candles", [])
        return self._validate_bars(bars, instrument)

    def get_prices(self, instruments: list[str]) -> dict[str, dict[str, float]]:
        """Fetch current bid/ask/mid for a list of instruments."""
        if not instruments:
            return {}
        endpoint = f"/v3/accounts/{self._account_id}/pricing"
        params = {"instruments": ",".join(instruments)}
        data = self._request("GET", endpoint, params=params)
        out: dict[str, dict[str, float]] = {}
        for p in data.get("prices", []):
            inst = p.get("instrument", "")
            if not inst:
                continue
            bid = self._safe_float(p.get("bids", [{}])[0].get("price"))
            ask = self._safe_float(p.get("asks", [{}])[0].get("price"))
            if bid and ask:
                out[inst] = {"bid": bid, "ask": ask, "mid": round((bid + ask) / 2, 5)}
        return out

    @staticmethod
    def _safe_float(v: Any) -> float | None:
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _validate_bars(bars: list[dict], instrument: str) -> list[dict]:
        """Défensive validation: low>0, high>=low, OHLC cohérent, bornes absolues."""
        valid: list[dict] = []
        # Bornes absolues indicatives par classe d'actif
        max_allowed = 1_000_000.0
        if "XAU" in instrument or "GOLD" in instrument:
            max_allowed = 10_000.0
        if "XAG" in instrument or "SILVER" in instrument:
            max_allowed = 500.0
        if "BTC" in instrument:
            max_allowed = 200_000.0
        if "ETH" in instrument:
            max_allowed = 20_000.0

        for b in bars:
            if not b.get("complete", True):
                continue
            mid = b.get("mid")
            if not isinstance(mid, dict):
                continue
            o = OandaClient._safe_float(mid.get("o"))
            h = OandaClient._safe_float(mid.get("h"))
            l = OandaClient._safe_float(mid.get("l"))
            c = OandaClient._safe_float(mid.get("c"))
            if None in (o, h, l, c):
                continue
            if not (l > 0 and h >= l and o > 0 and c > 0):
                continue
            if not (l <= o <= h and l <= c <= h):  # cohérence OHLC
                continue
            if h > max_allowed or l > max_allowed:
                continue
            valid.append(b)
        return valid

    def instrument_to_oanda(self, canonical: str) -> str:
        """EUR/USD → EUR_USD."""
        return canonical.replace("/", "_")


# ---------------------------------------------------------------------------
# 2. DIAGNOSTICS — Result carrier, jamais d'exception qui traverse le pipeline
# ---------------------------------------------------------------------------
T = TypeVar("T")


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
    context: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
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

    def extend(self, other: Result[Any]) -> None:
        self.diagnostics.extend(other.diagnostics)

    def add(self, d: Diagnostic) -> None:
        self.diagnostics.append(d)

    def __iter__(self) -> Iterator[Diagnostic]:
        return iter(self.diagnostics)


# ---------------------------------------------------------------------------
# 3. SYMBOLS — heuristique pure, aucun actif hardcodé sauf ISO fiat
# ---------------------------------------------------------------------------
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
    "MXN", "SGD", "HKD", "CNH", "CNY", "INR", "BRL", "RUB",
})
_METAL_HINT = re.compile(r"^(XAU|XAG|XPT|XPD|GOLD|SILVER|PLAT)", re.I)
_INDEX_HINT = re.compile(r"(\d{2,4}|SPX|NDX|DAX|FTSE|NIKKEI|HSI|ASX)", re.I)
_CRYPTO_HINT = re.compile(r"(BTC|ETH|XRP|LTC|BCH|ADA|SOL|DOT|DOGE|USDT|USDC)", re.I)


@dataclass(frozen=True, slots=True)
class CanonicalSymbol:
    raw: str
    canonical: str
    base: str
    quote: str | None
    asset_class: AssetClass


class SymbolNormalizer:
    _SEP = re.compile(r"[\s/_\-.:]")

    def normalize(self, raw: object) -> CanonicalSymbol:
        if raw is None:
            return CanonicalSymbol("", "", "", None, AssetClass.UNKNOWN)
        s = str(raw).strip().upper()
        if not s:
            return CanonicalSymbol("", "", "", None, AssetClass.UNKNOWN)
        parts = [p for p in self._SEP.split(s) if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            return CanonicalSymbol(
                raw=s, canonical=f"{base}/{quote}", base=base, quote=quote,
                asset_class=self._classify(base, quote),
            )
        token = parts[0] if parts else s
        for q in _FIAT_ISO:
            if token.endswith(q) and len(token) > len(q):
                base = token[: -len(q)]
                return CanonicalSymbol(
                    raw=s, canonical=f"{base}/{q}", base=base, quote=q,
                    asset_class=self._classify(base, q),
                )
        for q in ("USDT", "USDC", "BUSD"):
            if token.endswith(q) and len(token) > len(q):
                base = token[: -len(q)]
                return CanonicalSymbol(
                    raw=s, canonical=f"{base}/{q}", base=base, quote=q,
                    asset_class=AssetClass.CRYPTO,
                )
        return CanonicalSymbol(
            raw=s, canonical=token, base=token, quote=None,
            asset_class=self._classify(token, None),
        )

    @staticmethod
    def _classify(base: str, quote: str | None) -> AssetClass:
        if _METAL_HINT.search(base):
            return AssetClass.METAL
        if _CRYPTO_HINT.search(base) or (quote and quote in {"USDT", "USDC", "BUSD"}):
            return AssetClass.CRYPTO
        if _INDEX_HINT.search(base):
            return AssetClass.INDEX
        if base in _FIAT_ISO and (quote is None or quote in _FIAT_ISO):
            return AssetClass.FOREX
        if quote and quote in _FIAT_ISO and len(base) == 3 and base.isalpha():
            return AssetClass.FOREX
        return AssetClass.UNKNOWN


# ---------------------------------------------------------------------------
# 4. TIMEFRAMES — parsing générique
# ---------------------------------------------------------------------------
class Timeframe(str, Enum):
    M1 = "M1"; M5 = "M5"; M15 = "M15"; M30 = "M30"
    H1 = "H1"; H4 = "H4"; D1 = "D1"; W1 = "W1"; MN = "MN"
    UNKNOWN = "UNKNOWN"


_ALIAS: dict[str, Timeframe] = {
    "1m": Timeframe.M1, "m1": Timeframe.M1,
    "5m": Timeframe.M5, "m5": Timeframe.M5,
    "15m": Timeframe.M15, "m15": Timeframe.M15,
    "30m": Timeframe.M30, "m30": Timeframe.M30,
    "1h": Timeframe.H1, "h1": Timeframe.H1, "60m": Timeframe.H1,
    "4h": Timeframe.H4, "h4": Timeframe.H4, "240m": Timeframe.H4,
    "d": Timeframe.D1, "d1": Timeframe.D1, "daily": Timeframe.D1, "day": Timeframe.D1,
    "w": Timeframe.W1, "w1": Timeframe.W1, "weekly": Timeframe.W1, "week": Timeframe.W1,
    "m": Timeframe.MN, "mn": Timeframe.MN, "monthly": Timeframe.MN, "month": Timeframe.MN,
}


def parse_timeframe(raw: object) -> Timeframe:
    if raw is None:
        return Timeframe.UNKNOWN
    s = str(raw).strip().lower()
    if not s:
        return Timeframe.UNKNOWN
    if s in _ALIAS:
        return _ALIAS[s]
    m = re.search(r"(1m|5m|15m|30m|1h|4h|h1|h4|d1|w1|mn|daily|weekly|monthly)", s)
    if m:
        return _ALIAS.get(m.group(1), Timeframe.UNKNOWN)
    return Timeframe.UNKNOWN


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


# ---------------------------------------------------------------------------
# 5. MODELS CANONIQUES — pivot unique, extra="forbid", validate_assignment
# ---------------------------------------------------------------------------
_BASE_CONFIG = ConfigDict(extra="forbid", frozen=False, validate_assignment=True, arbitrary_types_allowed=True)


class Direction(str, Enum):
    BULLISH = "Bullish"; BEARISH = "Bearish"; NEUTRAL = "Neutral"


class DivergenceKind(str, Enum):
    NONE = "None"; BULL = "Bullish"; BEAR = "Bearish"


class RSIReading(BaseModel):
    model_config = _BASE_CONFIG
    timeframe: Timeframe
    value: float | None = Field(default=None, ge=0.0, le=100.0)
    divergence: DivergenceKind = DivergenceKind.NONE


class TrendBias(BaseModel):
    model_config = _BASE_CONFIG
    timeframe: Timeframe
    bias: str
    direction: Direction = Direction.NEUTRAL


class SRZone(BaseModel):
    model_config = _BASE_CONFIG
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
    model_config = _BASE_CONFIG
    raw: str = ""
    support_level: float | None = None
    support_dist_pct: float | None = None
    support_tag: str | None = None
    resistance_level: float | None = None
    resistance_dist_pct: float | None = None
    resistance_tag: str | None = None
    is_intermediate: bool = False
    oanda_mid: float | None = None  # enrichissement OANDA


class StructureEvent(BaseModel):
    model_config = _BASE_CONFIG
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
    candles_elapsed: int | None = None


class MTFConsensus(BaseModel):
    model_config = _BASE_CONFIG
    pct: int = Field(default=0, ge=0, le=100)
    direction: Direction = Direction.NEUTRAL
    quality: str | None = None
    nc: int = 0
    age_d1: int = 0
    atr_h1: float | None = None
    atr_h4: float | None = None
    atr_daily: float | None = None
    biases: dict[Timeframe, str] = Field(default_factory=dict)

    @field_validator("biases", mode="before")
    @classmethod
    def _normalize_biases(cls, v: object) -> object:
        if isinstance(v, dict):
            return {
                Timeframe(k) if not isinstance(k, Timeframe) else k: val
                for k, val in v.items()
            }
        return v


class CanonicalAsset(BaseModel):
    model_config = _BASE_CONFIG
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
            symbol=sym.canonical, base=sym.base, quote=sym.quote,
            asset_class=sym.asset_class,
        )


class MergeMeta(BaseModel):
    model_config = _BASE_CONFIG
    generated_at: datetime
    version: str = "3.0.0-oanda"
    scanners_detected: list[str] = Field(default_factory=list)
    scanners_unknown: int = 0
    assets_count: int = 0
    signals_count: int = 0
    oanda_enriched: bool = False


class EnrichmentQuality(BaseModel):
    model_config = _BASE_CONFIG
    status: Literal["complete", "partial", "minimal", "empty"] = "empty"
    scanners_matched: int = 0
    scanners_total: int = 0


class EnrichedSignal(BaseModel):
    model_config = _BASE_CONFIG
    event: StructureEvent
    asset: CanonicalAsset
    htf_aligned: bool = False
    nearest_aligned_zone: SRZone | None = None
    tp_zones: list[SRZone] = Field(default_factory=list)
    confluence_total: float = 0.0
    enrichment: EnrichmentQuality = Field(default_factory=EnrichmentQuality)
    warnings: list[str] = Field(default_factory=list)


class MergeOutput(BaseModel):
    model_config = _BASE_CONFIG
    meta: MergeMeta
    assets: dict[str, CanonicalAsset]
    signals: list[EnrichedSignal]
    correlation_groups: dict[str, list[dict[str, object]]] = Field(default_factory=dict)
    hot_zones: list[dict[str, object]] = Field(default_factory=list)
    top_consensus: dict[str, list[dict[str, object]]] = Field(default_factory=dict)
    diagnostics: list[dict[str, object]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 6. ADAPTERS — Protocol + Registry + Implémentations
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AdapterMatch:
    score: float
    reason: str


class ScannerAdapter:
    name: str = "unknown"

    def detect(self, payload: object) -> AdapterMatch:
        raise NotImplementedError

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        raise NotImplementedError


class GPSAdapter(ScannerAdapter):
    name = "gps"
    _MTF_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
    _MTF_DIR_RE = re.compile(r"(bullish|bearish|neutral|range)", re.I)
    _TF_KEYS = {
        "M": Timeframe.MN, "Monthly": Timeframe.MN,
        "W": Timeframe.W1, "Weekly": Timeframe.W1,
        "D": Timeframe.D1, "Daily": Timeframe.D1,
        "4H": Timeframe.H4, "H4": Timeframe.H4,
        "1H": Timeframe.H1, "H1": Timeframe.H1,
        "15m": Timeframe.M15, "M15": Timeframe.M15,
    }

    def __init__(self, symbols: SymbolNormalizer):
        self._symbols = symbols

    def detect(self, payload: object) -> AdapterMatch:
        if not isinstance(payload, list) or not payload:
            return AdapterMatch(0.0, "not a non-empty list")
        sample = payload[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "items are not objects")
        keys = set(sample.keys())
        signature = {"Paire", "MTF", "Quality"}
        hits = len(signature & keys)
        if hits >= 2:
            return AdapterMatch(0.6 + 0.15 * hits, f"matched {hits}/3 signature keys")
        return AdapterMatch(0.0, "no GPS signature")

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, list):
            res.add(Diagnostic("gps", Severity.ERROR, "bad_root", "expected list"))
            return res
        for i, raw in enumerate(payload):
            if not isinstance(raw, dict):
                res.add(Diagnostic("gps", Severity.WARNING, "skip_item", "non-dict item skipped", {"index": i}))
                continue
            sym_raw = raw.get("Paire") or raw.get("pair") or raw.get("symbol")
            if not sym_raw:
                res.add(Diagnostic("gps", Severity.WARNING, "missing_symbol", "no pair/symbol key", {"index": i}))
                continue
            sym = self._symbols.normalize(sym_raw)
            asset = CanonicalAsset.from_symbol(sym)
            pct, direction = self._parse_mtf(raw.get("MTF", ""))
            biases: dict[Timeframe, str] = {}
            for k, tf in self._TF_KEYS.items():
                if k in raw and raw[k] is not None:
                    biases[tf] = str(raw[k])
            asset.mtf = MTFConsensus(
                pct=pct, direction=direction, quality=raw.get("Quality"),
                nc=int(safe_float(raw.get("NC")) or 0),
                age_d1=int(safe_float(raw.get("Age D1")) or 0),
                atr_h1=safe_float(raw.get("ATR H1")),
                atr_h4=safe_float(raw.get("ATR H4")),
                atr_daily=safe_float(raw.get("ATR Daily")),
                biases=biases,
            )
            asset.provenance.setdefault("gps", []).append("mtf")
            out.append(asset)
        return res

    @staticmethod
    def _parse_mtf(raw: object) -> tuple[int, Direction]:
        if raw is None:
            return 0, Direction.NEUTRAL
        s = str(raw)
        pct = 0
        m = GPSAdapter._MTF_PCT_RE.search(s)
        if m:
            try:
                pct = max(0, min(100, int(float(m.group(1)))))
            except ValueError:
                pct = 0
        d = GPSAdapter._MTF_DIR_RE.search(s)
        direction = Direction.NEUTRAL
        if d:
            token = d.group(1).lower()
            if token == "bullish":
                direction = Direction.BULLISH
            elif token == "bearish":
                direction = Direction.BEARISH
        return pct, direction


class RSIAdapter(ScannerAdapter):
    name = "rsi"
    _DIV_MAP = {
        "none": DivergenceKind.NONE, "aucune": DivergenceKind.NONE,
        "bull": DivergenceKind.BULL, "haussière": DivergenceKind.BULL,
        "haussiere": DivergenceKind.BULL, "bullish": DivergenceKind.BULL,
        "bear": DivergenceKind.BEAR, "baissière": DivergenceKind.BEAR,
        "baissiere": DivergenceKind.BEAR, "bearish": DivergenceKind.BEAR,
    }

    def __init__(self, symbols: SymbolNormalizer):
        self._symbols = symbols

    def detect(self, payload: object) -> AdapterMatch:
        items = self._items(payload)
        if not items:
            return AdapterMatch(0.0, "no items")
        sample = items[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict items")
        keys_lc = {k.lower() for k in sample.keys()}
        has_pair = bool({"pair", "devises", "symbol", "instrument"} & keys_lc)
        if not has_pair:
            return AdapterMatch(0.0, "no symbol key")
        if "timeframes" in keys_lc and isinstance(sample.get("timeframes"), dict):
            return AdapterMatch(0.9, "nested timeframes layout")
        if any(k.startswith("rsi_") for k in keys_lc) or "rsi" in keys_lc:
            return AdapterMatch(0.8, "flat RSI fields")
        return AdapterMatch(0.0, "no RSI fields")

    @staticmethod
    def _items(payload: object) -> list[object]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for k in ("instruments", "data", "items", "rsi"):
                v = payload.get(k)
                if isinstance(v, list):
                    return v
        return []

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        items = self._items(payload)
        if not items:
            res.add(Diagnostic("rsi", Severity.ERROR, "empty", "no instruments found"))
            return res
        for i, raw in enumerate(items):
            if not isinstance(raw, dict):
                res.add(Diagnostic("rsi", Severity.WARNING, "skip_item", "non-dict skipped", {"index": i}))
                continue
            sym_raw = raw.get("pair") or raw.get("Devises") or raw.get("symbol") or raw.get("instrument")
            if not sym_raw:
                res.add(Diagnostic("rsi", Severity.WARNING, "missing_symbol", "no symbol key", {"index": i}))
                continue
            sym = self._symbols.normalize(sym_raw)
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
                        value=self._clip_rsi(safe_float(v.get("rsi"))),
                        divergence=self._norm_div(v.get("div")),
                    ))
            else:
                tf_seen: set[Timeframe] = set()
                for k, v in raw.items():
                    if not isinstance(k, str):
                        continue
                    kl = k.lower()
                    if kl.startswith("rsi_") or kl.startswith("rsi"):
                        tf = parse_timeframe(kl.replace("rsi", ""))
                        if tf is Timeframe.UNKNOWN or tf in tf_seen:
                            continue
                        tf_seen.add(tf)
                        div_key = next(
                            (kk for kk in raw if isinstance(kk, str)
                             and kk.lower().startswith("div") and parse_timeframe(kk) is tf),
                            None,
                        )
                        readings.append(RSIReading(
                            timeframe=tf,
                            value=self._clip_rsi(safe_float(v)),
                            divergence=self._norm_div(raw.get(div_key) if div_key else None),
                        ))
            asset.rsi = readings
            asset.provenance.setdefault("rsi", []).append(f"{len(readings)}tf")
            out.append(asset)
        return res

    @staticmethod
    def _norm_div(v: object) -> DivergenceKind:
        if v is None:
            return DivergenceKind.NONE
        return RSIAdapter._DIV_MAP.get(str(v).strip().lower(), DivergenceKind.NONE)

    @staticmethod
    def _clip_rsi(v: float | None) -> float | None:
        if v is None or v < 0 or v > 100:
            return None
        return v


class SRAdapter(ScannerAdapter):
    name = "sr"
    _SUP_RE = re.compile(r"(SUR\s+support|S\s+proche|support)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)%\)", re.I)
    _RES_RE = re.compile(r"(SUR\s+resistance|R\s+proche|resistance)[:\s]+([\d.]+)\s*\(([-+]?[\d.]+)%\)", re.I)
    _INTER_RE = re.compile(r"(intermediaire|intermediate|unavailable|indisponible)", re.I)
    _STATUS_COEFF = {"vierge": 1.0, "testee": 0.8, "tested": 0.8, "role reverse": 0.6}

    def __init__(self, symbols: SymbolNormalizer):
        self._symbols = symbols

    def detect(self, payload: object) -> AdapterMatch:
        if not isinstance(payload, dict):
            return AdapterMatch(0.0, "not a dict root")
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
            score += 0.4
        if "price_context" in keys or "trends" in keys:
            score += 0.2
        return AdapterMatch(score, f"keys={sorted(keys)[:5]}")

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, dict):
            res.add(Diagnostic("sr", Severity.ERROR, "bad_root", "expected dict"))
            return res
        assets = payload.get("assets", [])
        if not isinstance(assets, list):
            res.add(Diagnostic("sr", Severity.ERROR, "bad_assets", "assets not a list"))
            return res
        for i, raw in enumerate(assets):
            if not isinstance(raw, dict):
                res.add(Diagnostic("sr", Severity.WARNING, "skip_item", "non-dict skipped", {"index": i}))
                continue
            sym_raw = raw.get("symbol") or raw.get("pair") or raw.get("Paire")
            if not sym_raw:
                res.add(Diagnostic("sr", Severity.WARNING, "missing_symbol", "no symbol key", {"index": i}))
                continue
            sym = self._symbols.normalize(sym_raw)
            asset = CanonicalAsset.from_symbol(sym)
            asset.price_context = self._parse_ctx(raw.get("price_context", ""))
            zones_raw = raw.get("zones", [])
            if isinstance(zones_raw, list):
                zones = [self._parse_zone(z) for z in zones_raw if isinstance(z, dict)]
                asset.zones = sorted([z for z in zones if z is not None], key=lambda z: z.distance_pct)
            asset.provenance.setdefault("sr", []).append(f"{len(asset.zones)}zones")
            out.append(asset)
        return res

    @staticmethod
    def _parse_ctx(raw: object) -> PriceContext:
        ctx = PriceContext(raw=str(raw or ""))
        s = ctx.raw
        if not s or SRAdapter._INTER_RE.search(s):
            ctx.is_intermediate = True
            return ctx
        m = SRAdapter._SUP_RE.search(s)
        if m:
            ctx.support_tag = m.group(1).strip()
            ctx.support_level = safe_float(m.group(2))
            ctx.support_dist_pct = safe_float(m.group(3))
        m = SRAdapter._RES_RE.search(s)
        if m:
            ctx.resistance_tag = m.group(1).strip()
            ctx.resistance_level = safe_float(m.group(2))
            ctx.resistance_dist_pct = safe_float(m.group(3))
        return ctx

    @staticmethod
    def _parse_zone(z: dict[str, object]) -> SRZone | None:
        level = safe_float(z.get("level"))
        if level is None:
            return None
        score = safe_float(z.get("score")) or 0.0
        dist = safe_float(z.get("distance_pct"))
        dist = 999.0 if dist is None else dist
        status = str(z.get("status", "Unknown"))
        coeff = SRAdapter._STATUS_COEFF.get(status.lower(), 0.8)
        tf_raw = str(z.get("timeframes", ""))
        tf_list: list[Timeframe] = []
        for tok in re.split(r"[,+/]", tf_raw):
            tf = parse_timeframe(tok.strip())
            if tf is not Timeframe.UNKNOWN:
                tf_list.append(tf)
        sig_raw = str(z.get("signal", "")).upper()
        side = "BUY" if "BUY" in sig_raw else ("SELL" if "SELL" in sig_raw else "UNKNOWN")
        alert_raw = str(z.get("alert", "") or "").upper()
        if "CHAUDE" in alert_raw or "HOT" in alert_raw:
            alert = "ZONE CHAUDE"
        elif "PROCHE" in alert_raw or "NEAR" in alert_raw:
            alert = "Proche"
        else:
            alert = ""
        return SRZone(
            side=side, level=round(level, 5), score=round(score, 2),
            weighted_score=round(score * coeff, 2), status=status,
            distance_pct=round(dist, 3), alert=alert, timeframes=tf_list,
            has_weekly=Timeframe.W1 in tf_list,
            has_daily=Timeframe.D1 in tf_list,
            has_h4=Timeframe.H4 in tf_list,
        )


class CHoCHAdapter(ScannerAdapter):
    name = "choch"

    def __init__(self, symbols: SymbolNormalizer):
        self._symbols = symbols

    def detect(self, payload: object) -> AdapterMatch:
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
        if "type" in keys or "is_choch" in keys:
            score += 0.3
        if "direction" in keys:
            score += 0.2
        if "confluence_score" in keys:
            score += 0.2
        return AdapterMatch(min(score, 1.0), f"signals[0]_keys={sorted(keys)[:5]}")

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        if not isinstance(payload, dict):
            res.add(Diagnostic("choch", Severity.ERROR, "bad_root", "expected dict"))
            return res
        sigs = payload.get("signals", [])
        if not isinstance(sigs, list):
            res.add(Diagnostic("choch", Severity.ERROR, "bad_signals", "signals not list"))
            return res
        by_sym: dict[str, CanonicalAsset] = {}
        for i, raw in enumerate(sigs):
            if not isinstance(raw, dict):
                res.add(Diagnostic("choch", Severity.WARNING, "skip_item", "non-dict skipped", {"index": i}))
                continue
            sym_raw = raw.get("pair") or raw.get("symbol") or raw.get("pair_oanda")
            if not sym_raw:
                res.add(Diagnostic("choch", Severity.WARNING, "missing_symbol", "no symbol key", {"index": i}))
                continue
            sym = self._symbols.normalize(sym_raw)
            asset = by_sym.setdefault(sym.canonical, CanonicalAsset.from_symbol(sym))
            direction_raw = str(raw.get("direction", "")).lower()
            direction = (Direction.BULLISH if "bull" in direction_raw
                         else Direction.BEARISH if "bear" in direction_raw
                         else Direction.NEUTRAL)
            ts_raw = raw.get("signal_time")
            ts: datetime | None = None
            if ts_raw:
                try:
                    ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                except ValueError:
                    res.add(Diagnostic("choch", Severity.WARNING, "bad_time", "could not parse signal_time", {"value": ts_raw}))
            event = StructureEvent(
                signal_id=str(raw.get("signal_id") or f"auto_{i}"),
                kind=str(raw.get("type") or "CHoCH"),
                direction=direction,
                timeframe=parse_timeframe(raw.get("timeframe")),
                level=safe_float(raw.get("level")),
                close_price=safe_float(raw.get("close_price")),
                current_price=safe_float(raw.get("current_price")),
                confluence_score=safe_float(raw.get("confluence_score")),
                status=str(raw.get("status") or "Unknown"),
                signal_time=ts,
                distance_pct=safe_float(raw.get("distance_pct")),
                distance_atr_multiple=safe_float(raw.get("distance_atr_multiple")),
                volatility=raw.get("volatility"),
                force=raw.get("force"),
                bb_regime=raw.get("bb_regime"),
                session=raw.get("session"),
                candles_elapsed=int(safe_float(raw.get("candles_elapsed")) or 0),
            )
            asset.structure_events.append(event)
            asset.provenance.setdefault("choch", []).append(event.signal_id)
        out.extend(by_sym.values())
        return res


class HeuristicAdapter(ScannerAdapter):
    """Fallback générique par fuzzy-matching sur les clés (difflib, pas rapidfuzz)."""
    name = "heuristic"
    _SYMBOL_HINTS = ("pair", "symbol", "instrument", "ticker", "devises", "paire", "asset")
    _RSI_HINT = "rsi"

    def __init__(self, symbols: SymbolNormalizer):
        self._symbols = symbols

    @staticmethod
    def _best_key(keys: list[str], target: str, threshold: float = 0.7) -> str | None:
        best: tuple[str | None, float] = (None, 0.0)
        for k in keys:
            score = SequenceMatcher(None, target.lower(), k.lower()).ratio()
            if score > best[1]:
                best = (k, score)
        return best[0] if best[1] >= threshold else None

    def detect(self, payload: object) -> AdapterMatch:
        items = self._items(payload)
        if not items:
            return AdapterMatch(0.0, "no items")
        sample = items[0]
        if not isinstance(sample, dict):
            return AdapterMatch(0.0, "non-dict items")
        keys = list(sample.keys())
        if any(self._best_key(keys, h, 0.8) for h in self._SYMBOL_HINTS):
            return AdapterMatch(0.45, "fuzzy symbol key present")
        return AdapterMatch(0.0, "no recognizable symbol")

    @staticmethod
    def _items(payload: object) -> list[object]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for _, v in payload.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v
        return []

    def adapt(self, payload: object) -> Result[list[CanonicalAsset]]:
        out: list[CanonicalAsset] = []
        res: Result[list[CanonicalAsset]] = Result(value=out)
        items = self._items(payload)
        if not items:
            res.add(Diagnostic("heuristic", Severity.ERROR, "empty", "no items recognized"))
            return res
        for i, raw in enumerate(items):
            if not isinstance(raw, dict):
                continue
            keys = list(raw.keys())
            sym_key = None
            for hint in self._SYMBOL_HINTS:
                sym_key = self._best_key(keys, hint, 0.8)
                if sym_key:
                    break
            if sym_key is None:
                res.add(Diagnostic("heuristic", Severity.WARNING, "no_symbol", "skipped item without symbol", {"index": i}))
                continue
            sym = self._symbols.normalize(raw[sym_key])
            asset = CanonicalAsset.from_symbol(sym)
            readings: list[RSIReading] = []
            for k, v in raw.items():
                if not isinstance(k, str) or self._RSI_HINT not in k.lower():
                    continue
                val = safe_float(v) if not isinstance(v, dict) else safe_float(v.get("rsi"))
                if val is None or val < 0 or val > 100:
                    continue
                tf = parse_timeframe(k)
                if tf is Timeframe.UNKNOWN and isinstance(v, dict):
                    tf = parse_timeframe(v.get("tf") or v.get("timeframe"))
                if tf is Timeframe.UNKNOWN:
                    continue
                readings.append(RSIReading(timeframe=tf, value=val))
            asset.rsi = readings
            asset.provenance.setdefault("heuristic", []).append("introspected")
            out.append(asset)
        res.add(Diagnostic("heuristic", Severity.INFO, "introspected", f"extracted {len(out)} assets via fuzzy matching"))
        return res


@dataclass(slots=True)
class DetectionResult:
    adapter: ScannerAdapter | None
    score: float
    reason: str


class ScannerRegistry:
    def __init__(self, adapters: list[ScannerAdapter], fallback: ScannerAdapter | None = None):
        if not adapters:
            raise ValueError("ScannerRegistry requires at least one adapter")
        self._adapters = list(adapters)
        self._fallback = fallback

    def detect(self, payload: object) -> DetectionResult:
        best = DetectionResult(None, 0.0, "no match")
        for a in self._adapters:
            try:
                m = a.detect(payload)
            except Exception as exc:
                m = AdapterMatch(0.0, f"detect raised: {exc!r}")
            if m.score > best.score:
                best = DetectionResult(a, m.score, m.reason)
        if best.score < 0.5 and self._fallback is not None:
            return DetectionResult(self._fallback, 0.4, "fallback heuristic")
        return best

    def adapt(self, payload: object) -> tuple[str, Result[list[CanonicalAsset]]]:
        det = self.detect(payload)
        if det.adapter is None:
            r: Result[list[CanonicalAsset]] = Result(value=[])
            r.add(Diagnostic("registry", Severity.ERROR, "no_adapter", "no adapter could parse payload", {"reason": det.reason}))
            return "unknown", r
        result = det.adapter.adapt(payload)
        result.add(Diagnostic("registry", Severity.INFO, "adapter_selected", f"{det.adapter.name} selected (score={det.score:.2f})", {"reason": det.reason}))
        return det.adapter.name, result


# ---------------------------------------------------------------------------
# 7. PIPELINE — Merge, Enrich, Correlate, Orchestration
# ---------------------------------------------------------------------------
class MergeEngine:
    def merge(self, partial_groups: list[list[CanonicalAsset]]) -> Result[dict[str, CanonicalAsset]]:
        merged: dict[str, CanonicalAsset] = {}
        res: Result[dict[str, CanonicalAsset]] = Result(value=merged)
        collisions: dict[str, int] = defaultdict(int)
        for group in partial_groups:
            for asset in group:
                key = asset.symbol
                if not key:
                    res.add(Diagnostic("merge", Severity.WARNING, "empty_symbol", "asset with empty symbol dropped"))
                    continue
                if key not in merged:
                    merged[key] = asset.model_copy(deep=True)
                else:
                    self._fold_into(merged[key], asset, res)
                    collisions[key] += 1
        res.add(Diagnostic("merge", Severity.INFO, "summary", f"merged {len(merged)} unique assets", {"collisions": dict(collisions)}))
        return res

    @staticmethod
    def _fold_into(target: CanonicalAsset, source: CanonicalAsset, res: Result[Any]) -> None:
        if target.asset_class.value == "unknown" and source.asset_class.value != "unknown":
            target.asset_class = source.asset_class
        if not target.base:
            target.base = source.base
        if not target.quote:
            target.quote = source.quote
        existing_tfs = {r.timeframe for r in target.rsi}
        for r in source.rsi:
            if r.timeframe in existing_tfs:
                res.add(Diagnostic("merge", Severity.DEBUG, "rsi_conflict", f"RSI {r.timeframe.value} present from multiple sources", {"symbol": target.symbol}))
                continue
            target.rsi.append(r)
        existing_b = {b.timeframe for b in target.biases}
        for b in source.biases:
            if b.timeframe not in existing_b:
                target.biases.append(b)
        if target.mtf is None and source.mtf is not None:
            target.mtf = source.mtf
        elif target.mtf is not None and source.mtf is not None and source.mtf.pct > target.mtf.pct:
            target.mtf = source.mtf
        if target.price_context is None:
            target.price_context = source.price_context
        elif source.price_context and not source.price_context.is_intermediate and target.price_context.is_intermediate:
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
        for k, v in source.provenance.items():
            target.provenance.setdefault(k, []).extend(v)


_DIR_TOKENS = {
    Direction.BULLISH: ("bullish", "bull", "haussier", "hausse"),
    Direction.BEARISH: ("bearish", "bear", "baissier", "baisse"),
}


def _direction_from_bias(bias: str) -> Direction:
    s = (bias or "").lower()
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
                    event=event, asset=asset,
                    htf_aligned=self._htf_aligned(asset, event),
                    nearest_aligned_zone=aligned_zones[0] if aligned_zones else None,
                    tp_zones=opposite_zones[:3],
                    confluence_total=self._confluence(asset, event),
                    enrichment=eq,
                    warnings=self._invariant_warnings(asset, event),
                ))
        res.add(Diagnostic("enrich", Severity.INFO, "summary", f"enriched {len(signals)} signals across {len(assets)} assets"))
        return res

    @staticmethod
    def _htf_aligned(asset: CanonicalAsset, event: StructureEvent) -> bool:
        if asset.mtf is None:
            return False
        bias_h4 = asset.mtf.biases.get(Timeframe.H4)
        if not bias_h4:
            return False
        return _direction_from_bias(bias_h4) == event.direction

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
        sources = set(asset.provenance.keys())
        n = len(sources)
        if n >= 3:
            status = "complete"
        elif n == 2:
            status = "partial"
        elif n == 1:
            status = "minimal"
        else:
            status = "empty"
        return EnrichmentQuality(status=status, scanners_matched=n, scanners_total=n)

    @staticmethod
    def _invariant_warnings(asset: CanonicalAsset, event: StructureEvent) -> list[str]:
        warns: list[str] = []
        if event.direction is Direction.NEUTRAL:
            warns.append("event has neutral direction")
        if event.level is not None and event.level <= 0:
            warns.append(f"non-positive event level: {event.level}")
        if asset.mtf and not (0 <= asset.mtf.pct <= 100):
            warns.append(f"mtf_pct out of range: {asset.mtf.pct}")
        for r in asset.rsi:
            if r.value is not None and not (0 <= r.value <= 100):
                warns.append(f"rsi {r.timeframe.value} out of range: {r.value}")
        return warns


_QUALITY_RANK = {"A+": 4, "A": 3, "B+": 2, "B": 1}


class CorrelationEngine:
    def build(self, signals: list[EnrichedSignal]) -> dict[str, list[dict[str, object]]]:
        groups: dict[str, list[dict[str, object]]] = defaultdict(list)
        for s in signals:
            asset = s.asset
            legs = [l for l in (asset.base, asset.quote) if l]
            for leg in legs:
                groups[leg].append({
                    "symbol": asset.symbol,
                    "direction": s.event.direction.value,
                    "kind": s.event.kind,
                    "mtf_pct": asset.mtf.pct if asset.mtf else 0,
                    "quality": asset.mtf.quality if asset.mtf else None,
                    "confluence": s.confluence_total,
                })
        return {
            leg: sorted(items, key=lambda x: (
                _QUALITY_RANK.get(str(x.get("quality")), 0),
                float(x.get("confluence") or 0.0),
            ), reverse=True)
            for leg, items in sorted(groups.items()) if len(items) >= 2
        }


@dataclass(slots=True)
class IngestedFile:
    name: str
    payload: object


def build_default_pipeline() -> MergePipeline:
    symbols = SymbolNormalizer()
    adapters: list[ScannerAdapter] = [
        GPSAdapter(symbols), RSIAdapter(symbols), SRAdapter(symbols), CHoCHAdapter(symbols)
    ]
    registry = ScannerRegistry(adapters, fallback=HeuristicAdapter(symbols))
    return MergePipeline(registry=registry)


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
        diags: list[Diagnostic] = []
        partials: list[list[CanonicalAsset]] = []
        scanners_detected: list[str] = []
        unknown_count = 0
        for f in files:
            name, r = self._registry.adapt(f.payload)
            diags.extend(r.diagnostics)
            if name == "unknown" or r.value is None:
                unknown_count += 1
                continue
            scanners_detected.append(f"{f.name}:{name}")
            partials.append(r.value)
        merged_r = self._merger.merge(partials)
        diags.extend(merged_r.diagnostics)
        assets = merged_r.value or {}
        enriched_r = self._enricher.enrich(assets)
        diags.extend(enriched_r.diagnostics)
        signals: list[EnrichedSignal] = enriched_r.value or []
        groups = self._correlator.build(signals)
        hot_zones = self._hot_zones(assets)
        top = self._top_consensus(assets)
        out = MergeOutput(
            meta=MergeMeta(
                generated_at=datetime.now(timezone.utc),
                scanners_detected=scanners_detected,
                scanners_unknown=unknown_count,
                assets_count=len(assets),
                signals_count=len(signals),
            ),
            assets=assets,
            signals=signals,
            correlation_groups=groups,
            hot_zones=hot_zones,
            top_consensus=top,
            diagnostics=[d.to_dict() for d in diags],
        )
        result: Result[MergeOutput] = Result(value=out)
        result.diagnostics.extend(diags)
        if not files:
            result.add(Diagnostic("pipeline", Severity.ERROR, "no_input", "no files provided"))
        return result

    @staticmethod
    def _hot_zones(assets: dict[str, CanonicalAsset]) -> list[dict[str, object]]:
        zones: list[dict[str, object]] = []
        for sym, asset in assets.items():
            for z in asset.zones:
                if z.distance_pct < 2.0:
                    zones.append({"symbol": sym, **_zone_to_dict(z)})
        zones.sort(key=lambda z: float(z["distance_pct"]))
        return zones

    @staticmethod
    def _top_consensus(assets: dict[str, CanonicalAsset], min_pct: int = 85, top_n: int = 5) -> dict[str, list[dict[str, object]]]:
        bull, bear = [], []
        for sym, asset in assets.items():
            if asset.mtf is None or asset.mtf.pct < min_pct:
                continue
            entry = {
                "symbol": sym, "mtf_pct": asset.mtf.pct,
                "quality": asset.mtf.quality, "nc": asset.mtf.nc, "age_d1": asset.mtf.age_d1,
            }
            if asset.mtf.direction.value == "Bullish":
                bull.append(entry)
            elif asset.mtf.direction.value == "Bearish":
                bear.append(entry)

        def _rank(e: dict[str, object]) -> tuple[int, int, int]:
            q = {"A+": 3, "A": 2, "B+": 1, "B": 0}.get(str(e.get("quality")), 0)
            return q, int(e.get("nc") or 0), int(e.get("mtf_pct") or 0)

        bull.sort(key=_rank, reverse=True)
        bear.sort(key=_rank, reverse=True)
        return {"top_bullish": bull[:top_n], "top_bearish": bear[:top_n]}


def _zone_to_dict(z: SRZone) -> dict[str, object]:
    return {
        "side": z.side, "level": z.level, "score": z.score,
        "weighted_score": z.weighted_score, "status": z.status,
        "distance_pct": z.distance_pct, "alert": z.alert,
        "timeframes": [t.value for t in z.timeframes],
        "has_weekly": z.has_weekly, "has_daily": z.has_daily,
    }


# ---------------------------------------------------------------------------
# 8. OANDA CACHING — Streamlit cache_data avec copie défensive, TTL par timeframe
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, max_entries=100)
def _fetch_oanda_prices(api_key: str, account_id: str, env: str, instruments_csv: str) -> dict[str, dict[str, float]]:
    """
    Cached OANDA pricing fetch. Returns deep-copied dict to ensure defensiveness.
    TTL=300s (5 min) pour les prix temps réel.
    """
    client = OandaClient(api_key, account_id, env)
    instruments = [i for i in instruments_csv.split(",") if i]
    data = client.get_prices(instruments)
    # Copie défensive pour éviter mutation du cache en aval
    return copy.deepcopy(data)


@st.cache_data(ttl=900, max_entries=50)
def _fetch_oanda_candles(api_key: str, account_id: str, env: str, instrument: str, granularity: str, count: int) -> list[dict]:
    """
    Cached OANDA candles fetch. TTL=900s (15 min) par timeframe/instrument.
    Copie défensive des barres validées.
    """
    client = OandaClient(api_key, account_id, env)
    bars = client.get_candles(instrument, granularity, count)
    return copy.deepcopy(bars)


# ---------------------------------------------------------------------------
# 9. EXPORT — JSON stable
# ---------------------------------------------------------------------------
def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()
    if hasattr(o, "model_dump"):
        return o.model_dump(mode="json")
    if hasattr(o, "value"):
        return o.value
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def export_json(output: MergeOutput, *, indent: int = 2) -> str:
    payload = output.model_dump(mode="json")
    return json.dumps(payload, indent=indent, ensure_ascii=False, default=_json_default, sort_keys=False)


# ---------------------------------------------------------------------------
# 10. STREAMLIT UI — thin layer, zero business logic, caching explicite
# ---------------------------------------------------------------------------
def main() -> None:
    st.set_page_config(page_title="🔷 BLUESTAR MERGE v3.0", layout="wide", page_icon="🔷")
    _render_header()

    uploads = st.file_uploader(
        "Déposez tous vos scanners JSON (détection automatique)",
        type="json", accept_multiple_files=True,
        help="Aucun mapping manuel : chaque fichier est identifié par introspection.",
    )

    if not uploads:
        st.info("Aucun fichier chargé. Déposez 1 à N scanners JSON pour démarrer.")
        return

    if not st.button("🚀 Exécuter le pipeline", type="primary", use_container_width=True):
        return

    files: list[IngestedFile] = []
    parse_errors: list[str] = []
    for f in uploads:
        try:
            f.seek(0)
            payload = json.load(f)
            files.append(IngestedFile(name=f.name, payload=payload))
        except json.JSONDecodeError as e:
            parse_errors.append(f"{f.name}: {e}")

    if parse_errors:
        st.error("Fichiers JSON invalides:
" + "
".join(f"- {e}" for e in parse_errors))

    if not files:
        return

    pipeline = build_default_pipeline()
    result = pipeline.run(files)

    if result.value is None:
        st.error("Pipeline en erreur — voir diagnostics ci-dessous.")
        _render_diagnostics(result.diagnostics)
        return

    out = result.value

    # --- OANDA ENRICHMENT (optionnel, si secrets configurés) ---
    oanda_enriched = False
    try:
        api_key = st.secrets.get("OANDA_API_KEY", "")
        account_id = st.secrets.get("OANDA_ACCOUNT_ID", "")
        env = st.secrets.get("OANDA_ENV", "practice")
        if api_key and account_id:
            with st.spinner("Enrichissement OANDA en cours…"):
                oanda_enriched = _apply_oanda_enrichment(out, api_key, account_id, env)
    except Exception as exc:
        clean = _sanitize(str(exc), [str(st.secrets.get("OANDA_API_KEY", ""))])
        logger.exception("OANDA enrichment failed: %s", clean)
        st.warning(f"Enrichissement OANDA indisponible: {clean}")

    out.meta.oanda_enriched = oanda_enriched
    logger.info("pipeline_done assets=%d signals=%d oanda=%s",
                out.meta.assets_count, out.meta.signals_count, oanda_enriched)

    _render_summary(out)
    _render_diagnostics(result.diagnostics)
    _render_export(out)


def _apply_oanda_enrichment(out: MergeOutput, api_key: str, account_id: str, env: str) -> bool:
    """Pure function: enrichit les PriceContext et recalcule les distances avec le prix OANDA réel."""
    symbols = list(out.assets.keys())
    if not symbols:
        return False
    # Normaliser pour OANDA
    oanda_map = {sym: sym.replace("/", "_") for sym in symbols}
    # Batch par lots de 50 (limite OANDA)
    all_prices: dict[str, dict[str, float]] = {}
    batch_size = 50
    items = list(oanda_map.items())
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        csv = ",".join(oanda for _, oanda in batch)
        prices = _fetch_oanda_prices(api_key, account_id, env, csv)
        for sym, oanda in batch:
            if oanda in prices:
                all_prices[sym] = prices[oanda]

    if not all_prices:
        return False

    for sym, asset in out.assets.items():
        price_data = all_prices.get(sym)
        if not price_data:
            continue
        mid = price_data.get("mid")
        if mid is None:
            continue
        if asset.price_context is None:
            asset.price_context = PriceContext()
        asset.price_context.oanda_mid = mid

        # Recalcul des distances des zones par rapport au prix OANDA réel
        for z in asset.zones:
            if z.level and mid > 0:
                z.distance_pct = round(abs(z.level - mid) / mid * 100, 3)
        asset.zones.sort(key=lambda z: z.distance_pct)

        # Recalcul event distance si level présent
        for ev in asset.structure_events:
            if ev.level and mid and mid > 0:
                ev.distance_pct = round(abs(ev.level - mid) / mid * 100, 3)
    return True


def _render_header() -> None:
    st.markdown(
        """
        <div style="background:linear-gradient(135deg,#1B45B4 0%,#0f2d8a 100%);
             color:white;padding:18px 24px;border-radius:10px;margin-bottom:20px">
          <div style="font-family:monospace;font-size:10px;opacity:.65;letter-spacing:2px">
            BLUESTAR SYSTEM · Generic multi-scanner merge + OANDA enrichment
          </div>
          <div style="font-family:monospace;font-size:22px;font-weight:700">
            BLUESTAR MERGE <span style="opacity:.6;font-size:14px">v3.0</span>
          </div>
          <div style="font-family:monospace;font-size:11px;opacity:.8">
            Auto-detection · Canonical pivot · Format-agnostic · OANDA Live
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_summary(out: MergeOutput) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Scanners détectés", len(out.meta.scanners_detected))
    c2.metric("Scanners inconnus", out.meta.scanners_unknown)
    c3.metric("Actifs canoniques", out.meta.assets_count)
    c4.metric("Signaux enrichis", out.meta.signals_count)
    c5.metric("Zones chaudes ≤ 2%", len(out.hot_zones))

    if out.signals:
        st.subheader("📊 Signaux enrichis")
        for s in out.signals:
            badge = "🟢" if s.enrichment.status == "complete" else ("🟡" if s.enrichment.status == "partial" else "🔴")
            htf = "✅" if s.htf_aligned else "⚠️"
            nz = s.nearest_aligned_zone
            zone_txt = (f"@ {nz.level} (d={nz.distance_pct:.2f}%, sc={nz.score})" if nz else "no aligned zone")
            warns = f" ⚡{len(s.warnings)}w" if s.warnings else ""
            oanda_price = ""
            if s.asset.price_context and s.asset.price_context.oanda_mid:
                oanda_price = f" · OANDA mid={s.asset.price_context.oanda_mid}"
            st.markdown(
                f"- {badge} `{s.asset.symbol}` [{s.event.timeframe.value}] "
                f"**{s.event.direction.value}** · HTF {htf} · {zone_txt} · "
                f"confluence={s.confluence_total}{warns}{oanda_price}"
            )

    if out.correlation_groups:
        with st.expander(f"🔗 Clusters ({len(out.correlation_groups)} pivots)"):
            for leg, entries in out.correlation_groups.items():
                dirs = {e["direction"] for e in entries}
                flag = "✅" if len(dirs) == 1 else "⚠️"
                st.markdown(
                    f"**{leg}** {flag} · " + " · ".join(
                        f"`{e['symbol']}` {e['direction']}" for e in entries
                    )
                )

    if out.meta.oanda_enriched:
        st.success("✅ Données OANDA intégrées (prix temps réel + distances recalculées)")


def _render_diagnostics(diags: list[Diagnostic]) -> None:
    if not diags:
        return
    error_count = sum(1 for d in diags if d.severity.value in ("error", "critical"))
    warn_count = sum(1 for d in diags if d.severity.value == "warning")
    with st.expander(f"🔧 Diagnostics ({len(diags)} · {error_count} err · {warn_count} warn)"):
        for d in diags:
            icon = {"error": "🔴", "critical": "🔴", "warning": "🟡", "info": "🔵", "debug": "⚪"}.get(d.severity.value, "•")
            st.markdown(f"{icon} `[{d.stage}/{d.code}]` {d.message}")


def _render_export(out: MergeOutput) -> None:
    payload = export_json(out)
    fname = f"merged_{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json"
    st.download_button(
        "📥 Télécharger merged_pipeline.json",
        data=payload, file_name=fname, mime="application/json",
        use_container_width=True, type="primary",
    )
    with st.expander("Prévisualiser JSON (4000 premiers caractères)"):
        st.code(payload[:4000] + ("
…" if len(payload) > 4000 else ""), language="json")


if __name__ == "__main__":
    main()
