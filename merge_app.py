"""
BLUESTAR ENGINE v10 — Hybrid Absolute/Cross-Sectional (V4 architecture)

Moteur monofichier. Source de vérité = JSON fusionné (merge) + JSON calendaire.

Pipeline : ingestion des 2 JSON -> gates univers -> 7 facteurs V4 -> conviction
absolue + flags/caps -> niveaux SL/TP -> preflight -> ranking -> diversification
-> rendu HTML FX Desk calibré A4 (PDF natif optionnel via WeasyPrint).

Usage:
  python ENGINE.V10.py --merged merge.json --calendar-json calendar.json -o report.html
  python ENGINE.V10.py --merged merge.json --calendar-json calendar.json -o report.html --pdf report.pdf

API :
  run_pipeline(merged_path, calendar_json_path=..., output_path=..., pdf_path=...) -> str
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from enum import Enum
from types import MappingProxyType
from typing import Any, ClassVar, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

import jinja2
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Fuseau d'affichage unique du rendu Desk, DST-aware (CET/CEST). Repli défensif
# sur un offset fixe si la base tzdata est absente (Windows sans paquet tzdata).
try:
    REPORT_TZ: tzinfo = ZoneInfo("Europe/Paris")
except Exception:
    REPORT_TZ = timezone(timedelta(hours=1))

logger = logging.getLogger("bluestar.v10")

# Bump manuel à chaque changement de comportement de grading/scoring.
# app.py lit cet attribut via getattr(mod, "__version__", "inconnu").
__version__ = "10.5.3"  # Briefing calendaire orienté décision : releases
                        # dédoublonnées (release_group_id), consensus vs
                        # précédent, book exposé, marqueur de traversée
                        # d'événement. Constats d'intégrité rétrogradés en
                        # note ; seules les anomalies actionnables restent en
                        # bandeau. C3 dédoublonné. _surprise_factor neutralisé
                        # quand la source ne publie pas les résultats.
                        # f7_macro : le risque par devise est calculé avant
                        # tout garde-fou de couverture globale — un event S/A
                        # daté et identifié n'est plus écrasé par un 0.00
                        # « non mesuré » au seul motif que le flux ne couvre
                        # pas 168h. Fail-closed réservé aux deux angles morts
                        # réels (devise hors flux, silence sur fenêtre
                        # tronquée). MACRO_COVERAGE_GRANULAR déprécié (ex-
                        # comportement devenu permanent). CONVICTIONS
                        # DÉPLACÉES — cf. doctrine d'activation du flag.

# ════════════════════════════════════════════════════════════════════════════
# SECTION 0 — OPTIONAL PDF BACKEND (jamais bloquant à l'import)
# ════════════════════════════════════════════════════════════════════════════
try:
    from weasyprint import HTML as _WeasyHTML
    _HAS_WEASYPRINT = True
    _WEASYPRINT_ERROR = ""
except Exception as _wp_exc:
    _HAS_WEASYPRINT = False
    _WEASYPRINT_ERROR = f"{type(_wp_exc).__name__}: {_wp_exc}"


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ENUMS
# ════════════════════════════════════════════════════════════════════════════
class Direction(str, Enum):
    BULLISH = "Bullish"
    BEARISH = "Bearish"
    NEUTRAL = "Neutral"


class ImpactLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EventTier(str, Enum):
    S = "S"      # NFP, FOMC, CPI, Rate Decision
    A = "A"      # GDP, PMI, ADP, PCE
    B = "B"      # Speeches / press conf
    NONE = "NONE"


class GateCode(str, Enum):
    G0_SCHEMA_ASSET_ERROR = "SCHEMA_ASSET_ERROR"
    G1_CAL_BLACKOUT = "CAL_BLACKOUT"
    G2_LOW_QUALITY = "LOW_QUALITY"
    G3_NO_DIRECTION = "NO_DIRECTION"
    G4_LOW_CONSENSUS = "LOW_CONSENSUS"
    G5_NO_ATR = "NO_ATR"


class Conviction(str, Enum):
    AAA = "AAA"
    AA = "AA"
    A = "A"
    BBB = "BBB"
    BB = "BB"
    B = "B"


class CalStatus(str, Enum):
    OK = "OK"
    BLACKOUT = "BLACKOUT"
    PROXIMITY = "PROXIMITY"
    WATCH = "WATCH"


class MacroRegime(str, Enum):
    """Régime macro au niveau PORTEFEUILLE (pas par actif)."""
    EVENT_VACUUM = "EVENT_VACUUM"
    EVENT_DRIFT = "EVENT_DRIFT"
    PRE_POLICY_COMPRESSION = "PRE_POLICY_COMPRESSION"
    POST_POLICY_REPRICING = "POST_POLICY_REPRICING"
    UNKNOWN = "UNKNOWN"


class FreshnessAudit(str, Enum):
    """Réconciliation candles_elapsed vs signal_time."""
    FRESH = "FRESH"        # vérifié cohérent
    STALE = "STALE"        # vérifié INCOHÉRENT
    UNKNOWN = "UNKNOWN"    # non vérifiable -> comportement inchangé


# Ordinal rank for diversification preference (higher = stronger conviction).
_CONVICTION_ORDINAL: Mapping[str, int] = MappingProxyType({
    "AAA": 6, "AA": 5, "A": 4, "BBB": 3, "BB": 2, "B": 1,
})


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — HELPERS
# ════════════════════════════════════════════════════════════════════════════
def _dir_eq(a: Any, b: Any) -> bool:
    av = a.value if hasattr(a, "value") else str(a)
    bv = b.value if hasattr(b, "value") else str(b)
    return av.lower() == bv.lower()


def _norm_dir(v: Any) -> Direction:
    if isinstance(v, Direction):
        return v
    s = str(v).lower()
    if "bull" in s:
        return Direction.BULLISH
    if "bear" in s:
        return Direction.BEARISH
    return Direction.NEUTRAL


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def _clamp01(x: float) -> float:
    if math.isnan(x):
        return 0.0
    return max(0.0, min(1.0, x))


def _opposite_dir(d: Direction) -> Direction:
    if d is Direction.BULLISH:
        return Direction.BEARISH
    if d is Direction.BEARISH:
        return Direction.BULLISH
    return Direction.NEUTRAL


def _median(values: Iterable[float]) -> float:
    """Médiane pure-python déterministe. 0.0 sur séquence vide."""
    s = sorted(float(v) for v in values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _parse_iso_utc(raw: Any) -> Optional[datetime]:
    """ISO-8601 (suffixe Z toléré) -> datetime aware UTC. None si échec."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=timezone.utc) if raw.tzinfo is None else raw.astimezone(timezone.utc)
    try:
        s = str(raw).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ── Comptage de bougies FX-aware ────────────────────────────────────────────
# Semaine FX : dimanche 21:00 UTC -> vendredi 21:00 UTC (un comptage naïf en
# heures calendaires produirait des faux positifs le lundi).
_FX_WEEK_CLOSE_WEEKDAY = 4        # Friday (Monday=0)
_FX_WEEK_CLOSE_HOUR = 21
_FX_WEEK_CLOSED_HOURS = 48.0

_TF_ACTIVE_HOURS: Mapping[str, float] = MappingProxyType({
    "M15": 0.25, "M30": 0.5, "H1": 1.0, "H4": 4.0,
    "D1": 24.0, "DAILY": 24.0,
    "W1": 120.0, "WEEKLY": 120.0,
    "MN": 480.0, "MONTHLY": 480.0,
})


def _fx_active_hours(start: datetime, end: datetime) -> float:
    """Heures de marché FX ouvertes entre start et end (week-ends exclus)."""
    if end <= start:
        return 0.0
    total = (end - start).total_seconds() / 3600.0
    anchor = start.replace(hour=_FX_WEEK_CLOSE_HOUR, minute=0, second=0, microsecond=0)
    anchor -= timedelta(days=(anchor.weekday() - _FX_WEEK_CLOSE_WEEKDAY) % 7)
    if anchor > start:
        anchor -= timedelta(days=7)
    closed = 0.0
    cur = anchor
    while cur < end:
        ov_s = max(cur, start)
        ov_e = min(cur + timedelta(hours=_FX_WEEK_CLOSED_HOURS), end)
        if ov_e > ov_s:
            closed += (ov_e - ov_s).total_seconds() / 3600.0
        cur += timedelta(days=7)
    return max(0.0, total - closed)


def _elapsed_bars_fx(start: datetime, end: datetime, timeframe: str) -> int:
    """Bougies CLOSES entre start et end pour le TF donné. -1 si TF inconnu."""
    bar_h = _TF_ACTIVE_HOURS.get((timeframe or "").upper())
    if not bar_h or bar_h <= 0:
        return -1
    return int(_fx_active_hours(start, end) / bar_h)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — CALENDAR MODELS
# ════════════════════════════════════════════════════════════════════════════
_TIER_S = ("non-farm", "nonfarm", "nfp", "fomc", "cpi", "cash rate",
           "bank rate", "rate statement", "interest rate", "monetary policy",
           "funds rate", "policy rate", "refinancing")
_TIER_A = ("gdp", "pmi", "adp", "pce", "employment change", "unemployment",
           "average hourly", "retail sales", "ppi")
_TIER_B = ("speaks", "speech", "press conference", "testifies", "testimony")


def classify_tier(name: str) -> EventTier:
    n = (name or "").lower()
    if any(k in n for k in _TIER_S):
        return EventTier.S
    if any(k in n for k in _TIER_A):
        return EventTier.A
    if any(k in n for k in _TIER_B):
        return EventTier.B
    return EventTier.NONE


def classify_impact(name: str) -> ImpactLevel:
    return ImpactLevel.HIGH if classify_tier(name) != EventTier.NONE else ImpactLevel.MEDIUM


# (hours_before, hours_after) blackout windows by tier
TIER_WINDOWS: Mapping[EventTier, tuple[float, float]] = MappingProxyType({
    EventTier.S: (4.0, 48.0),
    EventTier.A: (2.0, 24.0),
    EventTier.B: (1.0, 6.0),
})
PROXIMITY_MAX_H = 48.0
WATCH_MAX_H = 168.0

DEFAULT_TIER_WINDOW = (2.0, 24.0)

# Seuils d'audit calendaire.
CAL_TIME_TOL_H = 0.25         # P0-A — tolérance fuseau individuelle (15 min)
CAL_TIME_MIN_RATIO = 0.80     # P0-A — part d'événements concordants requise
CALENDAR_STALE_TOL_H = 0.25   # calendrier ANTÉRIEUR au Desk (CACHE_TTL du
                              # producteur + tolérance d'ordonnancement)
MERGE_STALE_TOL_H = 0.25      # merge ANTÉRIEUR au calendrier

# Devises réellement traitées par le desk — sert à distinguer une jambe-devise
# d'une jambe-instrument (US30, NAS100, SPX500, DE30, XAU) dans le contrôle de
# couverture du flux calendaire (un indice n'a pas de calendrier propre).
_DESK_CURRENCIES = frozenset({"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"})

# Avertissements de source qui décrivent une CAPACITÉ constante du flux, pas un
# incident : ils n'ont rien à faire dans un bandeau d'alerte. Le flux Fair
# Economy hebdomadaire ne publie jamais les résultats (`supports_actual=false`)
# ; l'annoncer comme un problème de fraîcheur contredit `is_stale=false` et
# `quality_status=VALID` émis par la même métadonnée.
_BENIGN_SOURCE_WARNINGS = frozenset({"SOURCE_DOES_NOT_PROVIDE_ACTUAL"})

# Version minimale du schéma merge acceptée — source unique de vérité ;
# app.py applique la même valeur comme gate bloquant côté UI.
MIN_MERGE_SCHEMA: tuple = (3, 4, 0)


class CalendarEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")
    currency: str = Field(..., min_length=3, max_length=3)
    event_name: str = Field(..., max_length=256)
    datetime_utc: datetime
    impact: Optional[ImpactLevel] = None
    tier: EventTier = EventTier.NONE
    # Champs Module 04 (tous optionnels)
    actual: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    forecast_value: Optional[float] = None  # champ numérique Module 04 (fiable)
    previous_value: Optional[float] = None  # idem, côté valeur précédente
    # Identifiant de communiqué : Forex Factory éclate un même release en
    # plusieurs lignes (CPI m/m + y/y + Core m/m + Core y/y). Un desk annonce
    # une publication, pas quatre.
    release_group_id: Optional[str] = None
    hours_until: Optional[float] = None    # pré-calculé par Module 04, audit uniquement
    priority: Optional[str] = None         # CRITICAL/HIGH/MEDIUM/PAST — audit uniquement

    @field_validator("currency")
    @classmethod
    def _up(cls, v: str) -> str:
        return v.upper()

    @field_validator("datetime_utc")
    @classmethod
    def _tz(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _derive(self) -> "CalendarEvent":
        if self.tier is EventTier.NONE:
            self.tier = classify_tier(self.event_name)
        if self.impact is None:
            self.impact = classify_impact(self.event_name)
        # Cohérence bucket() (fenêtres filtrées par impact) vs logique gated
        # S/A (blackout score, F7, régime macro) : un event explicitement HIGH
        # au feed mais au nom non classé ne doit pas être invisible du scoring.
        if self.tier is EventTier.NONE and self.impact is ImpactLevel.HIGH:
            self.tier = EventTier.A
        return self


class CalendarSets(BaseModel):
    model_config = ConfigDict(extra="ignore")
    blackout: list[CalendarEvent] = Field(default_factory=list)
    proximity: list[CalendarEvent] = Field(default_factory=list)
    watch: list[CalendarEvent] = Field(default_factory=list)
    suspended_ccy: set[str] = Field(default_factory=set)
    proximity_ccy: set[str] = Field(default_factory=set)
    watch_ccy: set[str] = Field(default_factory=set)
    time_degraded: bool = False
    time_offset_hours: float = 0.0
    # Couverture partielle du flux — déclenche le fail-closed F7 (f7_macro).
    feed_horizon_truncated: bool = False
    covered_currencies: list[str] = Field(default_factory=list)
    feed_end_utc: Optional[datetime] = None
    # La source publie-t-elle les résultats (`actual`) ? Si non, toute mesure
    # de surprise post-event est structurellement impossible : on ne fabrique
    # pas une pénalité résiduelle à partir d'une valeur par défaut.
    supports_actual: bool = True

    @model_validator(mode="after")
    def _sets(self) -> "CalendarSets":
        self.suspended_ccy = {e.currency for e in self.blackout}
        self.proximity_ccy = {e.currency for e in self.proximity}
        self.watch_ccy = {e.currency for e in self.watch}
        return self


class CalendarData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    events: list[CalendarEvent] = Field(default_factory=list)
    timezone_source: str = "UTC"
    parsed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_html_hash: str = ""
    time_degraded: bool = False
    time_offset_hours: float = 0.0
    time_audit_detail: str = ""
    reachable: bool = True                  # flux potentiellement injoignable
    feed_horizon_truncated: bool = False
    feed_horizon_h: Optional[float] = None  # horizon réel du flux (export coverage)
    feed_coverage_detail: str = ""
    # Fraîcheur Desk vs flux — deux conditions OPPOSÉES, jamais agrégées par
    # abs() ; séparées de l'audit de fuseau (P0-A). N'affecte aucune fenêtre.
    stale: bool = False              # CALENDAR_STALE : calendrier antérieur au Desk
    stale_age_h: float = 0.0
    stale_detail: str = ""
    merge_stale: bool = False        # MERGE_STALE : snapshot marché antérieur au calendrier
    merge_stale_age_h: float = 0.0
    merge_stale_detail: str = ""
    # metadata.filters_applied.currencies (ou ui_filters_applied, ou déduit du flux)
    covered_currencies: list[str] = Field(default_factory=list)
    # Fraîcheur auto-déclarée par la SOURCE du calendrier (metadata Module 04)
    source_stale: bool = False
    source_warnings: list[str] = Field(default_factory=list)
    source_quality: str = "VALID"
    source_quality_score: Optional[float] = None
    supports_actual: bool = True
    # Borne haute réelle du flux (calculée dans load_calendar) — additif, lu
    # uniquement par l'export JSON calendar-coverage du rendu.
    feed_end_utc: Optional[datetime] = None

    def bucket(self, now: datetime) -> CalendarSets:
        """P0-A : si time_degraded, toutes les fenêtres sont élargies de |offset|
        des deux côtés — fail-closed conservateur (les deux champs du feed se
        contredisent ; rien ne prouve lequel est correct)."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        pad = abs(self.time_offset_hours) if self.time_degraded else 0.0
        prox_max = PROXIMITY_MAX_H + pad
        watch_max = WATCH_MAX_H + pad
        blackout, proximity, watch = [], [], []
        for ev in self.events:
            if ev.impact != ImpactLevel.HIGH:
                continue
            before, after = TIER_WINDOWS.get(ev.tier, DEFAULT_TIER_WINDOW)
            before += pad
            after += pad
            delta = (ev.datetime_utc - now).total_seconds() / 3600.0
            if -after <= delta <= before:
                blackout.append(ev)
            elif before < delta <= prox_max:
                proximity.append(ev)
            elif prox_max < delta <= watch_max:
                watch.append(ev)
        return CalendarSets(blackout=blackout, proximity=proximity, watch=watch,
                            time_degraded=self.time_degraded,
                            time_offset_hours=self.time_offset_hours,
                            feed_horizon_truncated=self.feed_horizon_truncated,
                            covered_currencies=list(self.covered_currencies),
                            feed_end_utc=self.feed_end_utc,
                            supports_actual=self.supports_actual)


def audit_calendar_time_consistency(
    events_raw: list[dict],
    generated_at: Optional[datetime],
    tol_h: float = CAL_TIME_TOL_H,
) -> tuple[float, int, int]:
    """P0-A — compare hours_until déclaré et (datetime_utc - generated_at).
    Retourne (offset_median_h, n_concordants, n_verifiables). MESURE seulement.
    """
    if not generated_at:
        return 0.0, 0, 0
    offsets: list[float] = []
    for ev in events_raw or []:
        hu = _safe_float(ev.get("hours_until"))
        dt = _parse_iso_utc(ev.get("datetime_utc"))
        if hu is None or dt is None:
            continue
        offsets.append(hu - (dt - generated_at).total_seconds() / 3600.0)
    if not offsets:
        return 0.0, 0, 0
    med = _median(offsets)
    return med, sum(1 for o in offsets if abs(o - med) <= tol_h), len(offsets)


# ── Regroupement par communiqué ─────────────────────────────────────────────
_TIER_LABEL: Mapping[EventTier, str] = MappingProxyType({
    EventTier.S: "S", EventTier.A: "A", EventTier.B: "B", EventTier.NONE: "—",
})
_TIER_ORDER: Mapping[EventTier, int] = MappingProxyType({
    EventTier.S: 0, EventTier.A: 1, EventTier.B: 2, EventTier.NONE: 3,
})


def release_key(ev: CalendarEvent) -> str:
    """Clé de communiqué. release_group_id quand la source le fournit, sinon
    repli déterministe (devise + horodatage) : deux lignes publiées à la même
    seconde pour la même devise sont le même release."""
    return ev.release_group_id or f"{ev.currency}|{ev.datetime_utc.isoformat()}"


def release_headline(group: list[CalendarEvent]) -> CalendarEvent:
    """Ligne représentative d'un communiqué : tier le plus fort d'abord, puis
    libellé le plus court ('Core CPI y/y' est un détail de 'CPI m/m'), puis
    ordre alphabétique. Entièrement déterministe."""
    return sorted(group, key=lambda e: (_TIER_ORDER[e.tier], len(e.event_name),
                                        e.event_name))[0]


def group_releases(events: Iterable[CalendarEvent],
                   tiers: tuple[EventTier, ...] = (EventTier.S, EventTier.A)
                   ) -> list[list[CalendarEvent]]:
    """Regroupe une liste d'événements en communiqués, filtrés par tier.
    Ordre de sortie : chronologique, puis devise (stable)."""
    groups: dict[str, list[CalendarEvent]] = defaultdict(list)
    for ev in events:
        if ev.tier in tiers:
            groups[release_key(ev)].append(ev)
    out = list(groups.values())
    out.sort(key=lambda g: (min(e.datetime_utc for e in g),
                            release_headline(g).currency))
    return out


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — CANONICAL ASSET VIEW
# ════════════════════════════════════════════════════════════════════════════
class MTFView(BaseModel):
    model_config = ConfigDict(extra="ignore")
    pct: int = 0
    direction: Direction = Direction.NEUTRAL
    quality: str = ""
    nc: int = 0
    age_d1: Optional[int] = 0   # null-safe: gap_open assets send None
    atr_h1: Optional[float] = None
    atr_h4: Optional[float] = None
    atr_daily: Optional[float] = None
    biases: dict[str, str] = Field(default_factory=dict)

    @field_validator("direction", mode="before")
    @classmethod
    def _d(cls, v: Any) -> Direction:
        return _norm_dir(v)


class StructureEventView(BaseModel):
    model_config = ConfigDict(extra="ignore")
    signal_id: str = ""
    kind: str = ""
    direction: Direction = Direction.NEUTRAL
    timeframe: str = ""
    level: Optional[float] = None
    confluence_score: float = 0.0
    status: str = ""
    distance_pct: Optional[float] = None
    distance_atr_multiple: Optional[float] = None
    volatility: str = ""
    force: str = ""
    bb_regime: str = "Normal"
    session: str = ""
    candles_elapsed: int = 999
    signal_time: Optional[datetime] = None  # présent dans le feed

    @field_validator("direction", mode="before")
    @classmethod
    def _d(cls, v: Any) -> Direction:
        return _norm_dir(v)

    @field_validator("signal_time")
    @classmethod
    def _tz(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is None:
            return None
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)


class ZoneView(BaseModel):
    model_config = ConfigDict(extra="ignore")
    level: float
    side: str = ""
    score: float = 0.0
    weighted_score: float = 0.0      # score pondéré par TF (déjà calculé en amont)
    distance_pct: float = 999.0
    timeframes: list[str] = Field(default_factory=list)
    has_weekly: bool = False
    has_daily: bool = False
    has_h4: bool = False


class CanonicalAsset(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str
    base: str = ""
    quote: Optional[str] = None
    asset_class: str = "forex"
    current_price: Optional[float] = None
    current_price_source: Optional[str] = None  # "stale", "live", etc.
    rsi_by_tf: dict[str, dict] = Field(default_factory=dict)
    rsi_h4_status: Optional[str] = None
    mtf: Optional[MTFView] = None
    zones: list[ZoneView] = Field(default_factory=list)
    structure_events: list[StructureEventView] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)
    atr_effective: Optional[float] = None
    atr_source: Optional[str] = None
    nearest_aligned_zone: Optional[ZoneView] = None
    hot_zone_primary: Optional[ZoneView] = None
    conviction_cap: Optional[str] = None  # JSON, e.g. "BBB" (ATR synthétique)
    c7_flag: Optional[str] = None         # délai event S/A (HH:MM approximatif)
    # Produit par le merge engine, read-only ici (None si absent ou crash sur
    # cet actif).
    market_context: Optional[dict[str, Any]] = None


class MergeMeta(BaseModel):
    model_config = ConfigDict(extra="ignore")
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = ""
    assets_count: int = 0
    signals_count: int = 0
    scanners_detected: list[str] = Field(default_factory=list)
    scanners_unknown: int = 0

    @field_validator("generated_at")
    @classmethod
    def _tz(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)


class Clock(BaseModel):
    now_utc: datetime
    now_local: datetime
    date_hdr: str

    # ClassVar obligatoire : sans annotation, Pydantic v2 traiterait
    # _REPORT_TZ comme un attribut privé (ModelPrivateAttr, pas le fuseau).
    _REPORT_TZ: ClassVar[tzinfo] = REPORT_TZ

    @classmethod
    def from_meta(cls, generated_at: datetime) -> "Clock":
        now_utc = generated_at if generated_at.tzinfo else generated_at.replace(tzinfo=timezone.utc)
        now_local = now_utc.astimezone(cls._REPORT_TZ)
        # L'étiquette suit le fuseau réel (CET/CEST) ; le scoring reste ancré UTC.
        tz_label = now_local.tzname() or "CET"
        return cls(now_utc=now_utc, now_local=now_local,
                   date_hdr=f"{now_local.strftime('%Y-%m-%d %H:%M')} {tz_label}")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 5 — V4 MODELS & DATACLASSES
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class ScoredFactor:
    name: str
    raw: Optional[float]
    score: float            # bounded [0,1]
    is_missing: bool
    detail: str


@dataclass
class FactorVector:
    symbol: str
    factors: dict[str, ScoredFactor]

    @property
    def present(self) -> list[str]:
        return [n for n, f in self.factors.items() if not f.is_missing]

    @property
    def missing(self) -> list[str]:
        return [n for n, f in self.factors.items() if f.is_missing]

    @property
    def absolute_mean(self) -> float:
        present = [f.score for f in self.factors.values() if not f.is_missing]
        if not present:
            return 0.0
        return sum(present) / len(present)

    def get(self, name: str) -> float:
        f = self.factors.get(name)
        return f.score if f else 0.0


@dataclass(frozen=True)
class Flag:
    code: str               # "C1".."C11"
    severity: str           # "minor" | "major"
    detail: str


class FactorScores(BaseModel):
    model_config = ConfigDict(extra="ignore")
    f1_hwa: float = 0.0
    f2_rmg: float = 0.0
    f3_ext: float = 0.0
    f4_trg: float = 0.0
    f5_xctx: float = 0.0
    f6_theme: float = 0.0
    f7_macro: float = 0.0
    absolute_mean: float = 0.0        # score post-decay (utilisé pour conviction + ranking)
    absolute_mean_raw: float = 0.0    # score pré-decay (audit uniquement)
    decay_factor: float = 1.0         # multiplicateur appliqué [DECAY_FLOOR, 1.0]
    decay_source: str = "age"
    quantile: float = 0.0
    missing: list[str] = Field(default_factory=list)
    details: dict[str, str] = Field(default_factory=dict)


class FlagModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    code: str
    severity: str
    detail: str


class SetupV4(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str
    direction: Direction
    scenario_hint: str = ""
    entry: float = 0.0
    entry_type: str = "Market"
    sl: float = 0.0
    sl_atr_multiple: float = 0.0
    tp1: float = 0.0
    tp1_atr_multiple: Optional[float] = None
    tp2: Optional[float] = None
    tp2_atr_multiple: Optional[float] = None
    rr: float = 0.0
    rr_synthetic: bool = False
    # Granularité TP1/TP2 pour le badge SR : rr_synthetic seul est insuffisant
    # (TP2 réel + TP1 synthétique = SR partiellement ancré). None = non déterminé.
    tp1_synthetic: Optional[bool] = None
    tp2_synthetic: Optional[bool] = None
    atr_effective: float = 0.0
    atr_source: str = "unknown"
    distance_atr: float = 0.0
    choch_score: Optional[float] = None
    choch_info: Optional[str] = None   # label court si CHoCH présent (aligné ou non)
    gps_quality: Optional[str] = None
    mtf_pct: int = 0
    rsi_h4: Optional[float] = None
    rsi_h4_status: Optional[str] = None
    age_d1: int = 0
    cal_status: CalStatus = CalStatus.OK
    cal_note: str = ""
    htf_aligned: bool = False
    sl_detail: str = ""
    rr_detail: str = ""
    rationale: str = ""
    conviction: Conviction = Conviction.BBB
    factor_scores: FactorScores = Field(default_factory=FactorScores)
    flags: list[FlagModel] = Field(default_factory=list)
    cluster: str = ""
    capped_reason: Optional[str] = None
    reject_code: Optional[str] = None
    reject_detail: Optional[str] = None
    current_price: float = 0.0    # prix courant au scan, validé par preflight
    asset_class: str = "forex"    # propagé depuis CanonicalAsset.asset_class
    age_known: bool = True
    horizon_days: Optional[float] = None
    horizon_event: Optional[str] = None
    horizon_event_days: Optional[float] = None
    invalidation: dict[str, str] = Field(default_factory=dict)
    rr_if_market: Optional[float] = None


class Universe(BaseModel):
    model_config = ConfigDict(extra="ignore")
    passed: list[CanonicalAsset] = Field(default_factory=list)
    rejected: list[tuple[CanonicalAsset, GateCode, str]] = Field(default_factory=list)


class Eliminated(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str
    direction: Direction = Direction.NEUTRAL
    scenario: Optional[str] = None
    reject_code: str
    reject_detail: str
    rsi_h4: Optional[float] = None
    age_d1: int = 0
    cal_status: CalStatus = CalStatus.OK
    rr: Optional[float] = None
    age_known: bool = True
    asset_class: str = "forex"    # cf. SetupV4.asset_class


@dataclass
class MarketThemes:
    strong: dict[str, str] = field(default_factory=dict)        # ccy -> "Bullish"/"Bearish"
    cohesion: dict[str, float] = field(default_factory=dict)    # ccy -> [0,1] consensus strength

    def bonus_for(self, base: str, quote: Optional[str], direction: Direction) -> float:
        """F6 score in [0,1]: how well the trade rides dominant currency themes."""
        d = direction.value
        inv = "Bearish" if d == "Bullish" else "Bullish"
        contributions: list[float] = []
        # base leg
        if base in self.strong:
            coh = self.cohesion.get(base, 0.0)
            contributions.append(coh if self.strong[base] == d else -coh)
        # quote leg (inverse)
        if quote and quote in self.strong:
            coh = self.cohesion.get(quote, 0.0)
            contributions.append(coh if self.strong[quote] == inv else -coh)
        if not contributions:
            return 0.5  # neutral when no theme touches the pair
        signed = sum(contributions) / len(contributions)  # [-1,1]
        return _clamp01((signed + 1.0) / 2.0)

    def is_counter_theme(self, base: str, quote: Optional[str],
                         direction: Direction) -> tuple[bool, float]:
        """True + cohesion if the trade fights a high-cohesion dominant theme."""
        d = direction.value
        inv = "Bearish" if d == "Bullish" else "Bullish"
        worst = 0.0
        counter = False
        if base in self.strong and self.strong[base] != d:
            counter = True
            worst = max(worst, self.cohesion.get(base, 0.0))
        if quote and quote in self.strong and self.strong[quote] != inv:
            counter = True
            worst = max(worst, self.cohesion.get(quote, 0.0))
        return counter, worst


# ════════════════════════════════════════════════════════════════════════════
# SECTION 6 — CONFIG  (seuils V4 ; bon sens structurel, non calibré)
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class V4Config:
    # universe
    MIN_QUALITY: frozenset = frozenset({"A+", "A"})
    MIN_CONSENSUS_PCT: int = 50
    # F1 HWA — ordinal seniority weights
    HWA_WEIGHTS: Mapping[str, int] = field(default_factory=lambda: MappingProxyType(
        {"MN": 6, "W1": 5, "D1": 4, "H4": 3, "H1": 2, "M15": 1}))
    # F2 RMG
    RMG_FAST: tuple = ("M15", "H1")
    RMG_SLOW: tuple = ("D1", "W1")
    RMG_SCALE: float = 15.0
    RMG_MIN_TF: int = 3
    # F3 EXT
    EXT_TF_COUNT: int = 5
    # F4 TRG
    TRG_SCORE_CAP: float = 85.0
    TRG_FRESH_MAX: int = 6
    TRG_DIST_ATR_MAX: float = 1.0
    # SR structure bonus dans F4 (zone SR multi-TF proche du trigger)
    SR_BONUS_MAX: float = 0.20
    SR_DIST_MAX_PCT: float = 2.0
    SR_W_W1: float = 0.50
    SR_W_D1: float = 0.30
    SR_W_H4: float = 0.20
    # F6 THEME
    THEME_MIN_VOTES: int = 3
    THEME_BULL_HI: float = 0.8
    THEME_BULL_LO: float = 0.2
    THEME_COHESION_C5: float = 0.8
    # F7 MACRO
    MACRO_TAU_HOURS: float = 48.0
    # conviction (ABSOLUTE thresholds)
    AAA_MIN: float = 0.80
    AA_MIN: float = 0.68
    A_MIN: float = 0.55
    BBB_MIN: float = 0.42
    BB_MIN: float = 0.30
    MACRO_CAP_RISK_THRESHOLD: float = 0.50   # macro RISK >= 0.5 -> cap AA
    # DÉPRÉCIÉ (10.5.1) — conservé uniquement pour compat descendante des
    # configs JSON existantes (clé acceptée, silencieusement sans effet).
    # f7_macro calcule désormais TOUJOURS le risque par devise dès qu'un event
    # S/A daté est identifié pour cette devise, avant tout garde-fou de
    # couverture globale : un event connu n'est jamais remplacé par un 0.00
    # « non mesuré ». Le fail-closed ne s'applique plus qu'aux deux angles
    # morts réels (devise hors flux, ou silence sur une fenêtre tronquée).
    # Ce changement DÉPLACE LES CONVICTIONS produites par le moteur — validez
    # sur des sessions archivées avant bascule en production live, comme
    # l'exigeait déjà la doctrine d'activation de ce flag.
    MACRO_COVERAGE_GRANULAR: bool = False
    # alpha decay (age_d1 -> score penalty). DECAY_TIME_CONSTANT est le tau de
    # exp(-age/tau), PAS une demi-vie (demi-vie = tau × ln(2) ≈ 24 j).
    DECAY_TIME_CONSTANT: int = 35
    DECAY_FLOOR: float = 0.30
    # contradictions
    C1_TRG_MIN: float = 0.5
    C1_RMG_MAX: float = 0.35
    C2_EXT_MAX: float = 0.3
    C2_HWA_MAX: float = 0.5
    # Le seuil du flag C3 est MACRO_CAP_RISK_THRESHOLD (pas de constante
    # séparée, pour éviter toute dérive cap/flag).
    C4_DIST_ATR: float = 1.0
    # ── Cohérence d'horizon (C7) ────────────────────────────────────────────
    # [NON CALIBRÉ] Alerte d'ordre de grandeur, pas une mesure.
    HORIZON_ATR_REALIZATION_RATE: float = 0.6
    HORIZON_MARGIN: float = 1.25
    # ── Réconciliation de fraîcheur (C9) ────────────────────────────────────
    FRESHNESS_TOLERANCE_BARS: int = 1
    FRESHNESS_NEUTRAL: float = 0.5
    # ── Régime macro portefeuille ───────────────────────────────────────────
    MACRO_REGIME_WINDOW_H: float = 48.0   # fenêtre GLISSANTE
    MACRO_REGIME_MIN_S: int = 3           # slots tier-S DISTINCTS
    MACRO_VACUUM_H: float = 120.0
    # ── C10 — divergence RSI senior contre-tendance ─────────────────────────
    C10_DIV_SENIOR_TFS: tuple = ("W1", "D1")
    C10_DIV_MIN_EVIDENCE: float = 0.25
    # ── Contrat d'invalidation (P1-D) ───────────────────────────────────────
    INVALIDATION_TIME_MULT: float = 1.5
    # ── Briefing calendaire (affichage) ─────────────────────────────────────
    CAL_BRIEF_CONTEXT_MAX: int = 3        # releases hors book affichés en toile de fond
    # preflight
    RR_MIN: float = 1.5
    RR_MAX: float = 20.0
    SL_FLOOR_MULT: float = 0.8
    SL_MAX_ATR_MULT: float = 3.0
    DEFAULT_BB_MULT: float = 1.5
    BB_REGIME_MULT: Mapping[str, float] = field(default_factory=lambda: MappingProxyType(
        {"Squeeze": 1.0, "Normal": 1.5, "Expansion": 2.0}))
    FRESH_ATR_MAX: float = 0.3
    LIMIT_ZONE_MAX_DIST: float = 2.0
    TP1_ATR_MULT: float = 2.0
    TP2_ATR_MULT: float = 1.0
    TP_MAX_ATR_MULT: float = 4.0
    # selection
    MAX_SETUPS: int = 5
    MAX_EXPOSURE_PER_CCY: int = 2
    MIN_CONVICTION: str = "BB"

    @classmethod
    def from_dict(cls, d: dict) -> "V4Config":
        """Overrides JSON (CLI --config) — doctrine fail-closed : clé inconnue
        ignorée (loggée), type incompatible rejeté ici plutôt que de faire
        exploser un seuil non numérique au milieu du scoring."""
        base = cls()
        kw = {}
        for k, v in (d or {}).items():
            if not hasattr(base, k):
                logger.warning("config override clé inconnue ignorée: %s", k)
                continue
            cur = getattr(base, k)
            if isinstance(cur, bool):
                if not isinstance(v, bool):
                    raise ValueError(f"config.{k}: attendu un booléen, reçu {v!r}")
                kw[k] = v
            elif isinstance(cur, (int, float)):
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    raise ValueError(f"config.{k}: attendu un nombre, reçu {v!r}")
                kw[k] = float(v) if isinstance(cur, float) else int(v)
            elif isinstance(cur, str):
                if not isinstance(v, str):
                    raise ValueError(f"config.{k}: attendu une chaîne, reçu {v!r}")
                kw[k] = v
            elif isinstance(cur, frozenset):
                if not isinstance(v, (list, set, frozenset)):
                    raise ValueError(f"config.{k}: attendu une liste, reçu {v!r}")
                kw[k] = frozenset(str(x).upper() for x in v)
            elif isinstance(cur, tuple):
                if not isinstance(v, (list, tuple)):
                    raise ValueError(f"config.{k}: attendu une liste, reçu {v!r}")
                kw[k] = tuple(v)
            elif isinstance(cur, Mapping):
                if not isinstance(v, dict) or not v:
                    raise ValueError(f"config.{k}: attendu un objet JSON non vide, reçu {v!r}")
                if not all(isinstance(x, (int, float)) and not isinstance(x, bool)
                           for x in v.values()):
                    raise ValueError(f"config.{k}: valeurs numériques requises, reçu {v!r}")
                kw[k] = MappingProxyType({str(kk): float(vv) for kk, vv in v.items()})
            else:
                kw[k] = v
        return cls(**kw)


CONFIG = V4Config()

# Execution-context point maps for F5 XCTX (bounded, categorical)
_XCTX_FORCE = MappingProxyType({
    "fort": 1.0, "strong": 1.0,
    "moyen": 0.6, "medium": 0.6,
    "": 0.5,
    "faible": 0.0, "weak": 0.0,
})
_XCTX_VOL = MappingProxyType({
    "haute": 1.0, "high": 1.0,
    "moyenne": 0.55, "medium": 0.55,
    "": 0.5,
    "faible": 0.3, "basse": 0.3, "low": 0.3,
})
_XCTX_SESSION = MappingProxyType({
    "london": 1.0, "newyork": 1.0, "ny": 1.0, "us": 1.0, "london_ny_overlap": 1.0,
    "overlap": 1.0, "overlap_london_ny": 1.0,
    "asian": 0.5, "tokyo": 0.5, "sydney": 0.5,
    "off": 0.0,
    "": 0.3,
})
_XCTX_BB = MappingProxyType({"squeeze": 1.0, "normal": 0.6, "expansion": 0.3, "": 0.6})

_EXT_STATUSES = ("extreme_overbought", "extreme_oversold", "overbought", "oversold")

# RSI divergence — poids par timeframe (somme théorique max = 0.74).
_DIV_TF_WEIGHT: Mapping[str, float] = MappingProxyType(
    {"W1": 0.35, "D1": 0.20, "H4": 0.12, "H1": 0.05, "M15": 0.02}
)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 7 — THEME DETECTION (avec cohésion)
# ════════════════════════════════════════════════════════════════════════════
def detect_currency_themes(assets: Mapping[str, CanonicalAsset],
                           cfg: V4Config = CONFIG) -> MarketThemes:
    votes: dict[str, list[str]] = defaultdict(list)
    for a in assets.values():
        if not a.mtf or a.mtf.direction is Direction.NEUTRAL:
            continue
        d = a.mtf.direction.value
        inv = "Bearish" if d == "Bullish" else "Bullish"
        votes[a.base].append(d)
        if a.quote:
            votes[a.quote].append(inv)
    strong: dict[str, str] = {}
    cohesion: dict[str, float] = {}
    for ccy, vs in votes.items():
        if len(vs) < cfg.THEME_MIN_VOTES:
            continue
        bull = vs.count("Bullish") / len(vs)
        if bull >= cfg.THEME_BULL_HI:
            strong[ccy] = "Bullish"
            cohesion[ccy] = bull
        elif bull <= cfg.THEME_BULL_LO:
            strong[ccy] = "Bearish"
            cohesion[ccy] = 1.0 - bull
    return MarketThemes(strong=strong, cohesion=cohesion)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 7b — RÉGIME MACRO PORTEFEUILLE
# ════════════════════════════════════════════════════════════════════════════
def classify_macro_regime(cal: Optional[CalendarSets], clock: "Clock",
                          cfg: V4Config = CONFIG) -> MacroRegime:
    """Régime calendaire au niveau PORTEFEUILLE. Déterministe : comptage seul.

    Dédoublonnage par (devise, datetime) : Forex Factory publie plusieurs lignes
    pour un même communiqué (CPI m/m + y/y + Trimmed Mean au même horodatage) —
    un comptage brut franchirait le seuil sur un seul release.

    Fenêtre GLISSANTE : un cluster FOMC/BOE/BOJ étalé sur 48 h ne tient pas
    dans « les 48 h suivantes » mais dans une fenêtre de 48 h glissante.
    """
    if cal is None:
        return MacroRegime.UNKNOWN
    now = clock.now_utc
    horizon = list(cal.blackout) + list(cal.proximity) + list(cal.watch)

    s_slots: dict[str, datetime] = {}  # F-05: release_key -> datetime min
    sa_future: list[float] = []
    sa_past: list[tuple[float, CalendarEvent]] = []
    for ev in horizon:
        delta = (ev.datetime_utc - now).total_seconds() / 3600.0
        if ev.tier is EventTier.S and delta >= 0:
            _k = release_key(ev)
            if _k not in s_slots or ev.datetime_utc < s_slots[_k]:
                s_slots[_k] = ev.datetime_utc
        if ev.tier in (EventTier.S, EventTier.A):
            if delta >= 0:
                sa_future.append(delta)
            else:
                sa_past.append((delta, ev))

    times = sorted(s_slots.values())
    for i, t0 in enumerate(times):
        cnt = sum(1 for t in times[i:]
                  if (t - t0).total_seconds() / 3600.0 <= cfg.MACRO_REGIME_WINDOW_H)
        if cnt >= cfg.MACRO_REGIME_MIN_S:
            return MacroRegime.PRE_POLICY_COMPRESSION

    for delta, ev in sa_past:
        _, after = TIER_WINDOWS.get(ev.tier, DEFAULT_TIER_WINDOW)
        if delta >= -after:
            return MacroRegime.POST_POLICY_REPRICING

    if not any(0.0 <= d <= cfg.MACRO_VACUUM_H for d in sa_future):
        return MacroRegime.EVENT_VACUUM
    return MacroRegime.EVENT_DRIFT


# ════════════════════════════════════════════════════════════════════════════
# SECTION 8 — FACTORS F1..F7  (pure functions, all bounded [0,1])
# ════════════════════════════════════════════════════════════════════════════
def _rsi_value(a: CanonicalAsset, tf: str) -> Optional[float]:
    d = a.rsi_by_tf.get(tf) or a.rsi_by_tf.get(tf.upper()) or a.rsi_by_tf.get(tf.lower())
    if isinstance(d, dict):
        return _safe_float(d.get("value"))
    return _safe_float(d)


def _rsi_status(a: CanonicalAsset, tf: str) -> str:
    d = a.rsi_by_tf.get(tf) or a.rsi_by_tf.get(tf.upper()) or a.rsi_by_tf.get(tf.lower())
    if isinstance(d, dict):
        return str(d.get("status") or "").lower()
    return ""


def _divergence_penalty(a: CanonicalAsset) -> float:
    """Pénalité [0.0, 0.40] : pression des divergences RSI confirmées contraires
    à la direction du trade. Une divergence dans le sens du trade est neutre."""
    if a.mtf is None:
        return 0.0
    direction = a.mtf.direction
    penalty = 0.0
    for tf, w in _DIV_TF_WEIGHT.items():
        d = a.rsi_by_tf.get(tf) or a.rsi_by_tf.get(tf.upper()) or a.rsi_by_tf.get(tf.lower())
        if not isinstance(d, dict):
            continue
        if not d.get("div_confirmed"):
            continue
        div_dir = str(d.get("divergence") or "").lower()
        is_contra = (
            (direction is Direction.BULLISH and div_dir == "bearish")
            or (direction is Direction.BEARISH and div_dir == "bullish")
        )
        if not is_contra:
            continue
        strength = _safe_float(d.get("div_strength_score")) or 0.0
        confidence = _safe_float(d.get("div_confidence_score")) or 0.0
        penalty += w * strength * confidence
    return min(penalty, 0.40)


def _aligned_trigger(a: CanonicalAsset) -> Optional[StructureEventView]:
    """Most recent Fresh CHoCH aligned with MTF direction (lowest candles_elapsed)."""
    if a.mtf is None:
        return None
    want = a.mtf.direction
    cands = [ev for ev in a.structure_events
             if ev.status.lower() == "fresh" and _dir_eq(ev.direction, want)]
    if not cands:
        return None
    return min(cands, key=lambda e: e.candles_elapsed)


def audit_freshness(ev: StructureEventView, now: Optional[datetime],
                    cfg: V4Config = CONFIG) -> tuple[FreshnessAudit, int]:
    """Contrat : signal_time absent ou TF non supporté -> UNKNOWN, comportement
    inchangé (mode silencieux sur un feed sans le champ)."""
    if now is None or ev.signal_time is None:
        return FreshnessAudit.UNKNOWN, -1
    bars = _elapsed_bars_fx(ev.signal_time, now, ev.timeframe)
    if bars < 0:
        return FreshnessAudit.UNKNOWN, -1
    if abs(bars - int(ev.candles_elapsed)) <= cfg.FRESHNESS_TOLERANCE_BARS:
        return FreshnessAudit.FRESH, bars
    return FreshnessAudit.STALE, bars


def f1_hwa(a: CanonicalAsset, cfg: V4Config = CONFIG) -> ScoredFactor:
    if a.mtf is None:
        return ScoredFactor("f1_hwa", None, 0.5, True, "MTF absent")
    biases = a.mtf.biases or {}
    direction = a.mtf.direction
    num = 0
    den = 0
    conflicts: list[str] = []
    for tf, w in cfg.HWA_WEIGHTS.items():
        b = (biases.get(tf) or "Range")
        if _dir_eq(_norm_dir(b), direction) and _norm_dir(b) is not Direction.NEUTRAL:
            s = 1
        elif (_norm_dir(b) is _opposite_dir(direction)
              and _opposite_dir(direction) is not Direction.NEUTRAL):
            s = -1
            conflicts.append(tf)
        else:
            s = 0
        num += w * s
        den += w
    raw = (num / den) if den else 0.0
    score = _clamp01((raw + 1.0) / 2.0)
    detail = f"HWA num={num}/den={den} raw={raw:.2f} conflits={conflicts or '∅'}"
    return ScoredFactor("f1_hwa", raw, score, False, detail)


def f2_rmg(a: CanonicalAsset, cfg: V4Config = CONFIG) -> ScoredFactor:
    if a.mtf is None:
        return ScoredFactor("f2_rmg", None, 0.5, True, "MTF absent")
    fast_vals = [v for v in (_rsi_value(a, tf) for tf in cfg.RMG_FAST) if v is not None]
    slow_vals = [v for v in (_rsi_value(a, tf) for tf in cfg.RMG_SLOW) if v is not None]
    n_available = len(fast_vals) + len(slow_vals)
    if not fast_vals or not slow_vals or n_available < cfg.RMG_MIN_TF:
        return ScoredFactor("f2_rmg", None, 0.5, True,
                            f"RSI insuffisant ({n_available} TF)")
    fast = sum(fast_vals) / len(fast_vals)
    slow = sum(slow_vals) / len(slow_vals)
    grad = fast - slow
    signed = grad if a.mtf.direction is Direction.BULLISH else -grad
    score = _clamp01(0.5 + 0.5 * math.tanh(signed / cfg.RMG_SCALE))
    div_penalty = _divergence_penalty(a)
    score = _clamp01(score - div_penalty)
    detail = f"RMG fast={fast:.1f} slow={slow:.1f} grad={grad:.1f} signed={signed:.1f}"
    if div_penalty > 0.0:
        detail += f" | div_penalty={div_penalty:.3f}"
    return ScoredFactor("f2_rmg", grad, score, False, detail)


def f3_ext(a: CanonicalAsset, cfg: V4Config = CONFIG) -> ScoredFactor:
    if a.mtf is None:
        return ScoredFactor("f3_ext", None, 0.5, True, "MTF absent")
    direction = a.mtf.direction
    ext_in_dir = 0
    checked = 0
    for tf in cfg.HWA_WEIGHTS.keys():
        st = _rsi_status(a, tf)
        if not st:
            continue
        checked += 1
        is_ext = any(k in st for k in _EXT_STATUSES)
        if not is_ext:
            continue
        if direction is Direction.BULLISH and ("overbought" in st):
            ext_in_dir += 1
        elif direction is Direction.BEARISH and ("oversold" in st):
            ext_in_dir += 1
    score = _clamp01(1.0 - ext_in_dir / cfg.EXT_TF_COUNT)
    detail = f"{ext_in_dir}/{cfg.EXT_TF_COUNT} TF surchauffés dans le sens (checked={checked})"
    return ScoredFactor("f3_ext", float(ext_in_dir), score, False, detail)


def _sr_structure_bonus(a: CanonicalAsset, cfg: V4Config) -> tuple[float, str]:
    """Bonus SR [0.0, SR_BONUS_MAX] si une zone SR proche est confirmée multi-TF."""
    if a.mtf is None or not a.zones:
        return 0.0, "SR: pas de zones"

    direction = a.mtf.direction

    def _side_ok(z: ZoneView) -> bool:
        s = z.side.upper()
        if direction is Direction.BULLISH:
            return s in ("BUY", "SUPPORT", "ROLE REVERSE", "")
        return s in ("SELL", "RESISTANCE", "ROLE REVERSE", "")

    candidates = [z for z in a.zones
                  if z.distance_pct <= cfg.SR_DIST_MAX_PCT and _side_ok(z)]
    if not candidates:
        return 0.0, f"SR: aucune zone compatible <{cfg.SR_DIST_MAX_PCT}%"

    best = min(candidates, key=lambda z: z.distance_pct)
    composite = (
        cfg.SR_W_W1 * float(best.has_weekly)
        + cfg.SR_W_D1 * float(best.has_daily)
        + cfg.SR_W_H4 * float(best.has_h4)
    )
    bonus = _clamp01(composite) * cfg.SR_BONUS_MAX
    tfs = best.timeframes or (
        (["W1"] if best.has_weekly else [])
        + (["D1"] if best.has_daily else [])
        + (["H4"] if best.has_h4 else [])
    )
    detail = (f"SR: zone@{best.level:.5f} dist={best.distance_pct:.2f}% "
              f"TF={tfs} composite={composite:.2f} bonus={bonus:.3f}")
    return bonus, detail


def f4_trg(a: CanonicalAsset, cfg: V4Config = CONFIG, *,
           now: Optional[datetime] = None) -> ScoredFactor:
    ev = _aligned_trigger(a)
    if ev is None:
        return ScoredFactor("f4_trg", None, 0.0, True, "pas de trigger aligné")
    score_n = min(ev.confluence_score, cfg.TRG_SCORE_CAP) / cfg.TRG_SCORE_CAP

    status, bars_real = audit_freshness(ev, now, cfg)
    if status is FreshnessAudit.STALE:
        fresh = cfg.FRESHNESS_NEUTRAL
        fresh_note = (f"fresh NEUTRALISÉ ({cfg.FRESHNESS_NEUTRAL:.2f}) — "
                      f"déclaré {ev.candles_elapsed}c, réel {bars_real}c")
    else:
        fresh = 1.0 - min(ev.candles_elapsed, cfg.TRG_FRESH_MAX) / cfg.TRG_FRESH_MAX
        fresh_note = (f"fresh={fresh:.2f} [{status.value}"
                      + (f", réel {bars_real}c" if bars_real >= 0 else "") + "]")

    dist = ev.distance_atr_multiple if ev.distance_atr_multiple is not None else cfg.TRG_DIST_ATR_MAX
    proximity = 1.0 - min(dist, cfg.TRG_DIST_ATR_MAX) / cfg.TRG_DIST_ATR_MAX
    base = _clamp01(0.4 * score_n + 0.3 * fresh + 0.3 * proximity)
    sr_bonus, sr_detail = _sr_structure_bonus(a, cfg)
    score = _clamp01(base + sr_bonus)
    detail = (f"TRG score_n={score_n:.2f} {fresh_note} prox={proximity:.2f} "
              f"(conf={ev.confluence_score:.0f}, {ev.candles_elapsed}c, {dist:.2f}ATR) "
              f"| {sr_detail}")
    return ScoredFactor("f4_trg", ev.confluence_score, score, False, detail)


def _norm_lookup(v: Any) -> str:
    return str(v or "").strip().lower()


def f5_xctx(a: CanonicalAsset, cfg: V4Config = CONFIG) -> ScoredFactor:
    ev = _aligned_trigger(a)
    if ev is None:
        return ScoredFactor("f5_xctx", None, 0.5, True, "trigger absent (contexte neutre)")
    force = _XCTX_FORCE.get(_norm_lookup(ev.force), 0.5)
    vol = _XCTX_VOL.get(_norm_lookup(ev.volatility), 0.5)
    session = _XCTX_SESSION.get(_norm_lookup(ev.session), 0.3)
    bb = _XCTX_BB.get((ev.bb_regime or "").lower(), 0.6)
    score = _clamp01((force + vol + session + bb) / 4.0)
    detail = (f"XCTX force={force:.1f} vol={vol:.1f} sess={session:.1f} bb={bb:.1f} "
              f"({ev.force}/{ev.volatility}/{ev.session}/{ev.bb_regime})")
    return ScoredFactor("f5_xctx", None, score, False, detail)


def f6_theme(a: CanonicalAsset, themes: MarketThemes,
             cfg: V4Config = CONFIG) -> ScoredFactor:
    if a.mtf is None:
        return ScoredFactor("f6_theme", None, 0.5, True, "MTF absent")
    score = themes.bonus_for(a.base, a.quote, a.mtf.direction)
    detail = (f"THEME {a.base}/{a.quote or '—'} dir={a.mtf.direction.value} "
              f"strong={themes.strong} -> {score:.2f}")
    return ScoredFactor("f6_theme", None, score, False, detail)


def _parse_ff_value(s: Optional[str]) -> Optional[float]:
    """Parse les valeurs Forex Factory ('0.5%', '25.8K', '-0.1%') en float."""
    if not s or s in ("—", "", "N/A", "n/a"):
        return None
    try:
        s2 = (s.strip()
               .replace("%", "")
               .replace("K", "e3")
               .replace("B", "e9")
               .replace("M", "e6"))
        return float(s2)
    except ValueError:
        return None


def _surprise_factor(ev: CalendarEvent) -> float:
    """Magnitude de surprise pour un event passé, dans [0.2, 1.0].

    N'est appelée QUE lorsque la source publie réellement les résultats
    (`CalendarSets.supports_actual`). Sur un flux sans `actual`, cette fonction
    retournerait 0.7 pour chaque événement — une pénalité résiduelle inventée à
    partir de rien. L'appelant garde donc la responsabilité du filtre.
    """
    actual = _parse_ff_value(ev.actual)
    forecast = _parse_ff_value(ev.forecast)
    if forecast is None:
        forecast = ev.forecast_value
    if actual is None or forecast is None:
        # Event qualitatif (Rate Statement, press conf) : pas de chiffre à
        # comparer, risque modéré par défaut.
        return 0.7
    if abs(forecast) < 1e-9:
        return 0.5
    deviation = abs(actual - forecast) / (abs(forecast) + 1e-9)
    return _clamp01(1.0 - min(deviation * 2.0, 0.8))


def f7_macro(a: CanonicalAsset, cal: Optional[CalendarSets], clock: Clock,
             cfg: V4Config = CONFIG) -> ScoredFactor:
    # cal absent -> aucune information, quelle que soit la devise : angle mort
    # réel, fail-closed immédiat (risque NON écarté, pas risque nul).
    if cal is None:
        return ScoredFactor("f7_macro", None, 0.0, True,
                            "aucun calendrier chargé — fail-closed (risque NON écarté, "
                            "pas risque nul)")
    sides = {a.base, (a.quote or "")}
    # Blackout active -> score 0 (hard veto handled in preflight)
    if sides & cal.suspended_ccy:
        return ScoredFactor("f7_macro", 1.0, 0.0, False, "BLACKOUT actif")
    now = clock.now_utc
    horizon: list[CalendarEvent] = list(cal.blackout) + list(cal.proximity) + list(cal.watch)

    # ── Chemin futur ────────────────────────────────────────────────────────
    # Le risque par devise est calculé AVANT tout garde-fou de couverture
    # globale : un event S/A identifié pour CETTE devise (avec son horodatage
    # réel) est une donnée, pas une absence — le fail-closed ne doit jamais
    # écraser une information qu'on possède déjà.
    relevant_h: list[float] = []
    for ev in horizon:
        if ev.tier not in (EventTier.S, EventTier.A):
            continue
        if ev.currency not in sides:
            continue
        delta = (ev.datetime_utc - now).total_seconds() / 3600.0
        if delta >= 0:
            relevant_h.append(delta)

    if not relevant_h:
        # Ici seulement : aucun event connu pour cette devise. Deux angles
        # morts réels et distincts, fail-closed dans les deux cas ; sinon,
        # silence == absence de risque S/A à l'horizon couvert (donnée, pas
        # supposition).
        cov = set(cal.covered_currencies or ())
        uncovered = sorted(c for c in sides
                           if cov and c in _DESK_CURRENCIES and c not in cov)
        if uncovered:
            return ScoredFactor("f7_macro", None, 0.0, True,
                                f"devise(s) hors couverture du flux "
                                f"({', '.join(uncovered)}) — fail-closed "
                                f"(silence invérifiable)")
        _end = getattr(cal, "feed_end_utc", None)
        _cover_h = (((_end - now).total_seconds() / 3600.0)
                    if _end is not None else -1.0)
        # F-04: fail-closed global 168h remplace par un test par setup:
        # le feed couvre-t-il la fenetre de risque pertinente (tau)?
        if cal.feed_horizon_truncated and _cover_h < cfg.MACRO_TAU_HOURS:
            return ScoredFactor("f7_macro", None, 0.0, True,
                                "aucun event S/A dans la fenêtre couverte et "
                                "flux tronqué sous τ=%.0fh (fin +%.0fh) — "
                                "fail-closed" % (cfg.MACRO_TAU_HOURS, _cover_h))
        base_score = 1.0
        base_detail = ("aucun event S/A futur — silence couvert (feed fin "
                       "+%.0fh >= τ)" % _cover_h
                       if cal.feed_horizon_truncated else "aucun event S/A futur")
        base_risk = 0.0
    else:
        hours = min(relevant_h)
        base_risk = math.exp(-hours / cfg.MACRO_TAU_HOURS)
        base_score = _clamp01(1.0 - base_risk)
        base_detail = f"MACRO event S/A dans {hours:.1f}h risk={base_risk:.2f} -> {base_score:.2f}"

    # ── Risque résiduel post-event ──────────────────────────────────────────
    # Deux garde-fous :
    #  1. la source doit publier les résultats, sinon la « surprise » serait une
    #     constante déguisée en mesure ;
    #  2. structurellement, un event passé dans sa fenêtre `after` place déjà sa
    #     devise en BLACKOUT (early-return plus haut) — ce bloc n'est donc actif
    #     que sur des configurations de fenêtres non standard. Conservé tel quel
    #     pour ne pas dépendre implicitement de cette coïncidence.
    residual_parts: list[str] = []
    residual_penalty = 0.0
    if cal.supports_actual:
        for ev in horizon:
            if ev.tier not in (EventTier.S, EventTier.A):
                continue
            if ev.currency not in sides:
                continue
            delta = (ev.datetime_utc - now).total_seconds() / 3600.0
            if delta >= 0:
                continue   # futur, déjà traité ci-dessus
            _, after = TIER_WINDOWS.get(ev.tier, DEFAULT_TIER_WINDOW)
            if delta < -after:
                continue   # hors fenêtre post-event
            surprise = _surprise_factor(ev)
            recency = math.exp(delta / max(after / 3.0, 1.0))   # delta < 0 → (0,1)
            penalty = recency * (1.0 - surprise)
            if penalty > 0.05:
                residual_penalty += penalty
                residual_parts.append(
                    f"résidu {ev.currency} {ev.event_name[:18]} surprise={surprise:.2f}"
                )
        residual_penalty = min(residual_penalty, 0.25)   # cap absolu
    else:
        residual_parts.append("post-event non mesurable (source sans résultats) — "
                              "aucune pénalité inventée")

    final_score = _clamp01(base_score - residual_penalty)
    detail_parts = [base_detail]
    if residual_parts:
        detail_parts.append("post-event: " + "; ".join(residual_parts))
    detail = " | ".join(detail_parts)

    return ScoredFactor("f7_macro", base_risk if relevant_h else 0.0,
                        final_score, False, detail)


def build_factor_vector(a: CanonicalAsset, themes: MarketThemes,
                        cal: Optional[CalendarSets], clock: Clock,
                        cfg: V4Config = CONFIG) -> FactorVector:
    factors = {
        "f1_hwa": f1_hwa(a, cfg),
        "f2_rmg": f2_rmg(a, cfg),
        "f3_ext": f3_ext(a, cfg),
        "f4_trg": f4_trg(a, cfg, now=clock.now_utc),
        "f5_xctx": f5_xctx(a, cfg),
        "f6_theme": f6_theme(a, themes, cfg),
        "f7_macro": f7_macro(a, cal, clock, cfg),
    }
    return FactorVector(symbol=a.symbol, factors=factors)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 9 — SCORING (absolute) + CROSS-SECTION (tie-break/diversif ONLY)
# ════════════════════════════════════════════════════════════════════════════
def _alpha_decay_factor(age_d1: int, cfg: V4Config) -> float:
    """Multiplicateur exponentiel [DECAY_FLOOR, 1.0] selon l'âge du signal."""
    if age_d1 <= 0:
        return 1.0
    raw = math.exp(-age_d1 / cfg.DECAY_TIME_CONSTANT)
    return max(cfg.DECAY_FLOOR, raw)


def resolve_unknown_decay(known_ages: list[int], cfg: V4Config) -> tuple[float, str]:
    """Decay applicable quand age_d1 est None : médiane empirique du decay de
    l'univers du run. Une absence d'information ne paie jamais le bonus de
    fraîcheur maximal."""
    if known_ages:
        return _median([_alpha_decay_factor(a, cfg) for a in known_ages]), "universe_median"
    return cfg.DECAY_FLOOR, "floor_no_reference"


def compute_quantiles(vectors: list[FactorVector]) -> dict[str, float]:
    """Pure-python percentile rank of absolute_mean within the universe."""
    means = [(v.symbol, v.absolute_mean) for v in vectors]
    if not means:
        return {}
    values = sorted(m for _, m in means)
    n = len(values)
    out: dict[str, float] = {}
    for sym, m in means:
        below = sum(1 for x in values if x < m)
        equal = sum(1 for x in values if x == m)
        out[sym] = (below + 0.5 * equal) / n if n else 0.0
    return out


def rank_setups(setups: list[SetupV4], cfg: V4Config = CONFIG) -> list[SetupV4]:
    """Sort DESC by absolute_mean; tie-break f4 -> f1 -> low-macro-risk -> quantile."""
    def key(s: SetupV4):
        fs = s.factor_scores
        return (
            -fs.absolute_mean,
            -fs.f4_trg,
            -fs.f1_hwa,
            -fs.f7_macro,
            -fs.quantile,
            s.symbol,
        )
    return sorted(setups, key=key)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 9b — HORIZON DE CIBLE ET PROCHAIN ÉVÉNEMENT MACRO
# ════════════════════════════════════════════════════════════════════════════
def _target_horizon_days(a: CanonicalAsset, lv: "LevelBundle",
                         cfg: V4Config = CONFIG) -> Optional[float]:
    """[ORDRE DE GRANDEUR] HORIZON_ATR_REALIZATION_RATE n'est pas calibré."""
    atr_d = (a.mtf.atr_daily if a.mtf else None) or lv.atr_effective
    if not atr_d or atr_d <= 0:
        return None
    denom = cfg.HORIZON_ATR_REALIZATION_RATE * atr_d
    return abs(lv.tp1 - lv.entry) / denom if denom > 0 else None


def _next_macro_event_days(a: CanonicalAsset, cal: Optional[CalendarSets],
                           clock: Clock) -> tuple[Optional[float], Optional[str]]:
    """Prochain communiqué tier S/A touchant base ou quote. (jours, libellé).

    Regroupé par release : le libellé annonce la publication, pas la première
    ligne du bloc.
    """
    if cal is None:
        return None, None
    sides = {a.base, (a.quote or "")}
    now = clock.now_utc
    scoped = [ev for ev in (list(cal.blackout) + list(cal.proximity))
              if ev.currency in sides]
    best: Optional[tuple[float, CalendarEvent, int]] = None
    for grp in group_releases(scoped):
        when = min(e.datetime_utc for e in grp)
        d = (when - now).total_seconds() / 3600.0
        if d < 0:
            continue
        if best is None or d < best[0]:
            best = (d, release_headline(grp), len(grp) - 1)
    if best is None:
        return None, None
    hours, head, extra = best
    days = hours / 24.0
    suffix = f" +{extra}" if extra else ""
    return days, f"{head.currency} {head.event_name}{suffix} (J+{days:.1f})"


# ════════════════════════════════════════════════════════════════════════════
# SECTION 10 — CONTRADICTIONS C1..C11
# ════════════════════════════════════════════════════════════════════════════
def _c1_struct_vs_momentum(fv: FactorVector, cfg: V4Config) -> Optional[Flag]:
    if fv.get("f4_trg") > cfg.C1_TRG_MIN and fv.get("f2_rmg") < cfg.C1_RMG_MAX:
        return Flag("C1", "minor",
                    f"Structure forte (TRG={fv.get('f4_trg'):.2f}) mais momentum faible "
                    f"(RMG={fv.get('f2_rmg'):.2f})")
    return None


def _c2_momentum_vs_trend(fv: FactorVector, cfg: V4Config) -> Optional[Flag]:
    if fv.get("f3_ext") < cfg.C2_EXT_MAX and fv.get("f1_hwa") < cfg.C2_HWA_MAX:
        return Flag("C2", "major",
                    f"Parabolique : surchauffe (EXT={fv.get('f3_ext'):.2f}) + alignement "
                    f"faible (HWA={fv.get('f1_hwa'):.2f})")
    return None


def _c3_trend_vs_calendar(a: CanonicalAsset, fv: FactorVector,
                          cal: Optional[CalendarSets], cfg: V4Config) -> Optional[Flag]:
    """Émis dès que le risque (1 - f7) atteint le seuil du cap AA, cohérence
    stricte avec apply_caps(). Les communiqués sont dédoublonnés : « Core PPI
    m/m, PPI m/m » est une seule publication, pas deux risques distincts."""
    if fv.get("f7_macro") > cfg.MACRO_CAP_RISK_THRESHOLD:
        return None
    if cal is None:
        return None
    sides = {a.base, (a.quote or "")}
    scoped = [e for e in (list(cal.blackout) + list(cal.proximity))
              if e.currency in sides]
    groups = group_releases(scoped)
    if not groups:
        return None
    labels: list[str] = []
    for grp in groups[:2]:
        head = release_headline(grp)
        extra = len(grp) - 1
        labels.append(f"{head.currency} {head.event_name}" + (f" +{extra}" if extra else ""))
    return Flag("C3", "major",
                f"Risque calendaire élevé "
                f"(risque={1.0 - fv.get('f7_macro'):.2f}) : "
                + ", ".join(labels))


def _c4_quality_vs_potential(a: CanonicalAsset, cfg: V4Config) -> Optional[Flag]:
    ev = _aligned_trigger(a)
    if ev is None or ev.distance_atr_multiple is None:
        return None
    if ev.distance_atr_multiple > cfg.C4_DIST_ATR:
        return Flag("C4", "minor",
                    f"Chasing : prix à {ev.distance_atr_multiple:.2f}×ATR du trigger "
                    f"(> {cfg.C4_DIST_ATR})")
    return None


def _c5_trade_vs_theme(a: CanonicalAsset, themes: MarketThemes,
                       cfg: V4Config) -> Optional[Flag]:
    if a.mtf is None:
        return None
    counter, coh = themes.is_counter_theme(a.base, a.quote, a.mtf.direction)
    if counter and coh >= cfg.THEME_COHESION_C5:
        return Flag("C5", "major",
                    f"Trade contre thème devise dominant (cohésion={coh:.2f})")
    return None


def _c6_structural_escalation(a: CanonicalAsset) -> Optional[Flag]:
    """Séquence CHoCH contre-tendance ascendante sur seniorité des TF."""
    evs = (a.market_context or {}).get("structure_events_summary") or {}
    if not evs.get("escalation_detected"):
        return None
    seq = evs.get("escalation_sequence") or []
    seq_str = " → ".join(seq) if seq else "multi-TF"
    return Flag("C6", "major",
                f"Escalade structurelle counter-MTF : {seq_str}")


def _c7_horizon_coherence(horizon: Optional[tuple], cfg: V4Config) -> Optional[Flag]:
    """La cible est-elle atteignable avant le prochain choc macro ?"""
    if not horizon:
        return None
    t_target, t_event, label = horizon
    if t_target is None or t_event is None:
        return None
    if t_target > t_event * cfg.HORIZON_MARGIN:
        return Flag("C7", "major",
                    f"Horizon : cible ≈ {t_target:.1f} j > {label or 'event macro'} "
                    f"à {t_event:.1f} j (marge ×{cfg.HORIZON_MARGIN})")
    return None


def _c8_age_unknown(a: CanonicalAsset) -> Optional[Flag]:
    """Âge de structure inconnu : le decay ne repose sur aucune mesure."""
    if a.mtf is not None and a.mtf.age_d1 is None:
        return Flag("C8", "minor",
                    "Âge de structure inconnu — decay substitué par la médiane de l'univers")
    return None


def _c9_freshness_mismatch(a: CanonicalAsset, now: Optional[datetime],
                           cfg: V4Config) -> Optional[Flag]:
    """candles_elapsed déclaré incohérent avec signal_time."""
    ev = _aligned_trigger(a)
    if ev is None:
        return None
    status, bars = audit_freshness(ev, now, cfg)
    if status is FreshnessAudit.STALE:
        return Flag("C9", "minor",
                    f"Fraîcheur incohérente : déclaré {ev.candles_elapsed}c, "
                    f"réel {bars}c ({ev.timeframe})")
    return None


def _c10_htf_divergence(a: CanonicalAsset, cfg: V4Config = CONFIG) -> Optional[Flag]:
    """Divergence RSI confirmée, CONTRAIRE au trade, sur TF senior (W1/D1)."""
    if a.mtf is None or a.mtf.direction is Direction.NEUTRAL:
        return None
    contra = _opposite_dir(a.mtf.direction)
    hits: list[str] = []
    for tf in cfg.C10_DIV_SENIOR_TFS:
        d = (a.rsi_by_tf.get(tf) or a.rsi_by_tf.get(tf.upper())
             or a.rsi_by_tf.get(tf.lower()))
        if not isinstance(d, dict) or not d.get("div_confirmed"):
            continue
        if _norm_dir(d.get("divergence")) is not contra:
            continue
        evidence = ((_safe_float(d.get("div_strength_score")) or 0.0)
                    * (_safe_float(d.get("div_confidence_score")) or 0.0))
        if evidence >= cfg.C10_DIV_MIN_EVIDENCE:
            hits.append(f"{tf}={evidence:.2f}")
    if not hits:
        return None
    return Flag("C10", "major",
                f"Divergence {contra.value} confirmée contre-tendance sur TF "
                f"senior ({', '.join(hits)}) — structure > alignement MTF")


def _c11_stale_price_market_entry(a: CanonicalAsset, entry_type: str) -> Optional[Flag]:
    """Entrée « Market » construite sur un prix marqué périmé."""
    if entry_type != "Market":
        return None
    if (a.current_price_source or "").lower() != "stale":
        return None
    return Flag("C11", "major",
                "Entrée Market construite sur un prix marqué périmé "
                "(current_price_source=stale) par la couche de fusion en amont")


def detect_contradictions(a: CanonicalAsset, fv: FactorVector, themes: MarketThemes,
                          cal: Optional[CalendarSets], cfg: V4Config = CONFIG, *,
                          now: Optional[datetime] = None,
                          horizon: Optional[tuple] = None) -> list[Flag]:
    flags: list[Flag] = []
    for f in (
        _c1_struct_vs_momentum(fv, cfg),
        _c2_momentum_vs_trend(fv, cfg),
        _c3_trend_vs_calendar(a, fv, cal, cfg),
        _c4_quality_vs_potential(a, cfg),
        _c5_trade_vs_theme(a, themes, cfg),
        _c6_structural_escalation(a),
        _c7_horizon_coherence(horizon, cfg),
        _c8_age_unknown(a),
        _c9_freshness_mismatch(a, now, cfg),
        _c10_htf_divergence(a, cfg),
    ):
        if f is not None:
            flags.append(f)
    return flags


# ════════════════════════════════════════════════════════════════════════════
# SECTION 11 — GRADE  (vetos -> caps -> grille). Le quantile n'entre jamais ici.
# ════════════════════════════════════════════════════════════════════════════
def apply_caps(a: CanonicalAsset, fv: FactorVector, cfg: V4Config = CONFIG, *,
               flags: Optional[list[Flag]] = None,
               regime: Optional[MacroRegime] = None
               ) -> tuple[Optional[Conviction], Optional[str]]:
    """Returns the most restrictive cap and a human reason, or (None, None)."""
    caps: list[tuple[Conviction, str]] = []
    if (a.conviction_cap or "").upper() == "BBB":
        caps.append((Conviction.BBB, "conviction_cap=BBB"))
    if (a.atr_source or "").lower() == "synthetic":
        caps.append((Conviction.BBB, "ATR source synthétique"))
    macro_risk = 1.0 - fv.get("f7_macro")
    if macro_risk >= cfg.MACRO_CAP_RISK_THRESHOLD:
        if "f7_macro" in fv.missing:
            caps.append((Conviction.AA,
                         "risque calendaire NON ÉVALUÉ (couverture "
                         "insuffisante) — cap prudentiel"))
        else:
            caps.append((Conviction.AA,
                         f"risque calendaire élevé ({macro_risk:.2f}) "
                         f"— proximité release S/A"))
    if (a.market_context or {}).get("structural_risk") == "Critical":
        caps.append((Conviction.BBB, "risque structurel critique (REVERSAL_RISK)"))
    if flags and any(f.code == "C7" for f in flags):
        caps.append((Conviction.BB, "horizon incohérent avec le calendrier (C7)"))
    if regime is MacroRegime.PRE_POLICY_COMPRESSION:
        caps.append((Conviction.AA,
                     f"régime pré-policy (≥{cfg.MACRO_REGIME_MIN_S} releases S / "
                     f"{cfg.MACRO_REGIME_WINDOW_H:.0f}h)"))
    if not caps:
        return None, None
    cap = min(caps, key=lambda c: _CONVICTION_ORDINAL[c[0].value])
    return cap[0], cap[1]


def grade(absolute_mean: float, flags: list[Flag], cap: Optional[Conviction],
          cfg: V4Config = CONFIG) -> Conviction:
    """Conviction AAA..B à partir du score absolu moyen et des contradictions."""
    minors = sum(1 for f in flags if f.severity == "minor")
    majors = sum(1 for f in flags if f.severity == "major")
    k = minors + 2 * majors
    m = absolute_mean
    if m >= cfg.AAA_MIN and k == 0:
        base = Conviction.AAA
    elif m >= cfg.AA_MIN and k <= 1:
        base = Conviction.AA
    elif m >= cfg.A_MIN and k <= 1:
        base = Conviction.A
    elif m >= cfg.BBB_MIN and k <= 2:
        base = Conviction.BBB
    elif m >= cfg.BB_MIN:
        base = Conviction.BB
    else:
        base = Conviction.B
    if cap is not None:
        if _CONVICTION_ORDINAL[cap.value] < _CONVICTION_ORDINAL[base.value]:
            base = cap
    return base


# ════════════════════════════════════════════════════════════════════════════
# SECTION 12 — LEVELS + PREFLIGHT
# ════════════════════════════════════════════════════════════════════════════
def _is_opposite(zone: ZoneView, direction: Direction) -> bool:
    side = (zone.side or "").upper()
    if direction is Direction.BULLISH:
        return side in ("SELL", "RESISTANCE", "SUPPLY")
    if direction is Direction.BEARISH:
        return side in ("BUY", "SUPPORT", "DEMAND")
    return False


def _get_opposite_zone(a: CanonicalAsset, direction: Direction) -> Optional[ZoneView]:
    zs = [z for z in a.zones if _is_opposite(z, direction)]
    return min(zs, key=lambda z: z.distance_pct) if zs else None


def atr_for_signal(a: CanonicalAsset,
                   ev: Optional[StructureEventView]) -> tuple[float, str]:
    if ev is not None and a.mtf:
        tf = (ev.timeframe or "").upper()
        m = {"H1": a.mtf.atr_h1, "H4": a.mtf.atr_h4, "D1": a.mtf.atr_daily,
             "W1": a.mtf.atr_daily, "MN": a.mtf.atr_daily}
        v = m.get(tf)
        if v and v > 0:
            return float(v), ("atr_daily" if tf in ("D1", "W1", "MN")
                              else f"atr_{tf.lower()}")
    return (a.atr_effective or 0.0), (a.atr_source or "h4")


def compute_entry(a: CanonicalAsset, ev: Optional[StructureEventView],
                  cfg: V4Config) -> tuple[float, str]:
    price = a.current_price or 0.0
    dist = (ev.distance_atr_multiple if (ev and ev.distance_atr_multiple is not None)
            else 999.0)
    if ev and ev.candles_elapsed <= 1 and dist <= cfg.FRESH_ATR_MAX:
        return price, "Market"
    direction = a.mtf.direction if a.mtf else Direction.NEUTRAL
    z = a.nearest_aligned_zone
    if z and z.distance_pct <= cfg.LIMIT_ZONE_MAX_DIST:
        zone_valid = (
            (direction is Direction.BULLISH and z.level < price) or
            (direction is Direction.BEARISH and z.level > price)
        )
        if zone_valid:
            return z.level, "Limit"
    if a.hot_zone_primary:
        hz = a.hot_zone_primary
        hz_valid = (
            (direction is Direction.BULLISH and hz.level < price) or
            (direction is Direction.BEARISH and hz.level > price)
        )
        if hz_valid:
            return hz.level, "Limit"
    return price, "Market"


def compute_sl(a: CanonicalAsset, entry: float, atr: float,
               ev: Optional[StructureEventView],
               cfg: V4Config) -> tuple[float, float, str]:
    direction = a.mtf.direction if a.mtf else Direction.NEUTRAL
    bb_regime = ev.bb_regime if ev else "Normal"
    bb_mult = cfg.BB_REGIME_MULT.get((bb_regime or "").capitalize() or "Normal",
                                     cfg.DEFAULT_BB_MULT)
    if direction is Direction.BULLISH:
        sl_raw = entry - atr * bb_mult
    elif direction is Direction.BEARISH:
        sl_raw = entry + atr * bb_mult
    else:
        sl_raw = entry
    sl = sl_raw
    detail = f"Raw SL={sl_raw:.5f} ({bb_regime} ×{bb_mult})"
    z = a.nearest_aligned_zone
    if z and z.distance_pct <= cfg.LIMIT_ZONE_MAX_DIST:
        max_dist = cfg.SL_MAX_ATR_MULT * atr if atr > 0 else float("inf")
        if direction is Direction.BULLISH:
            cand = z.level - 0.3 * atr
            if cand < sl_raw:
                if abs(entry - cand) <= max_dist:
                    sl = cand
                    detail += f" zone-adj→{sl:.5f}"
                else:
                    detail += (f" [zone-adj REFUSÉ : {abs(entry - cand) / atr:.2f}×ATR "
                               f"> {cfg.SL_MAX_ATR_MULT}×ATR]")
            else:
                detail += f" [zone {z.level:.5f} au-dessus du SL, pas d'ajustement]"
        elif direction is Direction.BEARISH:
            cand = z.level + 0.3 * atr
            if cand > sl_raw:
                if abs(entry - cand) <= max_dist:
                    sl = cand
                    detail += f" zone-adj→{sl:.5f}"
                else:
                    detail += (f" [zone-adj REFUSÉ : {abs(entry - cand) / atr:.2f}×ATR "
                               f"> {cfg.SL_MAX_ATR_MULT}×ATR]")
            else:
                detail += f" [zone {z.level:.5f} sous le SL, pas d'ajustement]"
    min_dist = atr * cfg.SL_FLOOR_MULT
    if abs(entry - sl) < min_dist:
        sl = entry - min_dist if direction is Direction.BULLISH else entry + min_dist
        detail += f" [floored {cfg.SL_FLOOR_MULT}×ATR]"
    return sl, bb_mult, detail


def compute_tp1(a: CanonicalAsset, entry: float, atr: float,
                cfg: V4Config) -> tuple[float, Optional[float], bool]:
    direction = a.mtf.direction if a.mtf else Direction.NEUTRAL
    opp = _get_opposite_zone(a, direction)
    if opp:
        dist_atr = abs(opp.level - entry) / atr if atr > 0 else float("inf")
        if dist_atr <= cfg.TP_MAX_ATR_MULT:
            return opp.level, round(dist_atr, 2), False
    tp1 = (entry + cfg.TP1_ATR_MULT * atr if direction is Direction.BULLISH
           else entry - cfg.TP1_ATR_MULT * atr)
    return tp1, cfg.TP1_ATR_MULT, True


def compute_tp2(a: CanonicalAsset, entry: float, tp1: float, atr: float,
                cfg: V4Config) -> tuple[Optional[float], Optional[float], bool]:
    direction = a.mtf.direction if a.mtf else Direction.NEUTRAL
    opp = [z for z in sorted(a.zones, key=lambda z: z.distance_pct)
           if _is_opposite(z, direction)]
    for z in opp:
        dist_atr = abs(z.level - entry) / atr if atr > 0 else float("inf")
        if dist_atr > cfg.TP_MAX_ATR_MULT:
            return z.level, round(dist_atr, 2), False
    tp2 = (tp1 + cfg.TP2_ATR_MULT * atr if direction is Direction.BULLISH
           else tp1 - cfg.TP2_ATR_MULT * atr)
    return tp2, (round(abs(tp2 - entry) / atr, 2) if atr > 0 else None), True


def compute_rr(entry: float, sl: float, tp1: float, tp2: Optional[float],
               tp1_syn: bool, tp2_syn: bool) -> tuple[float, str]:
    risk = abs(entry - sl)
    if risk <= 0 or math.isclose(risk, 0.0, abs_tol=1e-12):
        return 0.0, "Risk ~0, invalid"
    r1 = abs(tp1 - entry)
    if tp2 is None:
        rr = r1 / risk
        detail = f"RR(TP1 only)={rr:.2f}"
    else:
        r2 = abs(tp2 - entry)
        rr = (0.6 * r1 + 0.4 * r2) / risk
        detail = f"RR=(0.6×{r1:.5f}+0.4×{r2:.5f})/{risk:.5f}={rr:.2f}"
    flags = []
    if tp1_syn:
        flags.append("TP1 synth 2×ATR")
    if tp2_syn:
        flags.append("TP2 synth")
    if flags:
        detail += " [" + ", ".join(flags) + "]"
    return round(rr, 2), detail


@dataclass
class LevelBundle:
    entry: float
    entry_type: str
    sl: float
    sl_atr_multiple: float
    sl_detail: str
    tp1: float
    tp1_atr_multiple: Optional[float]
    tp1_synthetic: bool
    tp2: Optional[float]
    tp2_atr_multiple: Optional[float]
    tp2_synthetic: bool
    rr: float
    rr_detail: str
    atr_effective: float
    atr_source: str
    trigger: Optional[StructureEventView]
    rr_if_market: Optional[float] = None


def build_levels(a: CanonicalAsset, cfg: V4Config = CONFIG) -> LevelBundle:
    ev = _aligned_trigger(a)
    atr, atr_src = atr_for_signal(a, ev)
    entry, entry_type = compute_entry(a, ev, cfg)
    sl, sl_mult, sl_detail = compute_sl(a, entry, atr, ev, cfg)
    tp1, tp1_mult, tp1_syn = compute_tp1(a, entry, atr, cfg)
    tp2, tp2_mult, tp2_syn = compute_tp2(a, entry, tp1, atr, cfg)
    rr, rr_detail = compute_rr(entry, sl, tp1, tp2, tp1_syn, tp2_syn)
    rr_if_market: Optional[float] = None
    price = a.current_price or 0.0
    if entry_type == "Limit" and price > 0:
        rr_if_market, _ = compute_rr(price, sl, tp1, tp2, tp1_syn, tp2_syn)
    return LevelBundle(
        entry=round(entry, 5), entry_type=entry_type,
        sl=round(sl, 5), sl_atr_multiple=sl_mult, sl_detail=sl_detail,
        tp1=round(tp1, 5), tp1_atr_multiple=tp1_mult, tp1_synthetic=tp1_syn,
        tp2=(round(tp2, 5) if tp2 is not None else None),
        tp2_atr_multiple=tp2_mult, tp2_synthetic=tp2_syn,
        rr=rr, rr_detail=rr_detail,
        atr_effective=atr, atr_source=atr_src, trigger=ev,
        rr_if_market=rr_if_market,
    )


def preflight(setup: SetupV4, cfg: V4Config = CONFIG) -> SetupV4:
    if setup.cal_status is CalStatus.BLACKOUT:
        setup.reject_code = "CAL_BLACKOUT"
        setup.reject_detail = setup.cal_note
        return setup
    if setup.atr_effective <= 0:
        setup.reject_code = "NO_ATR"
        setup.reject_detail = "ATR ≤ 0"
        return setup
    if setup.rr < cfg.RR_MIN or setup.rr > cfg.RR_MAX:
        setup.reject_code = "RR_OUT_OF_RANGE"
        setup.reject_detail = f"RR {setup.rr} ∉ [{cfg.RR_MIN},{cfg.RR_MAX}]"
        return setup
    if setup.direction is Direction.BULLISH and setup.sl >= setup.entry:
        setup.reject_code = "SL_SIGN"
        setup.reject_detail = "SL ≥ entry (bullish)"
        return setup
    if setup.direction is Direction.BEARISH and setup.sl <= setup.entry:
        setup.reject_code = "SL_SIGN"
        setup.reject_detail = "SL ≤ entry (bearish)"
        return setup
    if setup.current_price > 0 and setup.atr_effective > 0:
        atr_overshoot = abs(setup.current_price - setup.entry) / setup.atr_effective
        if setup.direction is Direction.BULLISH and setup.current_price >= setup.tp1:
            setup.reject_code = "PRICE_PAST_TP"
            setup.reject_detail = (
                f"Prix {setup.current_price:.5f} ≥ TP1 {setup.tp1:.5f} "
                f"(entry dépassée de +{atr_overshoot:.2f}×ATR)"
            )
            return setup
        if setup.direction is Direction.BEARISH and setup.current_price <= setup.tp1:
            setup.reject_code = "PRICE_PAST_TP"
            setup.reject_detail = (
                f"Prix {setup.current_price:.5f} ≤ TP1 {setup.tp1:.5f} "
                f"(entry dépassée de +{atr_overshoot:.2f}×ATR)"
            )
            return setup
    min_ord = _CONVICTION_ORDINAL.get(cfg.MIN_CONVICTION, 0)
    setup_ord = _CONVICTION_ORDINAL.get(setup.conviction.value, 0)
    if setup_ord < min_ord:
        setup.reject_code = "LOW_CONVICTION"
        setup.reject_detail = (
            f"Conviction {setup.conviction.value} < minimum {cfg.MIN_CONVICTION} "
            f"(score={setup.factor_scores.absolute_mean:.4f} "
            f"raw={setup.factor_scores.absolute_mean_raw:.4f} "
            f"decay={setup.factor_scores.decay_factor:.4f})"
        )
        return setup
    return setup


# ════════════════════════════════════════════════════════════════════════════
# SECTION 13 — DIVERSIFY  (cluster by risk -> representative -> cap -> top N)
# ════════════════════════════════════════════════════════════════════════════
def _split_symbol(symbol: str) -> tuple[str, str]:
    if "/" in symbol:
        b, q = symbol.split("/", 1)
        return b, q
    return symbol, ""


def assign_clusters(setups: list[SetupV4], themes: MarketThemes) -> dict[str, str]:
    out: dict[str, str] = {}
    for s in setups:
        base, quote = _split_symbol(s.symbol)
        d = s.direction.value
        inv = "Bearish" if d == "Bullish" else "Bullish"
        key = None
        if base in themes.strong and themes.strong[base] == d:
            key = f"{base}_{'strong' if d == 'Bullish' else 'weak'}"
        elif quote and quote in themes.strong and themes.strong[quote] == inv:
            key = f"{quote}_{'weak' if d == 'Bullish' else 'strong'}"
        if key is None:
            key = f"isolated:{s.symbol}"
        out[s.symbol] = key
        s.cluster = key
    return out


def _build_correlation_index(correlation_groups: Optional[dict]) -> dict[str, set[str]]:
    idx: dict[str, set[str]] = defaultdict(set)
    for key, members in (correlation_groups or {}).items():
        if not isinstance(members, list):
            continue
        for m in members:
            sym = (m or {}).get("symbol") if isinstance(m, dict) else None
            if sym:
                idx[str(sym)].add(str(key))
    return idx


def diversify(setups: list[SetupV4], themes: MarketThemes,
              cfg: V4Config = CONFIG, *,
              correlation_groups: Optional[dict] = None) -> list[SetupV4]:
    if not setups:
        return []
    assign_clusters(setups, themes)
    groups: dict[str, list[SetupV4]] = defaultdict(list)
    for s in setups:
        groups[s.cluster].append(s)
    representatives: list[SetupV4] = []
    for key, members in groups.items():
        members_sorted = sorted(
            members,
            key=lambda x: (-x.factor_scores.absolute_mean,
                           -_CONVICTION_ORDINAL[x.conviction.value],
                           x.symbol))
        rep = members_sorted[0]
        representatives.append(rep)
        for loser in members_sorted[1:]:
            loser.reject_code = "CLUSTER_DUP"
            loser.reject_detail = f"Représentant cluster {key} = {rep.symbol}"
    ranked = sorted(
        representatives,
        key=lambda x: (-_CONVICTION_ORDINAL[x.conviction.value],
                       -x.factor_scores.absolute_mean,
                       x.symbol))
    corr_idx = _build_correlation_index(correlation_groups)
    net: Counter = Counter()
    kept: list[SetupV4] = []
    kept_meta: list[tuple[str, str, str, set[str], Direction]] = []

    reached_max = False
    for s in ranked:
        if reached_max:
            s.capped_reason = s.capped_reason or "limite MAX_SETUPS atteinte"
            s.reject_code = s.reject_code or "MAX_SETUPS_REACHED"
            continue

        base, quote = _split_symbol(s.symbol)
        sign = 1 if s.direction is Direction.BULLISH else -1

        over_base = abs(net[base] + sign) > cfg.MAX_EXPOSURE_PER_CCY
        over_quote = bool(quote) and abs(net[quote] - sign) > cfg.MAX_EXPOSURE_PER_CCY
        if over_base or over_quote:
            s.capped_reason = "exposition devise"
            s.reject_code = "EXPOSURE_CAP"
            s.cal_note = (s.cal_note + " [capped: exposition devise]").strip()
            continue

        s_groups = corr_idx.get(s.symbol, set())
        corr_hit: Optional[str] = None
        if s_groups:
            for k_sym, k_base, k_quote, k_groups, k_dir in kept_meta:
                shared = {base, quote} & {k_base, k_quote}
                shared.discard("")
                if shared:
                    continue
                common = s_groups & k_groups
                if common and _dir_eq(s.direction, k_dir):
                    corr_hit = f"{sorted(common)[0]} ~ {k_sym}"
                    break
        if corr_hit:
            s.capped_reason = f"corrélation groupe {corr_hit}"
            s.reject_code = "CORRELATION_CAP"
            s.cal_note = (s.cal_note + f" [capped: corrélation {corr_hit}]").strip()
            continue

        net[base] += sign
        if quote:
            net[quote] -= sign
        kept.append(s)
        kept_meta.append((s.symbol, base, quote, s_groups, s.direction))
        if len(kept) >= cfg.MAX_SETUPS:
            reached_max = True
    return kept


# ════════════════════════════════════════════════════════════════════════════
# SECTION 14 — PIPELINE  (linear orchestration, V4)
# ════════════════════════════════════════════════════════════════════════════
def _build_universe(assets: Mapping[str, CanonicalAsset], cal: CalendarSets,
                    cfg: V4Config) -> Universe:
    all_ccy: set[str] = set()
    for a in assets.values():
        all_ccy.add(a.base)
        if a.quote:
            all_ccy.add(a.quote)
    _cov_set = {str(c).upper() for c in cal.covered_currencies}
    uncovered = {c for c in (all_ccy - _cov_set) if c in _DESK_CURRENCIES}
    if uncovered:
        logger.info(
            "R5 devises sans couverture calendaire (f7_macro fail-closed ou défaut): %s",
            sorted(uncovered),
        )

    passed: list[CanonicalAsset] = []
    rejected: list[tuple[CanonicalAsset, GateCode, str]] = []
    for asset in assets.values():
        if asset.mtf is None:
            rejected.append((asset, GateCode.G0_SCHEMA_ASSET_ERROR, "MTF manquant"))
            continue
        base, quote = asset.base, (asset.quote or "")
        if base in cal.suspended_ccy or quote in cal.suspended_ccy:
            hit = ({base, quote} & cal.suspended_ccy)
            note = f"Blackout: {sorted(hit)}"
            if cal.time_degraded:
                note += f" [fenêtres élargies ±{abs(cal.time_offset_hours):.1f}h — P0-A]"
            rejected.append((asset, GateCode.G1_CAL_BLACKOUT, note))
            continue
        quality = asset.mtf.quality or ""
        if quality not in cfg.MIN_QUALITY:
            rejected.append((asset, GateCode.G2_LOW_QUALITY, f"Quality {quality}"))
            continue
        if asset.mtf.direction is Direction.NEUTRAL:
            rejected.append((asset, GateCode.G3_NO_DIRECTION, "Direction Neutral"))
            continue
        if asset.mtf.pct < cfg.MIN_CONSENSUS_PCT:
            rejected.append((asset, GateCode.G4_LOW_CONSENSUS, f"MTF {asset.mtf.pct}%"))
            continue
        if asset.atr_effective is None or asset.atr_effective <= 0:
            rejected.append((asset, GateCode.G5_NO_ATR, f"ATR {asset.atr_source}"))
            continue
        passed.append(asset)
    return Universe(passed=passed, rejected=rejected)


def _compute_cal_status(a: CanonicalAsset,
                        cal: Optional[CalendarSets]) -> tuple[CalStatus, str]:
    if cal is None:
        return CalStatus.OK, ""
    sides = {a.base, (a.quote or "")}
    hit_black = sides & cal.suspended_ccy
    if hit_black:
        scoped = [e for e in cal.blackout if e.currency in hit_black]
        labels = []
        for grp in group_releases(scoped, tiers=(EventTier.S, EventTier.A,
                                                 EventTier.B, EventTier.NONE)):
            head = release_headline(grp)
            extra = len(grp) - 1
            labels.append(f"{head.currency} {head.event_name}"
                          + (f" +{extra}" if extra else ""))
        return CalStatus.BLACKOUT, "; ".join(labels[:3])
    hit_prox = sides & cal.proximity_ccy
    if hit_prox:
        return CalStatus.PROXIMITY, ", ".join(sorted(hit_prox))
    hit_watch = sides & cal.watch_ccy
    if hit_watch:
        return CalStatus.WATCH, ", ".join(sorted(hit_watch))
    return CalStatus.OK, ""


def _scenario_hint(a: CanonicalAsset, lv: LevelBundle) -> str:
    """Descriptive label only — NOT a scoring pivot in V4."""
    parts = []
    age_known = (a.mtf is not None and a.mtf.age_d1 is not None)
    age = int(a.mtf.age_d1) if (a.mtf and age_known) else 0
    if lv.trigger is not None:
        ev = lv.trigger
        parts.append(f"CHoCH {ev.timeframe} {ev.candles_elapsed}c "
                     f"score={ev.confluence_score:.0f}")
    elif a.hot_zone_primary:
        parts.append("Hot Zone")
    if not age_known:
        parts.append("âge inconnu")
    elif age <= 15:
        parts.append("trend frais")
    elif age <= 30:
        parts.append("trend mûr")
    else:
        parts.append(f"trend âgé {age}j")
    parts.append(lv.entry_type)
    return " · ".join(parts)


def _htf_aligned(a: CanonicalAsset) -> bool:
    if a.mtf is None:
        return False
    d1 = a.mtf.biases.get("D1", "")
    h4 = a.mtf.biases.get("H4", "")
    dt = a.mtf.direction.value.lower()
    return dt in d1.lower() and dt in h4.lower()


def _rationale(a: CanonicalAsset, fv: FactorVector, themes: MarketThemes,
               flags: list[Flag], lv: Optional[LevelBundle] = None) -> str:
    if lv is None:
        lv = build_levels(a)
    parts = [f"Score brut {fv.absolute_mean:.2f}"]
    top = sorted(fv.present, key=lambda n: -fv.get(n))[:3]
    parts.append("forts: " + ", ".join(f"{n.split('_')[0].upper()}={fv.get(n):.2f}"
                                       for n in top))
    if lv.trigger:
        ev = lv.trigger
        parts.append(f"trigger {ev.direction.value} {ev.timeframe} "
                     f"({ev.session}, {ev.bb_regime})")
    if a.mtf:
        tb = themes.bonus_for(a.base, a.quote, a.mtf.direction)
        if tb > 0.6:
            parts.append(f"thème favorable ({tb:.2f})")
    ctx = a.market_context or {}
    market_state = ctx.get("market_state")
    if market_state and market_state not in ("DATA_INCOMPLETE", "RANGE_COMPRESSION"):
        parts.append(f"état {market_state}")
    risk_drivers = ctx.get("structural_risk_drivers") or []
    if risk_drivers:
        parts.append(f"risque: {risk_drivers[0]}")
    if flags:
        parts.append("flags: " + ", ".join(f.code for f in flags))
    return " · ".join(parts)


def _best_choch_info(a: CanonicalAsset) -> Optional[str]:
    """Label court pour le meilleur CHoCH disponible, aligné ou non."""
    if not a.structure_events:
        return None
    fresh = [ev for ev in a.structure_events if ev.status.lower() == "fresh"]
    if not fresh:
        return None
    trade_dir = a.mtf.direction if a.mtf else None

    def _sort_key(ev: StructureEventView) -> tuple:
        aligned = int(_dir_eq(ev.direction, trade_dir)) if trade_dir else 0
        return (-aligned, -(ev.confluence_score or 0))

    best = sorted(fresh, key=_sort_key)[0]
    tf = best.timeframe or "?"
    score = int(best.confluence_score or 0)
    candles = best.candles_elapsed
    label = f"{tf} {best.direction.value} {score} ({candles}c)"
    if trade_dir and not _dir_eq(best.direction, trade_dir):
        label += " ⚠contra"
    return label


def _invalidation_structure(a: CanonicalAsset, cfg: V4Config) -> str:
    d = a.mtf.direction if a.mtf else Direction.NEUTRAL
    opp = _opposite_dir(d)
    ev = _aligned_trigger(a)
    tf = (ev.timeframe.upper() if (ev and ev.timeframe) else "D1")
    if tf not in cfg.HWA_WEIGHTS:
        tf = "D1"
    txt = f"CHoCH {opp.value} confirmé sur {tf} ou timeframe plus senior"
    cur = ((a.market_context or {}).get("structure_events_summary") or {}).get(
        "highest_counter_tf")
    if cur:
        txt += f" — counter déjà présent sur {cur}"
    return txt


def _build_invalidation_contract(a, lv, cal, clock, horizon_days, horizon_event,
                                 cfg: V4Config) -> dict[str, str]:
    """P1-D — sortie pure. Toutes les composantes sont déjà calculées ailleurs."""
    if clock is None:
        return {}
    if horizon_days is not None and horizon_days > 0:
        dl = clock.now_utc + timedelta(days=cfg.INVALIDATION_TIME_MULT * horizon_days)
        time_txt = (f"{dl.strftime('%Y-%m-%d %H:%M UTC')} "
                    f"(×{cfg.INVALIDATION_TIME_MULT} horizon {horizon_days:.1f} j)")
    else:
        time_txt = "UNKNOWN — horizon non calculable"
    if horizon_event:
        ev_txt = horizon_event
    else:
        sides = {a.base, (a.quote or "")}
        cov = set(cal.covered_currencies or ()) if cal is not None else set()
        no_data = sorted(c for c in sides if cov and c not in cov)
        ev_txt = "" if no_data else "aucun event S/A sur base ou quote dans l'horizon"
    return {
        "price": f"{lv.sl:.5f} (stop)",
        "time": time_txt,
        "event": (f"contrat EXPIRE à : {ev_txt}" if horizon_event else ev_txt),
        "structure": _invalidation_structure(a, cfg),
    }


def _make_draft(a: CanonicalAsset, fv: FactorVector,
                cal: Optional[CalendarSets], cfg: V4Config,
                lv: Optional[LevelBundle] = None,
                clock: Optional[Clock] = None) -> SetupV4:
    if lv is None:
        lv = build_levels(a, cfg)
    cal_status, cal_note = _compute_cal_status(a, cal)
    age_known = (a.mtf is not None and a.mtf.age_d1 is not None)
    age_val = int(a.mtf.age_d1) if (a.mtf and a.mtf.age_d1 is not None) else 0
    h_days = _target_horizon_days(a, lv, cfg)
    ev_days, ev_label = (_next_macro_event_days(a, cal, clock) if clock else (None, None))
    fs = FactorScores(
        f1_hwa=round(fv.get("f1_hwa"), 4),
        f2_rmg=round(fv.get("f2_rmg"), 4),
        f3_ext=round(fv.get("f3_ext"), 4),
        f4_trg=round(fv.get("f4_trg"), 4),
        f5_xctx=round(fv.get("f5_xctx"), 4),
        f6_theme=round(fv.get("f6_theme"), 4),
        f7_macro=round(fv.get("f7_macro"), 4),
        absolute_mean=round(fv.absolute_mean, 4),
        quantile=0.0,
        missing=list(fv.missing),
        details={n: f.detail for n, f in fv.factors.items()},
    )
    return SetupV4(
        symbol=a.symbol,
        direction=(a.mtf.direction if a.mtf else Direction.NEUTRAL),
        scenario_hint=_scenario_hint(a, lv),
        entry=lv.entry, entry_type=lv.entry_type,
        sl=lv.sl, sl_atr_multiple=lv.sl_atr_multiple,
        tp1=lv.tp1, tp1_atr_multiple=lv.tp1_atr_multiple,
        tp2=lv.tp2, tp2_atr_multiple=lv.tp2_atr_multiple,
        rr=lv.rr, rr_synthetic=(lv.tp1_synthetic or lv.tp2_synthetic),
        tp1_synthetic=lv.tp1_synthetic, tp2_synthetic=lv.tp2_synthetic,
        atr_effective=lv.atr_effective, atr_source=lv.atr_source,
        distance_atr=(lv.trigger.distance_atr_multiple or 0.0) if lv.trigger else 0.0,
        choch_score=(lv.trigger.confluence_score if lv.trigger else None),
        choch_info=_best_choch_info(a),
        gps_quality=(a.mtf.quality if a.mtf else None),
        mtf_pct=(a.mtf.pct if a.mtf else 0),
        rsi_h4=_rsi_value(a, "H4"), rsi_h4_status=a.rsi_h4_status,
        cal_status=cal_status, cal_note=cal_note,
        htf_aligned=_htf_aligned(a),
        sl_detail=lv.sl_detail, rr_detail=lv.rr_detail,
        factor_scores=fs,
        current_price=(a.current_price or 0.0),
        asset_class=a.asset_class,
        age_d1=age_val, age_known=age_known,
        rr_if_market=lv.rr_if_market,
        horizon_days=(round(h_days, 2) if h_days is not None else None),
        horizon_event=ev_label,
        horizon_event_days=(round(ev_days, 2) if ev_days is not None else None),
        invalidation=_build_invalidation_contract(a, lv, cal, clock, h_days,
                                                  ev_label, cfg),
    )


def _pipeline_factors_and_grades(
    universe: Universe,
    themes: MarketThemes,
    cal_sets: CalendarSets,
    clock: Clock,
    config: V4Config,
    regime: MacroRegime = MacroRegime.UNKNOWN,
) -> tuple[list[FactorVector], list[SetupV4], dict[str, LevelBundle]]:
    """Etapes 5-7 : factor vectors, drafts, quantiles, contradictions, grade."""
    vectors: list[FactorVector] = []
    drafts: list[SetupV4] = []
    lv_cache: dict[str, LevelBundle] = {}
    for a in universe.passed:
        fv = build_factor_vector(a, themes, cal_sets, clock, config)
        vectors.append(fv)
        lv = build_levels(a, config)
        lv_cache[a.symbol] = lv
        drafts.append(_make_draft(a, fv, cal_sets, config, lv, clock))
    quantiles = compute_quantiles(vectors)
    for s in drafts:
        s.factor_scores.quantile = round(quantiles.get(s.symbol, 0.0), 4)
    asset_by_sym = {a.symbol: a for a in universe.passed}
    fv_by_sym = {v.symbol: v for v in vectors}
    known_ages = [s.age_d1 for s in drafts if s.age_known]
    unknown_decay, unknown_src = resolve_unknown_decay(known_ages, config)
    n_unknown = sum(1 for s in drafts if not s.age_known)
    if n_unknown:
        logger.info("P1-A %d actif(s) à âge inconnu — decay = %.4f (%s, n_ref=%d)",
                    n_unknown, unknown_decay, unknown_src, len(known_ages))
    for s in drafts:
        a = asset_by_sym[s.symbol]
        fv = fv_by_sym[s.symbol]
        if s.age_known:
            decay, decay_src = _alpha_decay_factor(s.age_d1, config), "age"
        else:
            decay, decay_src = unknown_decay, unknown_src
        raw_mean = s.factor_scores.absolute_mean
        decayed_mean = _clamp01(raw_mean * decay)
        s.factor_scores.absolute_mean_raw = round(raw_mean, 4)
        s.factor_scores.decay_factor = round(decay, 4)
        s.factor_scores.absolute_mean = round(decayed_mean, 4)
        s.factor_scores.decay_source = decay_src
        flags = detect_contradictions(
            a, fv, themes, cal_sets, config,
            now=clock.now_utc,
            horizon=(s.horizon_days, s.horizon_event_days, s.horizon_event))
        stale_flag = _c11_stale_price_market_entry(a, s.entry_type)
        if stale_flag is not None:
            flags.append(stale_flag)
        s.flags = [FlagModel(code=f.code, severity=f.severity, detail=f.detail)
                   for f in flags]
        cap, cap_reason = apply_caps(a, fv, config, flags=flags, regime=regime)
        s.conviction = grade(decayed_mean, flags, cap, config)
        if cap is not None and cap_reason:
            base_letter = grade(decayed_mean, flags, None, config)
            if (_CONVICTION_ORDINAL[cap.value]
                    < _CONVICTION_ORDINAL[base_letter.value]):
                s.capped_reason = cap_reason
        s.rationale = _rationale(a, fv, themes, flags, lv_cache.get(s.symbol))
    return vectors, drafts, lv_cache


def _pipeline_rank_and_diversify(
    drafts: list[SetupV4],
    themes: MarketThemes,
    config: V4Config,
    correlation_groups: Optional[dict] = None,
) -> tuple[list[SetupV4], list[SetupV4], list[SetupV4]]:
    """Etapes 8-10 : preflight, rank, diversify."""
    for s in drafts:
        preflight(s, config)
    valid = [s for s in drafts if s.reject_code is None]
    preflight_rejects = [s for s in drafts if s.reject_code is not None]
    ranked = rank_setups(valid, config)
    final = diversify(ranked, themes, config, correlation_groups=correlation_groups)
    return final, preflight_rejects, ranked


def _pipeline_collect_eliminated(
    universe: Universe,
    preflight_rejects: list[SetupV4],
    ranked: list[SetupV4],
    final: list[SetupV4],
    cal: Optional[CalendarSets] = None,
) -> list[Eliminated]:
    """Etape 11 : collecte des actifs éliminés."""
    eliminated = _collect_eliminated(universe, cal)
    eliminated.extend(_eliminated_from_setups(preflight_rejects))
    final_syms = {s.symbol for s in final}
    non_reps = [s for s in ranked if s.symbol not in final_syms]
    eliminated.extend(_eliminated_from_setups(non_reps))
    return eliminated


def run_pipeline(
    merged_path: str,
    calendar_json_path: Optional[str] = None,
    output_path: Optional[str] = None,
    pdf_path: Optional[str] = None,
    config: V4Config = CONFIG,
) -> str:
    """Pipeline complet : 2 JSON en entrée, HTML (et PDF optionnel) en sortie."""
    # 1 — ingestion
    meta, assets, correlation_groups = load_merged(merged_path)
    calendar_data = load_calendar(calendar_json_path, desk_generated_at=meta.generated_at)
    clock = Clock.from_meta(meta.generated_at)

    # 2 — calendar buckets
    cal_sets = calendar_data.bucket(clock.now_utc)

    # 3 — universe gates
    universe = _build_universe(assets, cal_sets, config)

    # 4 — themes + régime macro portefeuille
    themes = detect_currency_themes(assets, config)
    regime = classify_macro_regime(cal_sets, clock, config)
    logger.info("P1-C régime macro portefeuille = %s", regime.value)

    # 5-7 — factor vectors, drafts, grades
    _, drafts, _ = _pipeline_factors_and_grades(
        universe, themes, cal_sets, clock, config, regime)

    # 8-10 — preflight, rank, diversify
    final, preflight_rejects, ranked = _pipeline_rank_and_diversify(
        drafts, themes, config, correlation_groups)

    # 11 — collect eliminated
    eliminated = _pipeline_collect_eliminated(
        universe, preflight_rejects, ranked, final, cal_sets)

    # 12 — render (HTML)
    html = render_report(final, eliminated, meta, clock, cal_sets, themes,
                         n_passed=len(universe.passed), cfg=config,
                         correlation_groups=correlation_groups,
                         macro_regime=regime,
                         cal_time_degraded=calendar_data.time_degraded,
                         cal_time_detail=calendar_data.time_audit_detail,
                         cal_feed_truncated=calendar_data.feed_horizon_truncated,
                         cal_feed_detail=calendar_data.feed_coverage_detail,
                         cal_stale_h=calendar_data.stale_age_h,
                         cal_merge_stale_h=calendar_data.merge_stale_age_h,
                         cal_covered_currencies=calendar_data.covered_currencies,
                         cal_source_stale=calendar_data.source_stale,
                         cal_source_warnings=calendar_data.source_warnings,
                         cal_source_quality=calendar_data.source_quality,
                         cal_reachable=calendar_data.reachable,
                         cal_feed_end_utc=calendar_data.feed_end_utc,
                         cal_feed_horizon_h=calendar_data.feed_horizon_h,
                         version=__version__)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
    # 12b — PDF natif (optionnel, jamais bloquant).
    if pdf_path:
        _fb = (pdf_path[:-4] + ".html" if pdf_path.lower().endswith(".pdf")
               else pdf_path + ".html")
        render_pdf(html, pdf_path, fallback_html=_fb)
    return html


def _collect_eliminated(universe: Universe,
                        cal: Optional[CalendarSets] = None) -> list[Eliminated]:
    out: list[Eliminated] = []
    for asset, code, detail in universe.rejected:
        m = asset.mtf
        h4 = asset.rsi_by_tf.get("H4") if asset.rsi_by_tf else None
        cal_status, _ = _compute_cal_status(asset, cal)
        out.append(Eliminated(
            symbol=asset.symbol,
            direction=(m.direction if m else Direction.NEUTRAL),
            reject_code=code.value, reject_detail=detail,
            rsi_h4=(_safe_float(h4.get("value")) if isinstance(h4, dict) else None),
            age_d1=((m.age_d1 or 0) if m else 0),
            age_known=(m is not None and m.age_d1 is not None),
            cal_status=cal_status,
            asset_class=asset.asset_class,
        ))
    return out


def _eliminated_from_setups(setups: list[SetupV4]) -> list[Eliminated]:
    return [Eliminated(
        symbol=s.symbol, direction=s.direction, scenario=s.scenario_hint,
        reject_code=(s.reject_code or "CLUSTER_DUP"),
        reject_detail=(s.reject_detail or s.capped_reason or "non-représentant cluster"),
        rsi_h4=s.rsi_h4, age_d1=s.age_d1, cal_status=s.cal_status, rr=s.rr,
        age_known=s.age_known,
        asset_class=s.asset_class,
    ) for s in setups]


# ════════════════════════════════════════════════════════════════════════════
# SECTION 15 — INGESTION
# ════════════════════════════════════════════════════════════════════════════
def load_merged(merged_path: str) -> tuple[MergeMeta, dict[str, CanonicalAsset], dict]:
    with open(merged_path, encoding="utf-8") as f:
        raw = json.load(f)
    meta = MergeMeta.model_validate(raw.get("meta", {}))
    if meta.version:
        try:
            min_v = MIN_MERGE_SCHEMA
            min_version = ".".join(str(x) for x in min_v)
            meta_v = tuple(int(x) for x in meta.version.split(".")[:3])
            if meta_v < min_v:
                logger.warning("Schéma merge obsolète: %s (minimum recommandé: %s)",
                               meta.version, min_version)
        except (ValueError, AttributeError):
            logger.warning("Version schéma non parseable: %s", meta.version)
    else:
        logger.warning("Version schéma absente dans le merge")
    assets: dict[str, CanonicalAsset] = {}
    for sym, a in (raw.get("assets") or {}).items():
        try:
            assets[sym] = CanonicalAsset.model_validate(a)
        except Exception as exc:
            logger.warning("asset %s skipped: %s", sym, exc)
    raw_corr = raw.get("correlation_groups")
    correlation_groups: dict = raw_corr if isinstance(raw_corr, dict) else {}
    _hz_first: dict[str, dict] = {}
    for _hz in (raw.get("hot_zones") or []):
        if isinstance(_hz, dict) and isinstance(_hz.get("symbol"), str):
            _hz_first.setdefault(_hz["symbol"], _hz)
    for _sym, _a in assets.items():
        if _a.hot_zone_primary is None and _sym in _hz_first:
            try:
                _a.hot_zone_primary = ZoneView.model_validate(_hz_first[_sym])
            except Exception as exc:
                logger.warning("hot_zone %s ignorée: %s", _sym, exc)
    if meta.scanners_unknown:
        logger.warning("merge: %d scanner(s) inconnu(s) au registre producer",
                       meta.scanners_unknown)
    diags = raw.get("diagnostics")
    if isinstance(diags, list):
        _sev: Counter = Counter(
            str(d.get("severity", "?")).lower() for d in diags if isinstance(d, dict))
        for d in diags:
            if isinstance(d, dict) and str(d.get("severity", "")).lower() in ("error",
                                                                              "warning"):
                logger.warning("merge diagnostic [%s/%s] %s", d.get("stage"),
                               d.get("code"), str(d.get("message"))[:200])
        if _sev:
            logger.info("diagnostics merge: %s", dict(_sev))
    if isinstance(raw.get("signals"), list) and meta.signals_count \
            and len(raw["signals"]) != meta.signals_count:
        logger.warning("meta.signals_count=%d mais %d objet(s) dans signals",
                       meta.signals_count, len(raw["signals"]))
    return meta, assets, correlation_groups


def _extract_covered_currencies(meta: Mapping[str, Any]) -> list[str]:
    """Couverture déclarée. Le wrapper Module 04 expose selon les versions
    `filters_applied` ou `ui_filters_applied` — lire une seule des deux clés
    fait silencieusement retomber sur le repli par déduction."""
    for key in ("filters_applied", "ui_filters_applied"):
        blk = meta.get(key)
        if isinstance(blk, dict):
            cur = blk.get("currencies")
            if isinstance(cur, list) and cur:
                return sorted({str(c).upper() for c in cur})
    return []


def load_calendar(calendar_json_path: Optional[str],
                  desk_generated_at: Optional[datetime] = None) -> CalendarData:
    """Charge calendar.json — format wrapper Module 04 ou CalendarData natif.

    Priorité de lecture des events (wrapper) :
      1. ``events_engine`` — champ dédié ENGINE (passés 72h + futurs)
      2. ``events``        — champ UI filtré (fallback ancien format)

    Deux audits temporels DISTINCTS et non confondus :
      - P0-A (fuseau / cohérence interne) : `hours_until` vs `datetime_utc`
        rapportés à la propre horloge du flux (`metadata.generated_at_utc`).
      - Fraîcheur croisée Desk vs flux (`desk_generated_at`) : CALENDAR_STALE
        (calendrier antérieur au Desk) et MERGE_STALE (snapshot de marché
        antérieur au calendrier). Aucun de ces deux flags ne modifie une fenêtre
        de blackout — audit et affichage seulement.
    """
    if not calendar_json_path:
        # F-01: absence de source = angle mort reel -> fail-closed visible,
        # JAMAIS un "aucun risque" implicite (le feed ouvert etait le pire etat).
        return CalendarData(
            feed_horizon_truncated=True,
            feed_coverage_detail="AUCUN FLUX CALENDRIER FOURNI — risque "
                                 "evenementiel NON ecarte (fail-closed)",
            source_warnings=["aucun_calendrier_fourni — fail-closed"])
    with open(calendar_json_path, encoding="utf-8") as f:
        raw = f.read()

    raw_dict: dict = json.loads(raw)

    is_wrapper = "metadata" in raw_dict
    _cov_is_global = False  # F-02: filtres UI explicitement null => flux global
    gen_at: Optional[datetime] = None

    if is_wrapper:
        events_raw: list[dict] = (
            raw_dict.get("events_engine")
            or raw_dict.get("events", [])
        )
        meta = raw_dict.get("metadata", {})
        _cov_is_global = (meta.get("ui_filters_applied") is None
                          and meta.get("filters_applied") is None)

        if not events_raw:
            logger.warning(
                "calendar.json chargé avec 0 events "
                "(total_high_impact=%s, upcoming=%s). "
                "Vérifier la fenêtre temporelle du feed FF ou activer show_past.",
                meta.get("total_high_impact", "?"),
                meta.get("upcoming_count", "?"),
            )
            meta["feed_horizon_truncated"] = True  # F-01: vide != nul

        cal_events: list[CalendarEvent] = []
        for ev in events_raw:
            try:
                cal_events.append(CalendarEvent.model_validate(ev))
            except Exception as exc:
                logger.debug("calendar event skipped: %s", exc)

        gen_at = _parse_iso_utc(meta.get("generated_at_utc"))
        off, conc, tot = audit_calendar_time_consistency(events_raw, gen_at)
        if tot and (conc / tot) >= CAL_TIME_MIN_RATIO and abs(off) > CAL_TIME_TOL_H:
            time_degraded = True
            time_offset = off
            time_detail = (f"offset systématique {off:+.2f}h entre hours_until et "
                           f"datetime_utc ({conc}/{tot} événements) — fenêtres "
                           f"élargies de ±{abs(off):.1f}h (fail-closed)")
            logger.error("P0-A ALERTE FUSEAU : %s", time_detail)
        else:
            time_degraded, time_offset, time_detail = False, 0.0, ""

        stale, stale_age_h, stale_detail = False, 0.0, ""
        merge_stale, merge_stale_age_h, merge_stale_detail = False, 0.0, ""
        if desk_generated_at and gen_at:
            age_h = (desk_generated_at - gen_at).total_seconds() / 3600.0
            if age_h > CALENDAR_STALE_TOL_H:
                stale = True
                stale_age_h = age_h
                stale_detail = (
                    f"calendrier antérieur au Desk de {age_h:.2f}h "
                    f"(flux: {gen_at:%H:%M:%S} UTC, Desk: {desk_generated_at:%H:%M:%S} UTC)"
                )
                logger.warning("CALENDAR_STALE : %s", stale_detail)
            elif age_h < -MERGE_STALE_TOL_H:
                merge_stale = True
                merge_stale_age_h = -age_h
                merge_stale_detail = (
                    f"snapshot de marché antérieur au calendrier de {-age_h:.2f}h "
                    f"(merge : {desk_generated_at:%H:%M:%S} UTC, "
                    f"calendrier : {gen_at:%H:%M:%S} UTC)"
                )
                logger.warning("MERGE_STALE : %s", merge_stale_detail)

        covered = _extract_covered_currencies(meta)

        data = CalendarData(
            events=cal_events,
            time_degraded=time_degraded,
            time_offset_hours=time_offset,
            time_audit_detail=time_detail,
            stale=stale,
            stale_age_h=stale_age_h,
            stale_detail=stale_detail,
            merge_stale=merge_stale,
            merge_stale_age_h=merge_stale_age_h,
            merge_stale_detail=merge_stale_detail,
            covered_currencies=covered,
            source_stale=bool(meta.get("is_stale", False)),
            source_warnings=[str(w).strip() for w in (meta.get("warnings") or [])
                             if str(w).strip()],
            source_quality=str(meta.get("quality_status") or "VALID"),
            source_quality_score=_safe_float(meta.get("data_quality_score")),
            supports_actual=bool(meta.get("supports_actual", True)),
            reachable=bool(meta.get("reachable", True)),
            feed_horizon_truncated=bool(meta.get("feed_horizon_truncated", False)),
            feed_horizon_h=meta.get("feed_horizon_h"),
            feed_coverage_detail=meta.get("feed_coverage_detail", ""),
        )
    else:
        data = CalendarData.model_validate_json(raw)

    # Journalisation graduée : un avertissement de CAPACITÉ n'est pas un
    # incident de fraîcheur. Seul l'inattendu remonte en WARNING.
    _hard_warnings = [w for w in data.source_warnings
                      if w not in _BENIGN_SOURCE_WARNINGS]
    if data.source_stale or _hard_warnings or data.source_quality.upper() != "VALID":
        logger.warning(
            "SOURCE CALENDRIER : %s",
            " ; ".join(filter(None, [
                "is_stale=true" if data.source_stale else "",
                (f"quality_status={data.source_quality}"
                 if data.source_quality.upper() != "VALID" else ""),
                " ; ".join(_hard_warnings),
            ])),
        )
    elif data.source_warnings:
        logger.info("source calendrier — capacités déclarées : %s",
                    " ; ".join(data.source_warnings))
    if not data.supports_actual:
        logger.info("source sans résultats publiés (supports_actual=false) — "
                    "mesure de surprise post-event désactivée")

    # F-02: ne PLUS deduire la couverture des evenements: "pas d'evenement
    # NZD cette semaine" = info positive, pas angle mort. null explicite =
    # flux global (couverture = desk entier); absence totale de cle = repli
    # prudent sur la deduction (comportement anterieur, signale).
    if not data.covered_currencies:
        if _cov_is_global:
            data.covered_currencies = sorted(_DESK_CURRENCIES)
        elif data.events:
            data.covered_currencies = sorted({ev.currency for ev in data.events})

    # ── Horizon réel du flux vs fenêtre WATCH ────────────────────────────────
    if data.events:
        _ref = gen_at if (is_wrapper and gen_at is not None) else data.parsed_at
        if _ref.tzinfo is None:
            _ref = _ref.replace(tzinfo=timezone.utc)
        _feed_end = max(ev.datetime_utc for ev in data.events)
        _horizon_h = (_feed_end - _ref).total_seconds() / 3600.0
        data.feed_end_utc = _feed_end
        data.feed_horizon_h = _horizon_h
        if _horizon_h < WATCH_MAX_H:
            data.feed_horizon_truncated = True
            data.feed_coverage_detail = (
                f"couverture du flux : {_ref:%d/%m %H:%M} → {_feed_end:%d/%m %H:%M} UTC "
                f"({_horizon_h:+.0f}h) < fenêtre WATCH {WATCH_MAX_H:.0f}h"
            )
            logger.info("FEED-HORIZON TRONQUÉ : %s", data.feed_coverage_detail)
    elif is_wrapper:
        data.feed_horizon_truncated = True
        data.feed_coverage_detail = (
            "flux calendaire vide ou non accessible — risque NON écarté, "
            "pas risque nul"
        )
        logger.warning("FEED TRONQUÉ OU VIDE : %s", data.feed_coverage_detail)

    data.raw_html_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return data


# ════════════════════════════════════════════════════════════════════════════
# SECTION 16 — BRIEFING CALENDAIRE  (couche d'affichage, aucune décision)
# ════════════════════════════════════════════════════════════════════════════
def _consensus_str(ev: CalendarEvent) -> str:
    """« consensus vs précédent ». Les chaînes d'affichage priment (unités et
    signes conservés) ; repli sur les champs numériques Module 04."""
    f = (ev.forecast or "").strip()
    p = (ev.previous or "").strip()
    if not f and ev.forecast_value is not None:
        f = f"{ev.forecast_value:g}"
    if not p and ev.previous_value is not None:
        p = f"{ev.previous_value:g}"
    if f and p:
        return f"{f} vs {p}"
    return f or ""


def build_calendar_briefing(
    setups: list[SetupV4],
    calendar: Optional[CalendarSets],
    clock: Clock,
    cfg: V4Config = CONFIG,
    *,
    covered: Optional[list[str]] = None,
    feed_end_utc: Optional[datetime] = None,
    merge_stale_h: float = 0.0,
    cal_stale_h: float = 0.0,
    source_stale: bool = False,
    source_warnings: Optional[list[str]] = None,
    source_quality: str = "VALID",
    reachable: bool = True,
    time_degraded: bool = False,
    time_detail: str = "",
) -> dict[str, Any]:
    """Briefing calendaire orienté décision.

    Aucune donnée n'est estimée ici : regroupement par communiqué, tri
    chronologique, et mise en regard de l'horizon de cible des setups publiés.
    Ce qui est constaté mais sans conséquence sur une fenêtre de risque descend
    en note d'intégrité ; seul l'actionnable remonte en bandeau.
    """
    out: dict[str, Any] = {
        "events": [], "context": [], "integrity": "", "alert": "",
        "window": "", "has_crossing": False, "n_hidden": 0,
    }
    covered = [str(c).upper() for c in (covered or [])]
    source_warnings = source_warnings or []

    if calendar is None:
        out["alert"] = "aucun calendrier chargé — risque événementiel NON écarté"
        return out

    now = clock.now_utc
    horizon = list(calendar.blackout) + list(calendar.proximity) + list(calendar.watch)

    # Book publié : jambes exposées et horizon de cible par symbole.
    legs: dict[str, set[str]] = defaultdict(set)
    horizon_d: dict[str, float] = {}
    for s in setups:
        b, q = _split_symbol(s.symbol)
        for leg in (b, q):
            if leg:
                legs[leg].add(s.symbol)
        horizon_d[s.symbol] = s.horizon_days or 0.0

    rows: list[dict[str, Any]] = []
    context: list[dict[str, Any]] = []
    for grp in group_releases(horizon):
        head = release_headline(grp)
        when = min(e.datetime_utc for e in grp)
        days = (when - now).total_seconds() / 86400.0
        if days < 0:
            continue
        exposed = sorted(legs.get(head.currency, ()))
        row = {
            "when": f"{when:%d/%m %H:%M}Z",
            "days": round(days, 1),
            "currency": head.currency,
            "tier": _TIER_LABEL[head.tier],
            "label": head.event_name,
            "extra": len(grp) - 1,
            "consensus": _consensus_str(head),
            "symbols": exposed,
            # Cible postérieure à la publication : la position traverse
            # l'événement. C'est la seule ligne qui appelle une décision.
            "crossing": tuple(s for s in exposed if horizon_d.get(s, 0.0) > days),
        }
        (rows if exposed else context).append(row)

    out["events"] = rows
    ctx_s = [r for r in context if r["tier"] == "S"]
    out["context"] = ctx_s[:cfg.CAL_BRIEF_CONTEXT_MAX]
    out["n_hidden"] = len(context) - len(out["context"])
    out["has_crossing"] = any(r["crossing"] for r in rows)
    if feed_end_utc:
        out["window"] = (f"couvert jusqu'au {feed_end_utc:%d/%m %H:%M}Z"
                         + (f" · {', '.join(covered)}" if covered else ""))

    # ── Intégrité : constatée, non commentée ────────────────────────────────
    parts: list[str] = []
    if merge_stale_h > 0:
        parts.append(f"merge antérieur au calendrier de {merge_stale_h * 60:.0f} min "
                     f"(fenêtres inchangées)")
    if cal_stale_h > 0:
        parts.append(f"calendrier antérieur au merge de {cal_stale_h * 60:.0f} min")
    if covered:
        unc = sorted(c for c in legs if c in _DESK_CURRENCIES and c not in covered)
        if unc:
            parts.append(f"hors flux : {', '.join(unc)} — silence non interprétable")
    if "SOURCE_DOES_NOT_PROVIDE_ACTUAL" in source_warnings:
        parts.append("résultats publiés non fournis par la source (attendu sur ce flux)")
    out["integrity"] = " · ".join(parts)

    # ── Alerte : uniquement ce qui change une décision ──────────────────────
    hard = [w for w in source_warnings if w not in _BENIGN_SOURCE_WARNINGS]
    if source_quality and source_quality.upper() != "VALID":
        hard.insert(0, f"qualité de source déclarée {source_quality}")
    if not horizon:
        hard.insert(0, "aucun événement à haut impact reçu — risque NON écarté")
    if not reachable:
        hard.insert(0, "flux calendaire non joignable")
    if source_stale:
        hard.insert(0, "source marquée périmée (is_stale)")
    if time_degraded:
        hard.insert(0, "incohérence de fuseau — résolution intraday non fiable, "
                       "fenêtres élargies par sécurité"
                       + (f" ({time_detail})" if time_detail else ""))
    out["alert"] = " · ".join(hard)
    return out


# ════════════════════════════════════════════════════════════════════════════
# SECTION 17 — RENDER  (template HTML calibré écran + print A4)
# ════════════════════════════════════════════════════════════════════════════
# Pagination : pas de saut forcé par carte ; break-inside évite de couper une
# carte ; en-tête riche en page 1 uniquement ; la section « Éliminés » démarre
# sur une nouvelle page (break-before:page).
_INLINE_TEMPLATE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BLUESTAR FX Desk_Signal Report_{{date_hdr_file}}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');
:root{
  --royal:#1B45B4;--royal-mid:#2355C3;--royal-light:#E8EEFF;--royal-dim:#6B89D8;
  --bg:#f5f7fc;--white:#fff;--card:#f0f3fa;--dark:#0d1f4e;--body:#1a1a2e;--sec:#3a4a7a;--muted:#6B89D8;--th:#E8EEFF;
  --green:#1a7a4a;--grn-bg:#e8f5ee;--grn-bd:#6EE7B7;--grn-tx:#065F46;
  --red:#c0292a;--red-bg:#fdecea;--red-bd:#FCA5A5;--red-tx:#7F1D1D;
  --blue:#2355C3;--purple:#1B45B4;
  --border:#dde3f5;--border2:#bbc6e8;--r:5px;--rl:7px;--gap:12px;
  --sans:'IBM Plex Sans',system-ui,sans-serif;--mono:'IBM Plex Mono','SF Mono','Courier New',monospace
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--body);font-family:var(--sans);font-size:12px;line-height:1.45;-webkit-font-smoothing:antialiased}
#page{max-width:1180px;margin:0 auto;background:var(--bg)}
.wrap{padding:14px 20px}
.section{background:var(--white);border:1px solid var(--border);border-radius:var(--rl);margin-bottom:var(--gap);overflow:hidden;box-shadow:0 1px 3px rgba(13,31,78,.03)}
.sec-hdr{display:flex;align-items:center;gap:10px;padding:9px 16px;border-bottom:1px solid var(--border);background:var(--white)}
.sec-num{width:22px;height:22px;border-radius:50%;background:var(--royal);color:#fff;font-size:10px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-family:var(--mono)}
.sec-ttl{font-size:12px;font-weight:700;color:var(--dark);text-transform:uppercase;letter-spacing:.5px;font-family:var(--mono)}
.sec-sub{margin-left:auto;font-size:9.5px;color:var(--muted);font-style:italic}
.sec-body{padding:12px 16px}
.banner{background:var(--red-bg);border:1px solid var(--red-bd);color:var(--red-tx);border-radius:var(--r);padding:9px 14px;margin-bottom:12px;font-family:var(--mono);font-size:10.5px;font-weight:600}
.banner.warn{background:#fff7e6;border-color:#f0c98a;color:#7a4a00}
.banner.info{background:var(--royal-light);border-color:var(--royal-dim);color:var(--sec);font-weight:500}
/* ── Briefing calendaire ── */
.cal-brief{border:1px solid var(--border);border-radius:var(--r);margin-bottom:12px;overflow:hidden}
.cal-brief-hdr{display:flex;gap:10px;align-items:baseline;padding:6px 12px;background:var(--royal-light);border-bottom:1px solid var(--border);font-family:var(--mono);font-size:9px;font-weight:700;color:var(--royal);letter-spacing:.08em;text-transform:uppercase}
.cal-brief-hdr .win{margin-left:auto;font-weight:500;text-transform:none;letter-spacing:0;color:var(--sec)}
.cal-tbl{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:10px}
.cal-tbl td{padding:4px 12px;border-bottom:1px solid var(--border);background:var(--white)}
.cal-tbl tr:last-child td{border-bottom:none}
.cal-when{white-space:nowrap;font-weight:600;color:var(--dark)}
.cal-ccy{font-weight:700;color:var(--royal);white-space:nowrap}
.cal-ev{color:var(--body)}
.cal-ev .more{color:var(--muted);font-size:9px}
.cal-cons{color:var(--sec);white-space:nowrap}
.cal-chip{display:inline-block;padding:0 6px;margin-right:4px;border-radius:3px;background:var(--card);border:1px solid var(--border2);font-size:9px;color:var(--sec)}
.cal-chip.x{background:#fff7e6;border-color:#f0c98a;color:#7a4a00;font-weight:700}
.cal-ctx,.cal-foot{padding:4px 12px;font-family:var(--mono);font-size:8.5px;color:var(--muted);background:var(--card);border-top:1px solid var(--border);line-height:1.45}
.setup{border:1px solid var(--border);border-radius:var(--rl);overflow:hidden;margin-bottom:11px;box-shadow:0 1px 2px rgba(13,31,78,.03)}
.setup:last-child{margin-bottom:0}
.setup.aaa{border-left:3px solid var(--royal)}.setup.aa{border-left:3px solid var(--royal-mid)}.setup.a{border-left:3px solid var(--green)}.setup.bbb{border-left:3px solid var(--muted)}.setup.bb{border-left:3px solid var(--border2)}.setup.b{border-left:3px solid var(--border2)}
.setup-hdr{display:flex;align-items:center;gap:10px;padding:9px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.setup-hdr.long{background:var(--grn-bg)}.setup-hdr.short{background:var(--red-bg)}
.pair{font-size:16px;font-weight:700;font-family:var(--mono);color:var(--dark)}
.dir{display:inline-flex;align-items:center;gap:4px;padding:2px 9px;border-radius:4px;font-size:10.5px;font-weight:700;font-family:var(--mono)}
.dir.long{background:var(--grn-bg);border:1px solid var(--grn-bd);color:var(--grn-tx)}
.dir.short{background:var(--red-bg);border:1px solid var(--red-bd);color:var(--red-tx)}
.dir.neutral{background:var(--royal-light);border:1px solid var(--royal-dim);color:var(--sec)}
.conv{display:inline-flex;padding:2px 9px;border-radius:4px;font-size:10.5px;font-weight:700;font-family:var(--mono)}
.conv.aaa{background:var(--royal-light);border:1px solid var(--royal-dim);color:var(--royal)}
.conv.aa{background:var(--royal-light);border:1px solid var(--royal-dim);color:var(--royal-mid)}
.conv.a{background:var(--grn-bg);border:1px solid var(--grn-bd);color:var(--green)}
.conv.bbb,.conv.bb,.conv.b{background:var(--card);border:1px solid var(--border2);color:var(--sec)}
.scen-lbl{margin-left:auto;font-size:9.5px;color:var(--muted);font-family:var(--mono)}
.setup-body{padding:12px 16px;background:var(--white)}
.metrics-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:5px;margin-bottom:11px;padding:9px;background:var(--card);border:1px solid var(--border);border-radius:var(--r)}
.metric{text-align:center;padding:3px 0}
.metric-lbl{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;font-family:var(--mono);margin-bottom:2px}
.metric-val{font-size:12px;font-weight:700;font-family:var(--mono)}
.metric-val.na{color:var(--muted);font-style:italic}.metric-val.ok{color:var(--green)}.metric-val.warn{color:var(--royal)}.metric-val.danger{color:var(--red)}
.factor-grid{display:grid;grid-template-columns:repeat(8,1fr);gap:5px;margin-bottom:11px;padding:9px;background:var(--royal-light);border:1px solid var(--royal-dim);border-radius:var(--r)}
.factor{text-align:center}
.factor-lbl{font-size:7.5px;color:var(--royal);text-transform:uppercase;letter-spacing:.5px;font-family:var(--mono);margin-bottom:2px;font-weight:700}
.factor-val{font-size:12px;font-weight:700;font-family:var(--mono);color:var(--dark)}
.factor-val.miss{color:var(--muted);font-style:italic}
.factor.mean .factor-val{color:var(--royal)}
.px-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:7px;margin-bottom:11px}
.px-card{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:8px 10px;text-align:center}
.px-card.entry{border-top:2px solid var(--royal)}.px-card.sl{border-top:2px solid var(--red)}.px-card.tp1{border-top:2px solid var(--green)}.px-card.tp2{border-top:2px solid var(--royal-mid)}.px-card.rr{border-top:2px solid var(--royal-dim)}
.px-lbl{font-size:7.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600;margin-bottom:3px;font-family:var(--mono)}
.px-val{font-size:14px;font-weight:700;font-family:var(--mono)}
.px-sub{font-size:8.5px;color:var(--muted);margin-top:2px}
.rationale{background:var(--royal-light);border-left:3px solid var(--royal);padding:9px 12px;font-size:11px;color:var(--dark);margin-bottom:10px;line-height:1.55;border-radius:var(--r)}
.rationale strong{display:block;font-size:8.5px;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px;color:var(--royal);font-family:var(--mono)}
.flags-row{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.flag{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;font-family:var(--mono)}
.flag.minor{background:var(--royal-light);border:1px solid var(--royal-dim);color:var(--royal-mid)}
.flag.major{background:var(--red-bg);border:1px solid var(--red-bd);color:var(--red-tx)}
.cap-note{font-size:9.5px;color:var(--red);font-family:var(--mono);font-weight:600;margin-bottom:8px}
.cluster-tag{font-size:9px;font-family:var(--mono);color:var(--sec);background:var(--card);border:1px solid var(--border2);padding:1px 7px;border-radius:4px}
.cal-row{display:flex;align-items:center;gap:8px;font-size:10.5px;color:var(--sec);margin-bottom:10px}
.cal-ok,.cal-proximity,.cal-blackout,.cal-watch{padding:2px 8px;border-radius:4px;font-size:9.5px;font-weight:700;font-family:var(--mono)}
.cal-ok{background:var(--grn-bg);border:1px solid var(--grn-bd);color:var(--grn-tx)}
.cal-watch,.cal-proximity{background:var(--royal-light);border:1px solid var(--royal-dim);color:var(--royal-mid)}
.cal-blackout{background:var(--red-bg);border:1px solid var(--red-bd);color:var(--red-tx)}
.sub-lbl{font-size:8.5px;font-weight:700;color:var(--royal);text-transform:uppercase;letter-spacing:1px;margin:11px 0 7px;font-family:var(--mono)}
.sub-lbl:first-child{margin-top:0}
.elim{background:var(--card);border:1px solid var(--border);border-left:3px solid var(--border2);border-radius:var(--r);padding:8px 12px;margin-bottom:6px;display:flex;align-items:flex-start;gap:10px}
.elim.sus{border-left-color:var(--red);background:var(--red-bg)}
.elim-pair{font-size:12px;font-weight:700;font-family:var(--mono);color:var(--sec);min-width:84px;flex-shrink:0}
.elim-txt{font-size:10px;color:var(--muted)}
hr.div{border:none;border-top:1px solid var(--border);margin:9px 0}
table{width:100%;border-collapse:collapse;font-size:11px}
thead tr{background:var(--royal)!important}
thead th{padding:7px 10px;text-align:left;font-size:8.5px;font-weight:700;color:#fff;letter-spacing:.8px;text-transform:uppercase;white-space:nowrap;font-family:var(--mono)}
tbody tr{border-bottom:1px solid var(--border)}
tbody tr:nth-child(even){background:var(--card)}
tbody td{padding:5px 10px;vertical-align:middle}
.no-setup{background:var(--card);border:2px dashed var(--border2);border-radius:var(--rl);padding:36px 20px;text-align:center}
.no-setup-icon{font-size:32px;margin-bottom:10px}.no-setup-title{font-size:15px;font-weight:700;color:var(--dark);margin-bottom:6px}.no-setup-sub{font-size:11px;color:var(--muted);font-family:var(--mono)}
.reject-code{font-family:var(--mono);font-size:9.5px;font-weight:700;color:var(--red)}
.audit-block{background:#0d1f4e;color:#E8EEFF;border-radius:var(--r);padding:9px 12px;margin-top:10px;font-family:var(--mono);font-size:9px;line-height:1.55;word-break:break-word}
.audit-block strong{color:#6EE7B7;font-size:8.5px;text-transform:uppercase;letter-spacing:1px;display:block;margin-bottom:4px}
.footer{text-align:center;font-family:var(--mono);font-size:7.5px;color:var(--muted);border-top:1px solid var(--border);padding:9px 20px;margin-top:4px;letter-spacing:1.2px}
.page-header{background:linear-gradient(135deg,#F8FAFF 0%,#F0F4FE 100%);border:1px solid var(--border);border-radius:var(--rl) var(--rl) 0 0;display:flex;align-items:center;justify-content:space-between;padding:13px 24px;box-shadow:0 1px 4px rgba(13,31,78,.04),inset 0 1px 0 rgba(255,255,255,.8);position:relative}
.page-header::after{content:'';position:absolute;bottom:0;left:24px;right:24px;height:2px;background:linear-gradient(90deg,var(--royal),var(--royal-dim),transparent);border-radius:2px}
.header-left{display:flex;align-items:center;gap:14px}
.logo-marker{width:42px;height:42px;display:flex;align-items:center;justify-content:center;flex-shrink:0;background:var(--white);border:1px solid var(--border);border-radius:var(--r)}
.sys-label{font-size:8.5px;letter-spacing:.3em;color:var(--royal-dim);font-family:var(--mono);font-weight:600;text-transform:uppercase}
.sys-name{font-size:26px;font-weight:700;color:var(--dark);letter-spacing:-.02em;line-height:1.1;font-family:var(--mono)}
.sys-desc{font-size:8.5px;color:var(--muted);font-family:var(--mono);margin-top:2px;letter-spacing:.02em}
.header-right{text-align:right;border-left:1px solid var(--border2);padding-left:18px}
.briefing-label{font-size:10.5px;color:var(--royal);font-family:var(--mono);letter-spacing:.08em;font-weight:600;text-transform:uppercase}
.briefing-sub{font-size:8.5px;color:var(--sec);font-family:var(--mono);margin-top:4px;letter-spacing:.02em}
.page-subbar{background:rgba(27,69,180,.04);border-left:1px solid var(--border);border-right:1px solid var(--border);border-bottom:1px solid var(--border);padding:7px 24px;display:flex;align-items:center;gap:22px;flex-wrap:wrap;font-size:9.5px;font-family:var(--mono);color:var(--sec)}
.confidential{margin-left:auto;color:var(--royal);font-weight:600;background:rgba(27,69,180,.08);padding:2px 10px;border-radius:20px;font-size:8.5px}
.page-top{display:block}

/* ═══════════════ PDF / PRINT — CALIBRAGE A4 ZÉRO-BORD ═══════════════ */
@page{size:A4 portrait;margin:0}

#pdf-fab{position:fixed;bottom:28px;right:28px;z-index:9999}
#pdf-fab button{background:#1B45B4;color:#fff;border:none;padding:11px 20px;border-radius:8px;font-family:var(--mono);font-size:12px;font-weight:700;cursor:pointer;box-shadow:0 4px 16px rgba(27,69,180,.45)}
@media print{
  *{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important}
  html{background:var(--bg)!important;margin:0!important;padding:0!important;width:100%!important;zoom:1!important}
  body{background:var(--bg)!important;margin:0!important;padding:0!important;width:100%!important;font-size:7pt!important;line-height:1.38!important}
  #page{max-width:none!important;width:100%!important;margin:0!important;padding:0!important;background:var(--bg)!important}

  /* ── EN-TÊTE : dans le flux du wrap, margin négatif pour full-bleed ── */
  .page-top{
    break-inside:avoid!important;break-after:avoid!important;
    margin:-5mm -5mm 4mm -5mm!important;
  }
  .page-header{
    border-radius:0!important;box-shadow:none!important;
    padding:3px 5mm!important;min-height:0!important;
    border-left:none!important;border-right:none!important;border-top:none!important;
    border-bottom:1px solid var(--border)!important;
  }
  .page-header::after{display:none!important}
  .header-left{gap:6px!important}
  .sys-name{font-size:10px!important;line-height:1!important}
  .sys-label{font-size:5pt!important;letter-spacing:.1em!important}
  .sys-desc{font-size:5pt!important;margin-top:0!important}
  .briefing-label{font-size:5pt!important}
  .briefing-sub{font-size:5pt!important;margin-top:1px!important}
  .logo-marker{width:16px!important;height:16px!important;padding:1px!important}
  .logo-marker svg{width:14px!important;height:14px!important}
  .header-right{padding-left:8px!important}
  .page-subbar{
    padding:2px 5mm!important;gap:6px!important;font-size:5.5pt!important;
    border-left:none!important;border-right:none!important;
    border-bottom:1px solid var(--border)!important;
  }
  .confidential{font-size:5pt!important;padding:1px 6px!important}
  .wrap{padding:5mm 5mm 8mm 5mm!important}

  /* ── SECTION ── */
  .section{overflow:visible!important;box-shadow:none!important;margin-bottom:7px!important;border:1px solid var(--border)!important}
  .sec-body{overflow:visible!important;padding:7px 6px!important}
  .sec-hdr{padding:5px 10px!important;break-after:avoid!important;page-break-after:avoid!important}
  .sec-num{width:16px!important;height:16px!important;font-size:7pt!important}
  .sec-ttl{font-size:8pt!important}
  .sec-sub{font-size:7pt!important}

  /* ── BRIEFING CALENDAIRE ── */
  .cal-brief{break-inside:avoid!important;page-break-inside:avoid!important;margin-bottom:6px!important}
  .cal-brief-hdr{padding:3px 8px!important;font-size:6pt!important}
  .cal-tbl{font-size:6.5pt!important}
  .cal-tbl td{padding:2px 8px!important}
  .cal-chip{font-size:6pt!important;padding:0 4px!important}
  .cal-ctx,.cal-foot{padding:2px 8px!important;font-size:5.8pt!important}

  /* ── SETUP CARD ── */
  .setup{overflow:visible!important;box-shadow:none!important;margin-bottom:6px!important;
         break-inside:auto!important;page-break-inside:auto!important}
  .setup-hdr{padding:5px 10px!important;gap:6px!important;
             break-before:avoid!important;page-break-before:avoid!important;
             break-after:avoid!important;page-break-after:avoid!important;
             break-inside:avoid!important;page-break-inside:avoid!important}
  .factor-grid{padding:4px 6px!important;gap:3px!important;margin-bottom:5px!important;
               break-before:avoid!important;page-break-before:avoid!important;
               break-inside:avoid!important;page-break-inside:avoid!important}
  .setup-body{padding:6px 10px!important}
  .pair{font-size:12.5px!important}
  .dir,.conv{font-size:7.5pt!important;padding:1px 6px!important}
  .scen-lbl{font-size:7pt!important}
  .cluster-tag{font-size:7pt!important}

  /* ── GRILLES ── */
  .factor-lbl{font-size:6pt!important;margin-bottom:1px!important}
  .factor-val{font-size:9pt!important}
  .metrics-grid{padding:4px 6px!important;gap:3px!important;margin-bottom:5px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .metric-lbl{font-size:6pt!important;margin-bottom:1px!important}
  .metric-val{font-size:9pt!important}
  .px-grid{gap:4px!important;margin-bottom:5px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .px-card{padding:4px 7px!important}
  .px-lbl{font-size:6pt!important;margin-bottom:1px!important}
  .px-val{font-size:10.5pt!important}
  .px-sub{font-size:6.5pt!important;margin-top:1px!important}

  /* ── RATIONALE & AUDIT ── */
  .rationale{padding:5px 8px!important;margin-bottom:5px!important;font-size:6.8pt!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .rationale strong{font-size:6pt!important;margin-bottom:2px!important}
  .flags-row{gap:4px!important;margin-bottom:5px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .flag{font-size:6.5pt!important;padding:1px 5px!important}
  .cap-note{font-size:6.5pt!important;margin-bottom:4px!important}
  .cal-row{font-size:7pt!important;margin-bottom:5px!important;gap:5px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .cal-ok,.cal-proximity,.cal-blackout,.cal-watch{font-size:6.5pt!important;padding:1px 5px!important}
  .audit-block{padding:5px 8px!important;margin-top:5px!important;font-size:5.9pt!important;line-height:1.32!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .audit-block strong{font-size:6pt!important;margin-bottom:2px!important}
  .banner{padding:5px 8px!important;margin-bottom:7px!important;font-size:7pt!important;break-inside:avoid!important;page-break-inside:avoid!important}

  /* ── SECTION 2 ── */
  .section + .section{break-before:page!important;page-break-before:always!important}
  .sub-lbl{font-size:7pt!important;break-after:avoid!important;page-break-after:avoid!important}
  .elim{padding:5px 8px!important;margin-bottom:4px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .elim-pair{font-size:8pt!important;min-width:60px!important}
  .elim-txt{font-size:7pt!important}

  /* ── TABLE ── */
  table{font-size:7pt!important}
  thead th{padding:4px 7px!important;font-size:6.5pt!important}
  tbody td{padding:3px 7px!important}
  tr,thead{break-inside:avoid!important;page-break-inside:avoid!important}
  thead{display:table-header-group!important}
  tfoot{display:table-footer-group!important}
  .reject-code{font-size:7pt!important}

  /* ── DIVERS ── */
  .sus-grid{grid-template-columns:repeat(3,1fr)!important;gap:4px!important}
  .sus-item{padding:3px 7px!important;break-inside:avoid!important;page-break-inside:avoid!important}
  .sus-item-pair{font-size:8pt!important}
  .sus-item-txt{font-size:6.5pt!important}
  p,li{orphans:3;widows:3}
  .footer{display:block!important;padding:5px 5mm!important;font-size:6pt!important;letter-spacing:0!important;border-top:1px solid var(--border)!important}
  a[href]:after{content:""!important}
  #pdf-fab{display:none!important}
}
/* Blackout grid compact */
.sus-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-bottom:8px}
.sus-item{background:var(--red-bg);border:1px solid var(--red-bd);border-left:3px solid var(--red);border-radius:var(--r);padding:5px 9px;display:flex;flex-direction:column;gap:2px}
.sus-item-pair{font-family:var(--mono);font-weight:700;font-size:11px;color:var(--dark)}
.sus-item-txt{font-size:9px;color:var(--muted)}

</style>
</head>
<body>
<div id="pdf-fab"><button onclick="window.print()">Imprimer / PDF (A4)</button></div>
<div id="page">
<div class="wrap">
<div class="page-top">
<div class="page-header">
  <div class="header-left">
    <div class="logo-marker"><svg width="36" height="36" viewBox="0 0 24 24" fill="none"><path d="M12 17.27L18.18 21L16.54 13.97L22 9.24L14.81 8.63L12 2L9.19 8.63L2 9.24L7.46 13.97L5.82 21L12 17.27Z" fill="#1B45B4"/></svg></div>
    <div><div class="sys-label">BLUESTAR SYSTEM</div><div class="sys-name">BLUESTAR</div><div class="sys-desc">FX INSTITUTIONAL DESK · {{version}}</div></div>
  </div>
  <div class="header-right"><div class="briefing-label">FX CASCADE · TRADER</div><div class="briefing-sub">{{date_hdr}}</div></div>
</div>
<div class="page-subbar">
  <span>{{date_hdr}}</span>
  <span style="background:rgba(27,69,180,.12);color:var(--royal);padding:2px 10px;border-radius:20px;font-weight:700;border:1px solid var(--royal-dim)">{{n_setups}} setup(s)</span>
  <span>Universe <strong>{{n_passed}}/{{n_total}}</strong></span>
  <span>Event Risk : <strong style="color:{% if event_risk == 'High' %}var(--red){% elif event_risk == 'Medium' %}#EA580C{% else %}var(--green){% endif %}">{{event_risk}}</strong></span>
  <span>Régime : <strong>{{macro_regime}}</strong></span>
  {% if themes %}<span>Thèmes : {{themes}}</span>{% endif %}
  {% if sr_degraded %}<span style="background:var(--card);border:1px solid var(--border);color:var(--muted);padding:2px 10px;border-radius:20px">SR indisponible · mode ATR</span>{% endif %}
  <span class="confidential">CONFIDENTIEL</span>
</div>
</div><!-- /.page-top -->

<div class="section">
  <div class="sec-hdr"><div class="sec-num">1</div><div class="sec-ttl">Setups Valides</div><div class="sec-sub">{{n_setups}} validé(s) · Universe {{n_passed}}/{{n_total}}</div></div>
  <div class="sec-body">
  {% if cal_brief.alert %}<div class="banner">CALENDRIER — {{cal_brief.alert}}</div>{% endif %}
  {% if cal_brief.events or cal_brief.context %}
  <div class="cal-brief">
    <div class="cal-brief-hdr"><span>Risque événementiel</span>{% if cal_brief.window %}<span class="win">{{cal_brief.window}}</span>{% endif %}</div>
    {% if cal_brief.events %}
    <table class="cal-tbl"><tbody>
    {% for e in cal_brief.events %}
      <tr>
        <td class="cal-when">J+{{e.days}} · {{e.when}}</td>
        <td class="cal-ccy">{{e.currency}} · {{e.tier}}</td>
        <td class="cal-ev">{{e.label}}{% if e.extra %} <span class="more">+{{e.extra}}</span>{% endif %}</td>
        <td class="cal-cons">{{e.consensus or '—'}}</td>
        <td>{% for sym in e.symbols %}<span class="cal-chip{% if sym in e.crossing %} x{% endif %}">{{sym}}{% if sym in e.crossing %} ⟂{% endif %}</span>{% endfor %}</td>
      </tr>
    {% endfor %}
    </tbody></table>
    {% endif %}
    {% if cal_brief.context %}<div class="cal-ctx">Hors book — {% for c in cal_brief.context %}{{c.currency}} {{c.label}}{% if c.extra %} +{{c.extra}}{% endif %} (J+{{c.days}}{% if c.consensus %}, {{c.consensus}}{% endif %}){% if not loop.last %} · {% endif %}{% endfor %}{% if cal_brief.n_hidden %} · +{{cal_brief.n_hidden}} autre(s){% endif %}</div>{% endif %}
    {% if cal_brief.has_crossing %}<div class="cal-ctx">⟂ cible postérieure à la publication — la position traverse l'événement.</div>{% endif %}
    {% if cal_brief.integrity %}<div class="cal-foot">Intégrité — {{cal_brief.integrity}}</div>{% endif %}
  </div>
  {% endif %}
  {% if setups %}
  {% for s in setups %}
  {% set dc = 'long' if s.direction.value == 'Bullish' else 'short' %}
  {% set arrow = '▲' if s.direction.value == 'Bullish' else '▼' %}
  {% set cv = s.conviction.value|lower %}
  {% set fs = s.factor_scores %}
  <div class="setup {{cv}}" data-asset-class="{{s.asset_class}}">
    <div class="setup-hdr {{dc}}">
      <span class="pair">{{s.symbol}}</span>
      <span class="dir {{dc}}">{{arrow}} {{s.direction.value}}</span>
      <span class="conv {{cv}}">{{s.conviction.value}} (ajusté {{ '%.2f'|format((fs.absolute_mean * 100) | int / 100) }})</span>
      <span class="cluster-tag">{{s.cluster}}</span>
      <span class="scen-lbl">{{s.scenario_hint}}{% if s.cal_status.value != 'OK' %} · {{s.cal_status.value}}{% endif %}</span>
    </div>
    <div class="setup-body">
      <div class="factor-grid">
        <div class="factor"><div class="factor-lbl">F1 HWA</div><div class="factor-val {% if 'f1_hwa' in fs.missing %}miss{% endif %}">{{ '—' if 'f1_hwa' in fs.missing else '%.2f'|format(fs.f1_hwa) }}</div></div>
        <div class="factor"><div class="factor-lbl">F2 RMG</div><div class="factor-val {% if 'f2_rmg' in fs.missing %}miss{% endif %}">{{ '—' if 'f2_rmg' in fs.missing else '%.2f'|format(fs.f2_rmg) }}</div></div>
        <div class="factor"><div class="factor-lbl">F3 EXT</div><div class="factor-val {% if 'f3_ext' in fs.missing %}miss{% endif %}">{{ '—' if 'f3_ext' in fs.missing else '%.2f'|format(fs.f3_ext) }}</div></div>
        <div class="factor"><div class="factor-lbl">F4 TRG</div><div class="factor-val {% if 'f4_trg' in fs.missing %}miss{% endif %}">{{ '—' if 'f4_trg' in fs.missing else '%.2f'|format(fs.f4_trg) }}</div></div>
        <div class="factor"><div class="factor-lbl">F5 XCTX</div><div class="factor-val {% if 'f5_xctx' in fs.missing %}miss{% endif %}">{{ '—' if 'f5_xctx' in fs.missing else '%.2f'|format(fs.f5_xctx) }}</div></div>
        <div class="factor"><div class="factor-lbl">F6 THM</div><div class="factor-val {% if 'f6_theme' in fs.missing %}miss{% endif %}">{{ '—' if 'f6_theme' in fs.missing else '%.2f'|format(fs.f6_theme) }}</div></div>
        <div class="factor"><div class="factor-lbl">F7 CAL</div><div class="factor-val {% if 'f7_macro' in fs.missing %}miss{% endif %}">{{ '—' if 'f7_macro' in fs.missing else '%.2f'|format(fs.f7_macro) }}</div></div>
        <div class="factor mean"><div class="factor-lbl">Q-rang</div><div class="factor-val">{{ '%.2f'|format(fs.quantile) }}</div></div>
      </div>
      <div class="metrics-grid">
        <div class="metric"><div class="metric-lbl">Distance ATR</div><div class="metric-val {% if s.choch_score is none %}na{% elif (s.distance_atr or 0) <= 0.3 %}ok{% elif (s.distance_atr or 0) <= 1.0 %}warn{% else %}danger{% endif %}">{% if s.choch_score is none %}—{% else %}{{s.distance_atr|round(2)}}×{% endif %}</div></div>
        <div class="metric"><div class="metric-lbl">Score CHoCH</div><div class="metric-val {% if s.choch_score is none %}na{% elif (s.choch_score or 0) >= 70 %}ok{% elif (s.choch_score or 0) >= 50 %}warn{% else %}danger{% endif %}">{{s.choch_score|round(0)|int if s.choch_score else '—'}}</div>{% if s.choch_info %}<div style="font-size:7px;color:var(--muted);font-family:var(--mono);margin-top:1px">{{s.choch_info}}</div>{% endif %}</div>
        <div class="metric"><div class="metric-lbl">Quality</div><div class="metric-val {% if s.gps_quality in ['A+','A'] %}ok{% else %}warn{% endif %}">{{s.gps_quality or '—'}}</div></div>
        <div class="metric"><div class="metric-lbl">MTF %</div><div class="metric-val {% if s.mtf_pct >= 85 %}ok{% elif s.mtf_pct >= 60 %}warn{% else %}danger{% endif %}">{{s.mtf_pct}}%</div></div>
        <div class="metric"><div class="metric-lbl">RSI H4</div><div class="metric-val {% if s.rsi_h4_status == 'favorable' %}ok{% elif 'extreme' in (s.rsi_h4_status or '') %}danger{% else %}warn{% endif %}">{{s.rsi_h4|round(1) if s.rsi_h4 else '—'}}</div></div>
        <div class="metric"><div class="metric-lbl">Age</div><div class="metric-val {% if not s.age_known %}warn{% elif s.age_d1 <= 15 %}ok{% elif s.age_d1 <= 30 %}warn{% else %}danger{% endif %}">{% if s.age_known %}{{s.age_d1}}j{% else %}?{% endif %}</div></div>
      </div>
      <div class="px-grid">
        <div class="px-card entry"><div class="px-lbl">Entry</div><div class="px-val" style="color:var(--royal)">{{s.entry}}</div><div class="px-sub">{{s.entry_type}}</div></div>
        <div class="px-card sl"><div class="px-lbl">Stop Loss</div><div class="px-val" style="color:var(--red)">{{s.sl}}</div><div class="px-sub">{{s.sl_atr_multiple|round(1)}}×ATR</div></div>
        <div class="px-card tp1"><div class="px-lbl">TP1 (60%)</div><div class="px-val" style="color:var(--green)">{{s.tp1}}</div><div class="px-sub">{% if s.tp1_atr_multiple %}{{s.tp1_atr_multiple}}×ATR{% else %}synth{% endif %}</div></div>
        <div class="px-card tp2"><div class="px-lbl">TP2 (40%)</div><div class="px-val" style="color:var(--blue)">{{s.tp2 if s.tp2 else '—'}}</div><div class="px-sub">{% if s.tp2_atr_multiple %}{{s.tp2_atr_multiple}}×ATR{% else %}synth{% endif %}</div></div>
        <div class="px-card rr"><div class="px-lbl">R : R</div><div class="px-val" style="color:var(--purple)">{{s.rr|round(2)}}</div><div class="px-sub">pondéré 60/40{% if s.rr_if_market %} · si marché : {{s.rr_if_market}}{% endif %}</div></div>
      </div>
      {% if s.flags %}<div class="flags-row">{% for f in s.flags %}<span class="flag {{f.severity}}">{{f.code}} · {{f.detail}}</span>{% endfor %}</div>{% endif %}
      {% if s.capped_reason %}<div class="cap-note">Plafond conviction appliqué : {{s.capped_reason}}</div>{% endif %}
      <div class="rationale"><strong>Rationale</strong>{{s.rationale}}{% if s.cal_note %} · <em>{{s.cal_note}}</em>{% endif %}</div>
      <div class="cal-row"><span class="cal-{{s.cal_status.value|lower}}">{{s.cal_status.value}}</span>{% if s.cal_note %}<span>{{s.cal_note}}</span>{% endif %}</div>
      {% if s.horizon_days %}<div class="cal-row"><span>Horizon cible ≈ {{s.horizon_days}} j{% if s.horizon_event %} · {{s.horizon_event}}{% endif %}</span></div>{% endif %}
      {% if s.invalidation %}<div class="audit-block"><strong>Contrat d'invalidation</strong>Prix : {{s.invalidation.price}}<br>Temps : {{s.invalidation.time}}<br>{% if s.invalidation.event %}Événement : {{s.invalidation.event}}<br>{% endif %}Structure : {{s.invalidation.structure}}</div>{% endif %}
      <div class="audit-block"><strong>Audit Trail</strong>{{s.sl_detail}}<br>{{s.rr_detail}}<br>absolute_mean={{ '%.4f'|format(fs.absolute_mean) }} · raw={{ '%.4f'|format(fs.absolute_mean_raw) }} · decay={{ '%.4f'|format(fs.decay_factor) }}/{{fs.decay_source}} · quantile={{ '%.4f'|format(fs.quantile) }} · missing={{fs.missing}}<br>{% for k,v in fs.details.items() %}{{v}}<br>{% endfor %}ATR={{s.atr_source}} · cluster={{s.cluster}} · htf={{s.htf_aligned}}</div>
    </div>
  </div>
  {% endfor %}
  {% else %}
  <div class="no-setup"><div class="no-setup-icon">∅</div><div class="no-setup-title">Aucun setup conforme aujourd'hui</div><div class="no-setup-sub">Event Risk : {{event_risk}} · Universe {{n_passed}}/{{n_total}}</div></div>
  {% endif %}
  </div>
</div>

<div class="section">
  <div class="sec-hdr"><div class="sec-num">2</div><div class="sec-ttl">Éliminés &amp; Surveillance</div><div class="sec-sub">{{elimines|length}} actif(s) filtré(s)</div></div>
  <div class="sec-body">
  {% set suspendus = elimines | selectattr('reject_code', 'equalto', 'CAL_BLACKOUT') | list %}
  {% set rejets = elimines | rejectattr('reject_code', 'equalto', 'CAL_BLACKOUT') | list %}
  {% if suspendus %}
  <div class="sub-lbl">SUSPENDUS — Calendrier ({{suspendus|length}})</div>
  <div class="sus-grid">
  {% for e in suspendus %}
  {% set dc = 'long' if e.direction.value == 'Bullish' else ('short' if e.direction.value == 'Bearish' else 'neutral') %}
  <div class="sus-item" data-asset-class="{{e.asset_class}}"><span class="sus-item-pair">{{e.symbol}}</span><span class="dir {{dc}}" style="font-size:9px;padding:1px 5px">{{e.direction.value}}</span><span class="sus-item-txt">{{e.reject_detail}} · RSI H4 : {{e.rsi_h4|round(2) if e.rsi_h4 else '—'}} · Age : {{e.age_d1}}j</span><span class="reject-code" style="display:none">{{e.reject_code}}</span></div>
  {% endfor %}
  </div>
  <hr class="div">
  {% endif %}
  {% if rejets %}
  <div class="sub-lbl">REJETS — Filtre / Preflight / Cluster ({{rejets|length}})</div>
  <table>
    <thead><tr><th>Paire</th><th>Dir.</th><th>Code</th><th>Détail</th><th>RSI H4</th><th>Age</th><th>Cal.</th></tr></thead>
    <tbody>
    {% for e in rejets %}
    {% set dc = 'long' if e.direction.value == 'Bullish' else ('short' if e.direction.value == 'Bearish' else 'neutral') %}
    <tr data-asset-class="{{e.asset_class}}"><td style="font-family:var(--mono);font-weight:700">{{e.symbol}}</td><td><span class="dir {{dc}}" style="font-size:9.5px;padding:1px 6px">{{e.direction.value}}</span></td><td class="reject-code">{{e.reject_code}}</td><td style="font-size:10px">{{e.reject_detail}}</td><td style="font-family:var(--mono);font-size:10px">{{e.rsi_h4|round(2) if e.rsi_h4 else '—'}}</td><td style="font-family:var(--mono);font-size:10px">{{e.age_d1}}j</td><td><span class="cal-{{e.cal_status.value|lower}}" style="font-size:9.5px;padding:1px 6px">{{e.cal_status.value}}</span></td></tr>
    {% endfor %}
    </tbody>
  </table>
  {% endif %}
  {% if not elimines %}<div style="padding:14px;color:var(--muted);font-style:italic;font-size:11px">Aucun actif éliminé ce cycle.</div>{% endif %}
  </div>
</div>

</div>
<div class="footer">CONFIDENTIEL · BLUESTAR SYSTEM · {{version}} · {{date_hdr}} · MAX {{max_setups}} SETUPS · RR ∈ [{{rr_min}}, {{rr_max}}] · Score ajusté note, quantile départage</div>
<script type="application/json" id="correlation-groups">{{ correlation_groups_json | safe }}</script>
<script type="application/json" id="calendar-coverage">{{ calendar_coverage_json | safe }}</script>
</div>
</body></html>"""


def _get_template() -> jinja2.Template:
    tdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    tfile = os.path.join(tdir, "scaffold.html.j2")
    if os.path.isfile(tfile):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(tdir),
                                 autoescape=jinja2.select_autoescape(["html", "j2"]))
        return env.get_template("scaffold.html.j2")
    # autoescape forcé ici aussi : sinon le rendu diverge selon que le
    # template externe existe ou non (injection HTML via données du feed).
    return jinja2.Environment(autoescape=True).from_string(_INLINE_TEMPLATE)


def render_report(setups: list[SetupV4], eliminated: list[Eliminated], meta: MergeMeta,
                  clock: Clock, calendar: Optional[CalendarSets],
                  themes: Optional[MarketThemes],
                  n_passed: int, cfg: V4Config = CONFIG,
                  correlation_groups: Optional[dict] = None,
                  macro_regime: MacroRegime = MacroRegime.UNKNOWN,
                  cal_time_degraded: bool = False,
                  cal_time_detail: str = "",
                  cal_feed_truncated: bool = False,
                  cal_feed_detail: str = "",
                  # Fraîcheur croisée — en heures réelles (0.0 = rien à signaler).
                  cal_stale_h: float = 0.0,
                  cal_merge_stale_h: float = 0.0,
                  cal_covered_currencies: Optional[list[str]] = None,
                  cal_source_stale: bool = False,
                  cal_source_warnings: Optional[list[str]] = None,
                  cal_source_quality: str = "VALID",
                  cal_reachable: bool = True,
                  cal_feed_end_utc: Optional[datetime] = None,
                  cal_feed_horizon_h: Optional[float] = None,
                  version: str = __version__) -> str:
    risk = "Low"
    if calendar:
        if calendar.blackout:
            risk = "High"
        elif calendar.proximity:
            risk = "Medium"
    theme_str = ", ".join(f"{k} {v}" for k, v in (themes.strong.items() if themes else []))
    # Qualité SR granulaire : au moins UN niveau (entrée Limit, TP1 ou TP2)
    # ancré sur une zone réelle suffit à lever le badge « SR indisponible ».
    sr_entry_zone = sum(1 for s in setups if s.entry_type == "Limit")
    sr_tp_zone = sum(
        1 for s in setups
        if (s.tp1_synthetic is False)
        or (s.tp2 is not None and s.tp2_synthetic is False)
    )
    sr_degraded = (sr_entry_zone == 0 and sr_tp_zone == 0) if setups else False
    date_hdr_file = clock.now_local.strftime("%Y.%m.%d")
    # Sérialisé une seule fois ici, jamais dans le template : le HTML n'est
    # qu'un support d'affichage/transport, pas une source de vérité.
    corr_json = json.dumps(correlation_groups or {}, ensure_ascii=False).replace("</", "<\\/")

    _cov = [str(c).upper() for c in (cal_covered_currencies or [])]
    _uncovered: list[str] = []
    if _cov:
        _seen: set[str] = set()
        for _sym in [s.symbol for s in setups] + [e.symbol for e in eliminated]:
            _b, _q = _split_symbol(_sym)
            for _leg in (_b, _q):
                if _leg in _DESK_CURRENCIES and _leg not in _cov:
                    _seen.add(_leg)
        _uncovered = sorted(_seen)

    # Briefing calendaire — remplace les quatre bandeaux hérités.
    cal_brief = build_calendar_briefing(
        setups, calendar, clock, cfg,
        covered=_cov,
        feed_end_utc=cal_feed_end_utc,
        merge_stale_h=cal_merge_stale_h,
        cal_stale_h=cal_stale_h,
        source_stale=cal_source_stale,
        source_warnings=cal_source_warnings,
        source_quality=cal_source_quality,
        reachable=cal_reachable,
        time_degraded=cal_time_degraded,
        time_detail=cal_time_detail,
    )
    # Le détail de troncature reste disponible en journal ; il n'encombre plus
    # la page puisque la fenêtre couverte est affichée dans l'en-tête du bloc.
    if cal_feed_truncated and cal_feed_detail:
        logger.info("couverture calendaire : %s", cal_feed_detail)

    # Export structuré de la couverture calendaire (patron correlation-groups).
    _cal_coverage = {
        "covered": _cov,
        "uncovered": _uncovered,
        "feed_end_utc": cal_feed_end_utc.isoformat() if cal_feed_end_utc else None,
        "horizon_h": (round(cal_feed_horizon_h, 2)
                      if cal_feed_horizon_h is not None else None),
        "truncated": bool(cal_feed_truncated),
    }
    cal_coverage_json = json.dumps(_cal_coverage, ensure_ascii=False).replace("</", "<\\/")

    return _get_template().render(
        date_hdr=clock.date_hdr,
        date_hdr_file=date_hdr_file,
        n_setups=len(setups),
        n_passed=n_passed,
        n_total=meta.assets_count or (n_passed + len(eliminated)),
        event_risk=risk, themes=theme_str, sr_degraded=sr_degraded,
        setups=setups, elimines=eliminated,
        max_setups=cfg.MAX_SETUPS, rr_min=cfg.RR_MIN, rr_max=cfg.RR_MAX,
        correlation_groups_json=corr_json,
        macro_regime=macro_regime.value,
        cal_brief=cal_brief,
        cal_covered=_cov,
        cal_uncovered=_uncovered,
        calendar_coverage_json=cal_coverage_json,
        version=version,
    )


def render_pdf(html: str, pdf_path: str, base_url: Optional[str] = None,
               fallback_html: Optional[str] = None) -> str:
    """PDF natif calibré via WeasyPrint (@page + @media print du template).

    Sans WeasyPrint : écrit le HTML calibré dans fallback_html si fourni, sinon
    loggue une erreur. Jamais bloquant.
    """
    if not _HAS_WEASYPRINT:
        if fallback_html is None:
            logger.error(
                "WeasyPrint indisponible — PDF non généré. "
                "Installez-le (`pip install weasyprint`) ou passez fallback_html=... "
                "pour un repli HTML explicite.")
            return pdf_path
        with open(fallback_html, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("HTML calibré écrit en repli: %s", fallback_html)
        return fallback_html
    if base_url is None:
        base_url = os.getcwd()
    # BytesIO intermédiaire : compatibilité WeasyPrint 61+ (pydyf)
    buf = io.BytesIO()
    _WeasyHTML(string=html, base_url=base_url).write_pdf(buf)
    with open(pdf_path, "wb") as f:
        f.write(buf.getvalue())
    logger.info("PDF natif calibré généré: %s", pdf_path)
    return pdf_path


# ════════════════════════════════════════════════════════════════════════════
# SECTION 18 — CLI
# ════════════════════════════════════════════════════════════════════════════
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="BLUESTAR ENGINE v10 (Hybrid V4)")
    p.add_argument("--merged", required=True, help="Path to merge.json")
    p.add_argument("--calendar-json", help="Path to pre-parsed calendar.json")
    p.add_argument("--config", help="Optional JSON config overrides")
    p.add_argument("--output", "-o", help="Output HTML path")
    p.add_argument("--pdf", help="Output PDF path (PDF natif calibré via WeasyPrint)")
    args = p.parse_args()

    cfg = CONFIG
    if args.config:
        with open(args.config, encoding="utf-8") as f:
            cfg = V4Config.from_dict(json.load(f))

    html = run_pipeline(
        merged_path=args.merged,
        calendar_json_path=args.calendar_json,
        output_path=args.output,
        pdf_path=args.pdf,
        config=cfg,
    )
    logger.info("Report generated: %d bytes%s%s", len(html),
                f" → {args.output}" if args.output else "",
                f" (PDF → {args.pdf})" if args.pdf else "")
    if not args.output and not args.pdf:
        # Bytes UTF-8 bruts : le code page console (ex. cp1252 sous Windows)
        # ne peut pas encoder le HTML accentué et print() lèverait
        # UnicodeEncodeError.
        sys.stdout.buffer.write(html.encode("utf-8"))


if __name__ == "__main__":
    main()
