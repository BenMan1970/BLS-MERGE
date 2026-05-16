import streamlit as st
import json
import re
from datetime import datetime, timezone

st.set_page_config(page_title="🔷 BLUESTAR MERGE v2.1", layout="wide")

st.title("🔷 BLUESTAR MERGE v2.1")
st.markdown(
    "*Pipeline de merge cross-scanner — BLUESTAR DIRECT v7*  \n"
    "*Calibré sur les formats réels GPS / RSI / CHoCH / S/R Bluestar.*"
)
st.divider()

# ══════════════════════════════════════════════════════════════════
# NORMALISATION SYMBOLES
# Formats observés dans les JSON réels :
#   GPS    : "CAD/JPY", "US30/USD", "SPX500/USD", "DE30/EUR", "NAS100/USD"
#   RSI    : "EUR/USD", "DE30/EUR", "US30/USD", "SPX500/USD", "NAS100/USD"
#   CHoCH  : "AUD/CAD", "GBP/USD", "US30/USD"  (déjà avec slash)
#   S/R    : "EUR/GBP", "USD/CAD", "EUR/AUD"    (déjà avec slash)
# Tous utilisent le slash → normalize_symbol homogénéise les cas underscore résiduels.
# ══════════════════════════════════════════════════════════════════
def normalize_symbol(sym: str) -> str:
    s = str(sym).upper().strip()
    s_clean = s.replace("_", "").replace("/", "").replace("-", "")
    # Paires standard 6 chars : EURUSD, CADJPY, XAUUSD, USDJPY...
    if len(s_clean) == 6:
        return f"{s_clean[:3]}/{s_clean[3:]}"
    # Cas spéciaux longueur > 6 : US30USD(7), DE30EUR(7), SPX500USD(9), NAS100USD(9)
    # → fallback : remplacer underscore par slash si présent, sinon garder tel quel
    return s.replace("_", "/")


# ══════════════════════════════════════════════════════════════════
# PARSERS — GPS
# Format réel : liste d'objets JSON
# Clés confirmées : "Paire", "MTF", "Quality", "NC", "Age D1",
#                   "ATR H4", "ATR Daily", "4H", "M", "W", "D"
# ══════════════════════════════════════════════════════════════════
def parse_mtf(mtf_str: str) -> tuple[int, str]:
    """
    Parse le champ MTF du GPS.
    Format réel observé : "Bullish (95%)", "Bearish (91%)", "Bullish (68%)"
    Multi-formats couverts en fallback : "85% Bullish", "Bullish 92%", "BULLISH(78%)"
    """
    if not mtf_str:
        return 0, "Neutral"
    s = str(mtf_str).strip()
    pct = 0
    for pattern in [
        r'\((\d+(?:\.\d+)?)%\)',           # (95%) ← format réel
        r'(\d+(?:\.\d+)?)%',               # 85%
        r'(\d+(?:\.\d+)?)\s*(?:Bullish|Bearish|Neutral)',  # 85 Bullish
    ]:
        m = re.search(pattern, s, re.I)
        if m:
            try:
                pct = int(float(m.group(1)))
                break
            except (ValueError, TypeError):
                pass
    if re.search(r'\bBullish\b', s, re.I):
        direction = "Bullish"
    elif re.search(r'\bBearish\b', s, re.I):
        direction = "Bearish"
    else:
        direction = "Neutral"
    return pct, direction


def parse_gps_entry(g: dict) -> dict:
    """
    Extrait et normalise toutes les données utiles d'un enregistrement GPS.
    Propage les biais par TF (M/W/D/4H/1H) pour enrichissement contextuel LLM.
    """
    mtf_pct, mtf_dir = parse_mtf(g.get("MTF", ""))
    try:
        age_d1 = int(g.get("Age D1", 0) or 0)
    except (ValueError, TypeError):
        age_d1 = 0
    try:
        atr_h4 = float(g.get("ATR H4", 0) or 0)
    except (ValueError, TypeError):
        atr_h4 = 0.0
    try:
        atr_daily = float(g.get("ATR Daily", 0) or 0)
    except (ValueError, TypeError):
        atr_daily = 0.0
    try:
        nc = int(g.get("NC", 0) or 0)
    except (ValueError, TypeError):
        nc = 0
    return {
        "mtf_pct":       mtf_pct,
        "mtf_direction": mtf_dir,
        "quality":       g.get("Quality"),
        "nc":            nc,        # nombre de confluences GPS (indicateur supplémentaire)
        "age_d1":        age_d1,
        "atr_h4":        atr_h4,
        "atr_daily":     atr_daily,
        # Biais par timeframe — utiles pour confirmer la direction LLM
        "bias_monthly":  g.get("M"),
        "bias_weekly":   g.get("W"),
        "bias_daily":    g.get("D"),
        "bias_h4":       g.get("4H"),   # clé pour htf_aligned
        "bias_h1":       g.get("1H"),
    }


# ══════════════════════════════════════════════════════════════════
# PARSERS — RSI
# Format réel : liste d'objets JSON
# Clés confirmées : "Devises", "Status", "RSI_H4", "DIV_H4",
#                   "RSI_Daily", "DIV_Daily", "RSI_Weekly", "DIV_Weekly",
#                   "RSI_Monthly" (peut être null), "RSI_H1", "DIV_H1"
# ══════════════════════════════════════════════════════════════════
def safe_float(val) -> float | None:
    """Conversion float sécurisée — gère null JSON (None Python)."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def rsi_status(rsi_val, direction: str) -> str:
    """
    Statut RSI contextualisé — logique mean-reversion pour CHoCH sur S/R.
    RSI bas = favorable pour Bullish (rebond attendu).
    RSI haut = favorable pour Bearish (rejet attendu).
    Seuils configurables via sidebar.
    """
    if rsi_val is None:
        return "unknown"
    r = safe_float(rsi_val)
    if r is None:
        return "unknown"
    s = st.session_state.get("rsi_thresholds", {
        "favorable_bull": 40, "neutral_low": 60, "overbought": 68, "tension": 72,
        "favorable_bear": 60, "neutral_high": 40, "oversold": 32, "tension_bear": 28,
    })
    if direction == "Bullish":
        if r < s["favorable_bull"]: return "favorable"
        if r < s["neutral_low"]:    return "neutre"
        if r < s["overbought"]:     return "overbought"
        if r < s["tension"]:        return "tension"
        return "extreme_overbought"
    else:
        if r > s["favorable_bear"]: return "favorable"
        if r > s["neutral_high"]:   return "neutre"
        if r > s["oversold"]:       return "oversold"
        if r > s["tension_bear"]:   return "tension"
        return "extreme_oversold"


def normalize_div(val: str) -> str:
    """
    Normalise les valeurs de divergence entre les deux formats RSI.
    Ancien format : "Aucune" / "Haussière" / "Baissière"
    Nouveau format : "NONE"  / "BULL"      / "BEAR"
    Sortie unifiée : "Aucune" / "Haussière" / "Baissière"
    """
    if not val:
        return "Aucune"
    v = str(val).strip().upper()
    if v in ("NONE", "AUCUNE"):    return "Aucune"
    if v in ("BULL", "HAUSSIÈRE", "HAUSSIERE"): return "Haussière"
    if v in ("BEAR", "BAISSIÈRE", "BAISSIERE"): return "Baissière"
    return val  # valeur inconnue conservée telle quelle


def parse_rsi_entry(r: dict, direction: str) -> dict:
    """
    Parse un enregistrement RSI — compatible avec les DEUX formats :

    FORMAT ANCIEN (RSI_Report_*.json liste plate) :
      {"Devises":"EUR/USD", "RSI_H4":22.74, "DIV_H4":"Aucune",
       "RSI_Daily":40.96, "DIV_Daily":"Aucune", "RSI_Weekly":48.25, ...}

    FORMAT NOUVEAU (RSI_Report_*.json avec wrapper meta/instruments) :
      {"pair":"EUR/USD", "status":"OK",
       "timeframes": {"H4":{"rsi":22.74,"div":"NONE"},
                      "D": {"rsi":40.96,"div":"NONE"},
                      "W": {"rsi":48.26,"div":"NONE"}, ...}}

    Différences clés identifiées :
      - clé identifiant : "Devises" → "pair"
      - structure RSI   : flat  →  imbriquée sous "timeframes"
      - clé Daily       : "RSI_Daily" → "D"  (pas "Daily" !)
      - clé Weekly      : "RSI_Weekly" → "W" (pas "Weekly" !)
      - valeur div      : "Aucune"/"Haussière" → "NONE"/"BULL"/"BEAR"
      - RSI_Monthly peut être null dans l'ancien format

    La détection du format est automatique via la présence de "timeframes".
    """
    if "timeframes" in r:
        # ── NOUVEAU FORMAT ──
        tfs     = r.get("timeframes", {})
        tf_h4   = tfs.get("H4", {})
        tf_d    = tfs.get("D",  {})
        tf_w    = tfs.get("W",  {})
        tf_h1   = tfs.get("H1", {})
        tf_m    = tfs.get("M",  {})
        rsi_h4  = safe_float(tf_h4.get("rsi"))
        return {
            "rsi_h1":        safe_float(tf_h1.get("rsi")),
            "rsi_h4":        rsi_h4,
            "rsi_h4_status": rsi_status(rsi_h4, direction),
            "rsi_daily":     safe_float(tf_d.get("rsi")),
            "rsi_weekly":    safe_float(tf_w.get("rsi")),
            "rsi_monthly":   safe_float(tf_m.get("rsi")),
            "div_h4":        normalize_div(tf_h4.get("div", "NONE")),
            "div_daily":     normalize_div(tf_d.get("div",  "NONE")),
            "div_weekly":    normalize_div(tf_w.get("div",  "NONE")),
        }
    else:
        # ── ANCIEN FORMAT ──
        rsi_h4 = safe_float(r.get("RSI_H4"))
        return {
            "rsi_h1":        safe_float(r.get("RSI_H1")),
            "rsi_h4":        rsi_h4,
            "rsi_h4_status": rsi_status(rsi_h4, direction),
            "rsi_daily":     safe_float(r.get("RSI_Daily")),
            "rsi_weekly":    safe_float(r.get("RSI_Weekly")),
            "rsi_monthly":   safe_float(r.get("RSI_Monthly")),
            "div_h4":        normalize_div(r.get("DIV_H4",    "Aucune")),
            "div_daily":     normalize_div(r.get("DIV_Daily", "Aucune")),
            "div_weekly":    normalize_div(r.get("DIV_Weekly","Aucune")),
        }


# ══════════════════════════════════════════════════════════════════
# PARSERS — S/R BLUESTAR
# Format réel : {"generated_at":..., "assets":[...]}
# Points critiques confirmés :
#   - "level" est une STRING → float() obligatoire
#   - "alert" peut être "" → normalisation
#   - "timeframes" séparé par " + " → split(" + ") pas split("+")
#   - "score" et "distance_pct" sont float → cast défensif
# ══════════════════════════════════════════════════════════════════
def parse_price_context(ctx: str) -> dict:
    """
    Parse price_context du scanner S/R Bluestar.
    Tags confirmés dans les données réelles :
      "SUR support: 0.86835 (-0.32%)"
      "S proche: 1.61708 (-0.62%)"
      "SUR resistance: 0.87236 (+0.14%)"
      "Zone intermediaire"
    """
    result = {
        "raw": ctx, "support_level": None, "support_dist_pct": None,
        "support_tag": None, "resistance_level": None,
        "resistance_dist_pct": None, "resistance_tag": None,
        "is_intermediate": False,
    }
    if not ctx or ctx in ("Zone intermediaire", "Prix indisponible", ""):
        result["is_intermediate"] = True
        return result
    _sup = r'(SUR\s+support|S\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'
    _res = r'(SUR\s+resistance|R\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'
    sup_m = re.search(_sup, ctx, re.I)
    res_m = re.search(_res, ctx, re.I)
    if sup_m:
        result["support_tag"]      = sup_m.group(1).strip()
        result["support_level"]    = float(sup_m.group(2))
        result["support_dist_pct"] = float(sup_m.group(3))
    if res_m:
        result["resistance_tag"]      = res_m.group(1).strip()
        result["resistance_level"]    = float(res_m.group(2))
        result["resistance_dist_pct"] = float(res_m.group(3))
    return result


def parse_zone(z: dict) -> dict:
    """
    Parse et normalise une zone du scanner S/R.
    CRITIQUE : "level" est une STRING dans le JSON Bluestar → float() obligatoire.
    """
    try:    level = float(z.get("level", 0))
    except: level = 0.0
    try:    score = float(z.get("score", 0))
    except: score = 0.0
    try:    dist  = float(z.get("distance_pct", 999))
    except: dist  = 999.0

    # "timeframes" : "Daily + H4 + Weekly" → split par " + " (espaces inclus)
    tf_raw  = str(z.get("timeframes", ""))
    tf_list = [t.strip() for t in tf_raw.split("+") if t.strip()]
    tf_nb   = len(tf_list)
    tf_weight = (3 if "Weekly" in tf_list else 0) + \
                (2 if "Daily"  in tf_list else 0) + \
                (1 if "H4"     in tf_list else 0)

    # "alert" : "" / "Proche" / "ZONE CHAUDE"
    alert_raw = str(z.get("alert", "") or "").strip().upper()
    alert = "ZONE CHAUDE" if "CHAUDE" in alert_raw else ("Proche" if "PROCHE" in alert_raw else "")

    # "signal" : "BUY ZONE" / "SELL ZONE"
    sig_raw = str(z.get("signal", "")).upper()
    signal  = "BUY ZONE" if "BUY" in sig_raw else ("SELL ZONE" if "SELL" in sig_raw else sig_raw)

    status = str(z.get("status", "Testee"))
    STATUS_COEFF = {"Vierge": 1.0, "Testee": 0.8, "Role Reverse": 0.6}

    return {
        "signal":         signal,
        "level":          round(level, 5),
        "score":          round(score, 1),
        "status":         status,
        "status_coeff":   STATUS_COEFF.get(status, 0.8),
        "weighted_score": round(score * STATUS_COEFF.get(status, 0.8), 1),
        "distance_pct":   round(dist, 3),
        "alert":          alert,
        "timeframes":     tf_raw,
        "tf_list":        tf_list,
        "tf_nb":          tf_nb,
        "tf_weight":      tf_weight,
        "tf_has_weekly":  "Weekly" in tf_list,
        "tf_has_daily":   "Daily"  in tf_list,
        "tf_has_h4":      "H4"     in tf_list,
    }


def parse_sr_file(raw) -> dict:
    """Index S/R : {symbol_normalisé → asset enrichi}."""
    assets = raw.get("assets", []) if isinstance(raw, dict) else raw
    index  = {}
    for asset in assets:
        sym   = normalize_symbol(asset.get("symbol", ""))
        ctx   = parse_price_context(asset.get("price_context", ""))
        zones = sorted(
            [parse_zone(z) for z in asset.get("zones", [])],
            key=lambda z: z["distance_pct"]
        )
        index[sym] = {
            "symbol":        sym,
            "price_context": ctx,
            "zones":         zones,
            "buy_zones":     [z for z in zones if z["signal"] == "BUY ZONE"],
            "sell_zones":    [z for z in zones if z["signal"] == "SELL ZONE"],
            "hot_zones":     [z for z in zones if z["distance_pct"] < 0.5],
            "zones_count":   len(zones),
        }
    return index


# ══════════════════════════════════════════════════════════════════
# VALIDATION INVARIANTS FINANCIERS
# ══════════════════════════════════════════════════════════════════
def validate_invariants(signal: dict) -> list[str]:
    w = []
    mtf = signal.get("gps", {}).get("mtf_pct", 0)
    if not (0 <= (mtf or 0) <= 100):
        w.append(f"mtf_pct hors [0,100]: {mtf}")
    atr = signal.get("gps", {}).get("atr_h4", 0)
    if atr is not None and isinstance(atr, (int, float)) and atr < 0:
        w.append(f"atr_h4 négatif: {atr}")
    if signal.get("direction") not in ("Bullish", "Bearish"):
        w.append(f"direction invalide: {signal.get('direction')!r}")
    rsi = signal.get("rsi", {}).get("rsi_h4")
    if rsi is not None and not (0 <= float(rsi) <= 100):
        w.append(f"rsi_h4 hors [0,100]: {rsi}")
    return w


# ══════════════════════════════════════════════════════════════════
# SIDEBAR — Seuils RSI configurables
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ Seuils RSI")
    st.caption(
        "Logique mean-reversion : RSI bas favorable pour Bullish "
        "(rebond S/R), RSI haut favorable pour Bearish."
    )
    st.session_state["rsi_thresholds"] = {
        "favorable_bull": st.slider("Bull favorable <",  20, 50, 40, key="rsi_fb"),
        "neutral_low":    st.slider("Bull neutre <",     50, 70, 60, key="rsi_nl"),
        "overbought":     st.slider("Bull overbought <", 60, 75, 68, key="rsi_ob"),
        "tension":        st.slider("Bull tension <",    65, 80, 72, key="rsi_t"),
        "favorable_bear": st.slider("Bear favorable >",  50, 80, 60, key="rsi_fbr"),
        "neutral_high":   st.slider("Bear neutre >",     30, 55, 40, key="rsi_nh"),
        "oversold":       st.slider("Bear oversold >",   25, 45, 32, key="rsi_os"),
        "tension_bear":   st.slider("Bear tension >",    20, 40, 28, key="rsi_tb"),
    }

# ══════════════════════════════════════════════════════════════════
# SECTION 1 — UPLOAD
# ══════════════════════════════════════════════════════════════════
st.subheader("📁 Upload des scanners JSON")

c1, c2 = st.columns(2)
with c1:
    gps_file   = st.file_uploader("Scanner GPS (Bluestar_GPS_*.json)",        type="json", key="gps")
    rsi_file   = st.file_uploader("Scanner RSI (RSI_Report_*.json)",          type="json", key="rsi")
with c2:
    sr_file    = st.file_uploader("Scanner S/R Bluestar (sr_bluestar_*.json)",type="json", key="sr")
    choch_file = st.file_uploader("Scanner CHoCH (choch_pipeline_*.json)",    type="json", key="choch")

st.caption(
    "ℹ️ Le S/R doit être l'**export JSON Merge** du scanner Bluestar (bouton dédié sidebar). "
    "Ce format exporte toutes les zones actives sans filtre de distance."
)

# ══════════════════════════════════════════════════════════════════
# SECTION 2 — GÉNÉRATION
# ══════════════════════════════════════════════════════════════════
st.divider()

if st.button("🔵 GÉNÉRER LE MERGED PIPELINE", type="primary", use_container_width=True):

    if not all([gps_file, rsi_file, sr_file, choch_file]):
        st.error("Les 4 fichiers JSON sont obligatoires.")
        st.stop()

    # ── LECTURE ──
    gps_raw   = json.load(gps_file)
    rsi_raw   = json.load(rsi_file)
    sr_raw    = json.load(sr_file)
    choch_raw = json.load(choch_file)

    # CHoCH : {"meta":{...}, "signals":[...]}
    choch_signals = choch_raw.get("signals", []) if isinstance(choch_raw, dict) else choch_raw

    # GPS : liste directe (pas de wrapper)
    gps_list = gps_raw if isinstance(gps_raw, list) else gps_raw.get("data", [])
    # RSI — compatible ancien format (liste plate) et nouveau format (meta + instruments)
    # Ancien : liste de {"Devises":"EUR/USD", "RSI_H4":..., "DIV_H4":...}
    # Nouveau : {"meta":{...}, "summary":[...], "instruments":[{"pair":"EUR/USD", "timeframes":{...}}]}
    if isinstance(rsi_raw, list):
        rsi_list = rsi_raw
    elif isinstance(rsi_raw, dict) and "instruments" in rsi_raw:
        rsi_list = rsi_raw["instruments"]   # nouveau format
    else:
        rsi_list = rsi_raw.get("data", [])

    # Clé d'identification : "Devises" (ancien) ou "pair" (nouveau)
    def _rsi_key(r):
        return normalize_symbol(r.get("pair") or r.get("Devises") or "")

    # ══ INDEX NORMALISÉS O(1) ══
    sr_index  = parse_sr_file(sr_raw)
    gps_index = {normalize_symbol(g.get("Paire","")): g
                 for g in gps_list if isinstance(g, dict)}
    rsi_index = {_rsi_key(r): r
                 for r in rsi_list if isinstance(r, dict)}

    # Diagnostics
    diag_no_gps, diag_no_rsi, diag_no_sr = [], [], []
    diag_warnings = {}

    # ── STRUCTURE DE SORTIE ──
    merged = {
        "meta": {
            "generated_at":    datetime.now(timezone.utc).isoformat(),
            "version":         "2.1",
            "choch_version":   choch_raw.get("meta", {}).get("scanner_version", "?")
                               if isinstance(choch_raw, dict) else "?",
            "source_scanners": ["gps", "rsi", "sr_bluestar", "choch"],
            "signals_count":   0,
            "note":            "Calendrier économique à joindre séparément dans le prompt LLM.",
        },
        "signals":      [],
        "gps_summary":  {"top_bullish": [], "top_bearish": []},
        "sr_hot_zones": [],
    }

    # ══════════════════════════════════════════════════════════════
    # ENRICHISSEMENT — 1 signal CHoCH → données de 3 scanners
    # ══════════════════════════════════════════════════════════════
    for signal in choch_signals:
        # CHoCH "pair" est déjà en format "EUR/GBP" dans les données réelles
        pair      = signal.get("pair", "")
        pair_norm = normalize_symbol(pair)
        direction = signal.get("direction", "Bullish")

        gps_match = gps_index.get(pair_norm)
        rsi_match = rsi_index.get(pair_norm)
        sr_match  = sr_index.get(pair_norm)

        if not gps_match: diag_no_gps.append(pair_norm)
        if not rsi_match: diag_no_rsi.append(pair_norm)
        if not sr_match:  diag_no_sr.append(pair_norm)

        scanners_matched  = sum([bool(gps_match), bool(rsi_match), bool(sr_match)])
        enrichment_status = (
            "complet"  if scanners_matched == 3 else
            "partiel"  if scanners_matched >= 1 else
            "minimal"
        )

        # ── GPS ──
        gps_data_enriched = {
            "mtf_pct": 0, "mtf_direction": "Neutral", "quality": None,
            "nc": 0, "age_d1": 0, "atr_h4": 0.0, "atr_daily": 0.0,
            "bias_monthly": None, "bias_weekly": None,
            "bias_daily": None, "bias_h4": None, "bias_h1": None,
        }
        if gps_match:
            gps_data_enriched = parse_gps_entry(gps_match)

        # ── RSI ──
        rsi_data_enriched = {
            "rsi_h1": None, "rsi_h4": None, "rsi_h4_status": "unknown",
            "rsi_daily": None, "rsi_weekly": None, "rsi_monthly": None,
            "div_h4": "Aucune", "div_daily": "Aucune", "div_weekly": "Aucune",
        }
        if rsi_match:
            rsi_data_enriched = parse_rsi_entry(rsi_match, direction)

        # ── S/R ──
        zone_available = False
        nearest_zone   = None
        zone_badge     = "🔧 ZONE MANUELLE"
        tp_zones       = []
        price_ctx      = {}

        if sr_match:
            zone_available = True
            price_ctx      = sr_match["price_context"]
            aligned_zones  = (
                sr_match["buy_zones"] if direction == "Bullish"
                else sr_match["sell_zones"]
            )
            if aligned_zones:
                nearest_zone = aligned_zones[0]  # triées par distance
                d = nearest_zone["distance_pct"]
                zone_badge = (
                    "⚡ ZONE CHAUDE"   if d < 0.5 else
                    "⚠️ ZONE PROCHE"   if d < 1.5 else
                    "📍 ZONE DISTANTE"
                )
            tp_source = (
                sr_match["sell_zones"] if direction == "Bullish"
                else sr_match["buy_zones"]
            )
            tp_zones = tp_source[:3]

        # ── HTF aligned ──
        # GPS "4H" vs CHoCH "direction"
        bias_h4     = gps_data_enriched.get("bias_h4")
        htf_aligned = bool(bias_h4 and bias_h4 == direction)

        # ── Order Block ──
        # CHoCH "level" et "close_price" sont déjà float dans les données réelles
        level    = signal.get("level",       0) or 0
        close_px = signal.get("close_price", 0) or 0
        try:
            ob_top    = float(max(level, close_px)) if (level and close_px) else float(level or close_px)
            ob_bottom = float(min(level, close_px)) if (level and close_px) else float(level or close_px)
        except (TypeError, ValueError):
            ob_top = ob_bottom = 0.0

        # ── Assemblage final ──
        enriched = {
            # Identité CHoCH
            "signal_id":             signal.get("signal_id"),
            "pair":                  pair_norm,
            "pair_oanda":            signal.get("pair_oanda"),
            "timeframe":             signal.get("timeframe"),  # "H4" ou "D1"
            "type":                  signal.get("type"),        # "CHoCH" ou "BOS"
            "direction":             direction,
            "order":                 signal.get("order"),       # "buy" ou "sell"
            "is_choch":              signal.get("is_choch"),
            "choch_status":          signal.get("status"),      # "Fresh" ou "Aged"
            "confluence_score":      signal.get("confluence_score"),
            "level":                 float(level) if level else None,
            "close_price":           float(close_px) if close_px else None,
            "current_price":         signal.get("current_price"),
            "distance_pct":          signal.get("distance_pct"),
            "distance_atr_multiple": signal.get("distance_atr_multiple"),
            "volatility":            signal.get("volatility"),    # "Haute"/"Moyenne"
            "force":                 signal.get("force"),         # "Fort"/"Moyen"
            "bb_regime":             signal.get("bb_regime"),     # "Normal"/"Squeeze"/"Expansion"
            "session":               signal.get("session"),
            "signal_time":           signal.get("signal_time"),
            "candles_elapsed":       signal.get("candles_elapsed"),
            # GPS enrichi
            "gps":                   gps_data_enriched,
            # RSI enrichi (H4 + Daily + Weekly)
            "rsi":                   rsi_data_enriched,
            # S/R enrichi
            "sr": {
                "price_context":           price_ctx,
                "nearest_aligned_zone":    nearest_zone,
                "tp_zones":                tp_zones,
                "zone_available":          zone_available,
                "zone_badge":              zone_badge,
                "sur_support_level":       price_ctx.get("support_level")       if price_ctx else None,
                "sur_support_dist_pct":    price_ctx.get("support_dist_pct")    if price_ctx else None,
                "sur_resistance_level":    price_ctx.get("resistance_level")    if price_ctx else None,
                "sur_resistance_dist_pct": price_ctx.get("resistance_dist_pct") if price_ctx else None,
            },
            "order_block":    {"top": ob_top, "bottom": ob_bottom},
            "htf_aligned":    htf_aligned,
            # Méta-qualité
            "enrichment_quality": {
                "status":           enrichment_status,
                "scanners_matched": scanners_matched,
                "gps_found":        bool(gps_match),
                "rsi_found":        bool(rsi_match),
                "sr_found":         bool(sr_match),
            },
            "data_warnings": [],
        }

        warnings = validate_invariants(enriched)
        enriched["data_warnings"] = warnings
        if warnings:
            diag_warnings[pair_norm] = warnings

        merged["signals"].append(enriched)

    # ── GPS SUMMARY — Top 5 Bullish / Bearish (MTF ≥ 85%) ──
    for g in gps_list:
        if not isinstance(g, dict): continue
        pct, direction = parse_mtf(g.get("MTF", ""))
        if pct >= 85 and direction in ("Bullish", "Bearish"):
            entry = {
                "pair":     normalize_symbol(g.get("Paire", "")),
                "mtf_pct":  pct,
                "quality":  g.get("Quality"),
                "nc":       int(g.get("NC", 0) or 0),
                "age_d1":   int(g.get("Age D1", 0) or 0),
                "bias_h4":  g.get("4H"),
                "atr_h4":   safe_float(g.get("ATR H4")),
            }
            key = "top_bullish" if direction == "Bullish" else "top_bearish"
            merged["gps_summary"][key].append(entry)

    for key in ("top_bullish", "top_bearish"):
        merged["gps_summary"][key].sort(key=lambda x: x["mtf_pct"], reverse=True)
        merged["gps_summary"][key] = merged["gps_summary"][key][:5]

    # ── SR HOT ZONES — toutes paires, distance ≤ 2% ──
    for sym, asset in sr_index.items():
        for z in asset["zones"]:
            if z["distance_pct"] < 2.0:
                merged["sr_hot_zones"].append({
                    "pair":           sym,
                    "signal":         z["signal"],
                    "level":          z["level"],
                    "score":          z["score"],
                    "weighted_score": z["weighted_score"],
                    "status":         z["status"],
                    "distance_pct":   z["distance_pct"],
                    "alert":          z["alert"],
                    "timeframes":     z["timeframes"],
                    "tf_nb":          z["tf_nb"],
                    "tf_has_weekly":  z["tf_has_weekly"],
                })
    merged["sr_hot_zones"].sort(key=lambda x: x["distance_pct"])
    merged["meta"]["signals_count"] = len(merged["signals"])

    # ── EXPORT ──
    merged_json = json.dumps(merged, indent=2, ensure_ascii=False)

    n_complet  = sum(1 for s in merged["signals"] if s["enrichment_quality"]["status"] == "complet")
    n_htf      = sum(1 for s in merged["signals"] if s["htf_aligned"])
    n_sr_zones = len(merged["sr_hot_zones"])

    st.success(
        f"✅ **{len(merged['signals'])}** signal(s) enrichi(s) — "
        f"**{n_complet}** complets · **{n_htf}** HTF alignés · "
        f"**{n_sr_zones}** zones S/R ≤ 2%"
    )

    dl_col, preview_col = st.columns([1, 2])
    with dl_col:
        st.download_button(
            "📥 Télécharger merged_pipeline.json",
            data=merged_json,
            file_name=f"merged_{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json",
            mime="application/json",
            use_container_width=True,
        )
    with preview_col:
        with st.expander("Prévisualiser JSON"):
            st.code(merged_json[:4000] + "\n...", language="json")

    # ── RÉSUMÉ TRADER ──
    st.divider()
    st.subheader("Résumé du merge")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Signaux CHoCH",    len(merged["signals"]))
    m2.metric("Enrichis complet", n_complet)
    m3.metric("HTF alignés",      n_htf)
    m4.metric("Zones S/R ≤ 2%",  n_sr_zones)
    m5.metric("Avertissements",   len(diag_warnings))

    # Diagnostics matching
    all_missing = set(diag_no_gps + diag_no_rsi + diag_no_sr)
    if all_missing:
        with st.expander(f"⚠️ {len(all_missing)} paire(s) sans données complètes"):
            dc1, dc2, dc3 = st.columns(3)
            with dc1:
                st.markdown("**Sans GPS :**")
                for p in sorted(set(diag_no_gps)): st.markdown(f"- `{p}`")
            with dc2:
                st.markdown("**Sans RSI :**")
                for p in sorted(set(diag_no_rsi)): st.markdown(f"- `{p}`")
            with dc3:
                st.markdown("**Sans S/R :**")
                for p in sorted(set(diag_no_sr)):  st.markdown(f"- `{p}`")
            st.caption(
                "Normal si le S/R ne couvre pas toutes les paires CHoCH. "
                "Utiliser l'export JSON Merge du scanner Bluestar (33 actifs complets)."
            )

    # Avertissements invariants
    if diag_warnings:
        with st.expander(f"🔴 Avertissements invariants ({len(diag_warnings)} paire(s))"):
            for pair, warns in diag_warnings.items():
                st.markdown(f"**`{pair}`** : " + " | ".join(warns))

    # Détail signaux enrichis
    if merged["signals"]:
        st.markdown("**Détail des signaux :**")
        for s in merged["signals"]:
            htf    = "✅" if s["htf_aligned"] else "⚠️"
            enrich = {"complet": "🟢", "partiel": "🟡", "minimal": "🔴"}.get(
                s["enrichment_quality"]["status"], "⚪")
            nz     = s["sr"]["nearest_aligned_zone"]
            zinfo  = f"Sc:{nz['score']:.0f} {nz['status']}" if nz else "—"
            rsi_s  = s["rsi"].get("rsi_h4_status", "?")
            warns  = f" ⚡{len(s['data_warnings'])}w" if s["data_warnings"] else ""
            fresh  = "🟢" if s.get("choch_status") == "Fresh" else "🟡"
            st.markdown(
                f"- {fresh} `{s['pair']}` [{s['timeframe']}] "
                f"{s['direction']} | {htf} HTF | {enrich} Enrich | "
                f"{s['sr']['zone_badge']} [{zinfo}] | "
                f"RSI H4:{s['rsi'].get('rsi_h4','?')} ({rsi_s}) | "
                f"MTF {s['gps']['mtf_pct']}% {s['gps']['quality']} | "
                f"Score {s['confluence_score']}{warns}"
            )

    # Zones S/R hot
    if merged["sr_hot_zones"]:
        st.markdown("**Zones S/R ≤ 2% :**")
        for z in merged["sr_hot_zones"]:
            badge = "🔥" if z["alert"] == "ZONE CHAUDE" else "📍"
            w_tag = " [W]" if z["tf_has_weekly"] else ""
            st.markdown(
                f"- {badge} `{z['pair']}` **{z['signal']}** "
                f"@ **{z['level']:.5f}** | "
                f"Sc:{z['score']:.0f} (↘{z['weighted_score']:.0f}) | "
                f"{z['status']} | {z['distance_pct']:.2f}% | "
                f"{z['timeframes']}{w_tag}"
            )

    # GPS Summary
    if merged["gps_summary"]["top_bullish"] or merged["gps_summary"]["top_bearish"]:
        st.markdown("**GPS — Biais forts (MTF ≥ 85%) :**")
        gb1, gb2 = st.columns(2)
        with gb1:
            st.markdown("🟢 **Top Bullish**")
            for g in merged["gps_summary"]["top_bullish"]:
                st.markdown(
                    f"- `{g['pair']}` {g['mtf_pct']}% {g['quality']} "
                    f"| 4H:{g['bias_h4']} | Age D1:{g['age_d1']}j"
                )
        with gb2:
            st.markdown("🔴 **Top Bearish**")
            for g in merged["gps_summary"]["top_bearish"]:
                st.markdown(
                    f"- `{g['pair']}` {g['mtf_pct']}% {g['quality']} "
                    f"| 4H:{g['bias_h4']} | Age D1:{g['age_d1']}j"
                )
