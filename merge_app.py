"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       BLUESTAR MERGE  —  v2.1-OPT                                          ║
║                                                                              ║
║  Base     : merge_app v2.1 (parsers calés sur les formats Bluestar réels)  ║
║  Cibles   : BLUESTAR DIRECT v7.1 · BLUESTAR LUNDI v6.1                    ║
║                                                                              ║
║  Formats JSON couverts :                                                    ║
║    GPS    : Bluestar_GPS_*.json  (V4.1.1+)                                 ║
║    RSI    : RSI_Report_*.json   (ancien flat + nouveau timeframes)         ║
║    S/R    : sr_bluestar_*.json  (format {"generated_at"…"assets":[…]})     ║
║    CHoCH  : choch_pipeline_*.json  ({"meta":{…},"signals":[…]})            ║
║                                                                              ║
║  Additions v2.1→v2.1-opt :                                                 ║
║    [O1] correlation_groups  — step 4 v7.1 (cluster devises)               ║
║    [O2] GPS summary tri Quality→NC→mtf_pct (A+ nc=6 avant A nc=2)         ║
║    [O3] meta enrichi  (gps_version, prompt_target)                         ║
║    [O4] sr_hot_zones expose tf_has_weekly + weighted_score (spec v6.1)     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone

import streamlit as st

st.set_page_config(
    page_title="🔷 BLUESTAR MERGE v2.1-OPT",
    layout="wide",
    page_icon="🔷",
)

st.markdown("""
<div style="background:linear-gradient(135deg,#1B45B4 0%,#0f2d8a 100%);
     color:white;padding:16px 24px;border-radius:10px;margin-bottom:20px;
     box-shadow:0 4px 15px rgba(0,0,0,.4)">
  <div style="font-family:monospace;font-size:10px;opacity:.6;letter-spacing:2px">
    BLUESTAR SYSTEM · Cross-Scanner Pipeline
  </div>
  <div style="font-family:monospace;font-size:22px;font-weight:700;letter-spacing:1px;margin:4px 0">
    BLUESTAR MERGE <span style="opacity:.55;font-size:14px">v2.1-OPT</span>
  </div>
  <div style="font-family:monospace;font-size:11px;opacity:.75">
    GPS v4.1.1 · RSI · S/R · CHoCH → merged_pipeline.json
    · BLUESTAR DIRECT v7.1 · BLUESTAR LUNDI v6.1
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION SYMBOLES
# Formats observés dans les JSON réels :
#   GPS    : "CAD/JPY", "US30/USD", "SPX500/USD", "DE30/EUR", "NAS100/USD"
#   RSI    : "EUR/USD", "DE30/EUR", "US30/USD"
#   CHoCH  : "AUD/CAD", "GBP/USD"   (slash présent)
#   S/R    : "EUR/GBP", "USD/CAD"   (slash présent)
# ══════════════════════════════════════════════════════════════════════════════
def normalize_symbol(sym: str) -> str:
    s = str(sym).upper().strip()
    s_clean = s.replace("_", "").replace("/", "").replace("-", "")
    if len(s_clean) == 6:                  # EURUSD → EUR/USD
        return f"{s_clean[:3]}/{s_clean[3:]}"
    return s.replace("_", "/")             # US30/USD, NAS100/USD, etc.


# ══════════════════════════════════════════════════════════════════════════════
# PARSERS — GPS (V4.1.1+)
# Clés confirmées : "Paire","MTF","Quality","NC","Age D1",
#                   "ATR H4","ATR Daily","M","W","D","4H","1H","15m"
# ══════════════════════════════════════════════════════════════════════════════
def parse_mtf(mtf_str: str) -> tuple[int, str]:
    """
    Format réel : "Bullish (95%)" / "Bearish (91%)" / "Range"
    Fallbacks    : "85% Bullish" / "BULLISH(78%)" / "Bullish 92%"
    """
    if not mtf_str:
        return 0, "Neutral"
    s   = str(mtf_str).strip()
    pct = 0
    for pattern in [
        r'\((\d+(?:\.\d+)?)%\)',
        r'(\d+(?:\.\d+)?)%',
    ]:
        m = re.search(pattern, s, re.I)
        if m:
            try:
                pct = int(float(m.group(1)))
                break
            except (ValueError, TypeError):
                pass
    direction = (
        "Bullish" if re.search(r'\bBullish\b', s, re.I) else
        "Bearish" if re.search(r'\bBearish\b', s, re.I) else
        "Neutral"
    )
    return pct, direction


def safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def parse_gps_entry(g: dict) -> dict:
    """
    Extrait et normalise toutes les données d'un enregistrement GPS V4.1.1.
    Exporte les biais sur tous les TF pour les étapes 5 (Scénario) et 7
    (Conviction) du prompt v7.1.
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
        "nc":            nc,
        "age_d1":        age_d1,
        "atr_h4":        atr_h4,
        "atr_daily":     atr_daily,
        "bias_monthly":  g.get("M"),
        "bias_weekly":   g.get("W"),
        "bias_daily":    g.get("D"),
        "bias_h4":       g.get("4H"),
        "bias_h1":       g.get("1H"),
        "bias_15m":      g.get("15m"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# PARSERS — RSI (double format)
# Ancien : liste plate  {"Devises":"EUR/USD","RSI_H4":22.74,"DIV_H4":"Aucune"…}
# Nouveau : nested      {"pair":"EUR/USD","timeframes":{"H4":{"rsi":22.74,"div":"NONE"}…}}
# ══════════════════════════════════════════════════════════════════════════════
def rsi_status(rsi_val, direction: str) -> str:
    """
    Statut RSI contextualisé — mean-reversion sur zone S/R.
    Seuils configurables depuis la sidebar (session_state).
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
        if r < s["favorable_bull"]:
            return "favorable"
        if r < s["neutral_low"]:
            return "neutre"
        if r < s["overbought"]:
            return "overbought"
        if r < s["tension"]:
            return "tension"
        return "extreme_overbought"
    if r > s["favorable_bear"]:
        return "favorable"
    if r > s["neutral_high"]:
        return "neutre"
    if r > s["oversold"]:
        return "oversold"
    if r > s["tension_bear"]:
        return "tension"
    return "extreme_oversold"


def normalize_div(val: str) -> str:
    """
    Unifie les deux formats de divergence RSI.
    Ancien  : "Aucune" / "Haussière" / "Baissière"
    Nouveau : "NONE"   / "BULL"      / "BEAR"
    Sortie  : "Aucune" / "Haussière" / "Baissière"
    """
    if not val: return "Aucune"
    v = str(val).strip().upper()
    if v in ("NONE", "AUCUNE"):                          return "Aucune"
    if v in ("BULL", "HAUSSIÈRE", "HAUSSIERE"):          return "Haussière"
    if v in ("BEAR", "BAISSIÈRE", "BAISSIERE"):          return "Baissière"
    return val


def parse_rsi_entry(r: dict, direction: str) -> dict:
    """
    Compatible ancien format (flat) ET nouveau format (nested timeframes).
    La détection est automatique via la présence de la clé "timeframes".
    Expose rsi_h1/h4/daily/weekly/monthly + div_h4/daily/weekly.
    """
    if "timeframes" in r:
        tfs   = r.get("timeframes", {})
        tf_h1 = tfs.get("H1", {}); tf_h4 = tfs.get("H4", {})
        tf_d  = tfs.get("D",  {}); tf_w  = tfs.get("W",  {})
        tf_m  = tfs.get("M",  {})
        rsi_h4 = safe_float(tf_h4.get("rsi"))
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


# ══════════════════════════════════════════════════════════════════════════════
# PARSERS — S/R BLUESTAR
# Format réel : {"generated_at":…, "assets":[…]}
# Points critiques :
#   - "level"        = STRING → float() obligatoire
#   - "timeframes"   = "Daily + H4 + Weekly" → split(" + ") avec espaces
#   - "alert"        peut être ""
#   - "distance_pct" et "score" = float
# ══════════════════════════════════════════════════════════════════════════════
def parse_price_context(ctx: str) -> dict:
    """
    Parse le champ price_context du scanner S/R Bluestar.
    Tags réels confirmés :
      "SUR support: 0.86835 (-0.32%)"
      "S proche: 1.61708 (-0.62%)"
      "SUR resistance: 0.87236 (+0.14%)"
      "Zone intermediaire"
    """
    result = {
        "raw": ctx,
        "support_level":       None, "support_dist_pct":    None, "support_tag":    None,
        "resistance_level":    None, "resistance_dist_pct": None, "resistance_tag": None,
        "is_intermediate":     False,
    }
    if not ctx or ctx.strip() in ("Zone intermediaire", "Prix indisponible", ""):
        result["is_intermediate"] = True
        return result
    _sup = r'(SUR\s+support|S\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'
    _res = r'(SUR\s+resistance|R\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'
    sm = re.search(_sup, ctx, re.I)
    rm = re.search(_res, ctx, re.I)
    if sm:
        result["support_tag"]      = sm.group(1).strip()
        result["support_level"]    = float(sm.group(2))
        result["support_dist_pct"] = float(sm.group(3))
    if rm:
        result["resistance_tag"]      = rm.group(1).strip()
        result["resistance_level"]    = float(rm.group(2))
        result["resistance_dist_pct"] = float(rm.group(3))
    return result


def parse_zone(z: dict) -> dict:
    """
    Parse et normalise une zone S/R.
    CRITIQUE : "level" est une STRING dans le JSON Bluestar → cast float obligatoire.
    Calcule weighted_score, tf_weight, tf_has_weekly pour les filtres v6.1.
    """
    try:    level = float(z.get("level", 0))
    except: level = 0.0
    try:    score = float(z.get("score", 0))
    except: score = 0.0
    try:    dist  = float(z.get("distance_pct", 999))
    except: dist  = 999.0

    tf_raw  = str(z.get("timeframes", ""))
    tf_list = [t.strip() for t in tf_raw.split("+") if t.strip()]
    tf_nb   = len(tf_list)
    tf_weight = (
        (3 if "Weekly" in tf_list else 0) +
        (2 if "Daily"  in tf_list else 0) +
        (1 if "H4"     in tf_list else 0)
    )

    alert_raw = str(z.get("alert", "") or "").strip().upper()
    alert = (
        "ZONE CHAUDE" if "CHAUDE" in alert_raw else
        "Proche"      if "PROCHE" in alert_raw else ""
    )

    sig_raw = str(z.get("signal", "")).upper()
    signal  = "BUY ZONE" if "BUY" in sig_raw else ("SELL ZONE" if "SELL" in sig_raw else sig_raw)

    status = str(z.get("status", "Testee"))
    STATUS_COEFF = {"Vierge": 1.0, "Testee": 0.8, "Role Reverse": 0.6}

    return {
        "signal":        signal,
        "level":         round(level, 5),
        "score":         round(score, 1),
        "status":        status,
        "status_coeff":  STATUS_COEFF.get(status, 0.8),
        "weighted_score":round(score * STATUS_COEFF.get(status, 0.8), 1),
        "distance_pct":  round(dist, 3),
        "alert":         alert,
        "timeframes":    tf_raw,
        "tf_list":       tf_list,
        "tf_nb":         tf_nb,
        "tf_weight":     tf_weight,
        "tf_has_weekly": "Weekly" in tf_list,
        "tf_has_daily":  "Daily"  in tf_list,
        "tf_has_h4":     "H4"     in tf_list,
    }


def parse_sr_file(raw) -> dict:
    """
    Construit l'index S/R normalisé : {symbol → asset enrichi}.
    Pré-trie les zones par distance (proches en premier).
    Sépare buy_zones, sell_zones, hot_zones pour un lookup direct.
    """
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


# ══════════════════════════════════════════════════════════════════════════════
# [O1] CORRELATION GROUPS — step 4 v7.1
# Regroupe les signaux par devise commune pour aider le LLM à identifier
# les clusters et choisir le meilleur véhicule.
# ══════════════════════════════════════════════════════════════════════════════
def build_correlation_groups(signals: list) -> dict:
    """
    Clé = devise (USD, EUR, JPY…).
    Valeur = liste de signaux actifs sur cette devise, triés par qualité.
    Seules les devises avec ≥ 2 signaux sont incluses (cluster potentiel).
    """
    ccy_map = defaultdict(list)
    for s in signals:
        for ccy in s["pair"].split("/"):
            ccy_map[ccy].append({
                "pair":             s["pair"],
                "direction":        s["direction"],
                "mtf_pct":          s["gps"]["mtf_pct"],
                "quality":          s["gps"]["quality"],
                "nc":               s["gps"]["nc"],
                "confluence_score": s.get("confluence_score"),
                "choch_status":     s.get("choch_status"),
            })
    # Tri : qualité (A+ > A > B+ > B) → nc → mtf_pct
    def _q(q): return {"A+": 4, "A": 3, "B+": 2, "B": 1}.get(str(q), 0)
    return {
        k: sorted(v, key=lambda x: (_q(x["quality"]), x["nc"], x["mtf_pct"]), reverse=True)
        for k, v in sorted(ccy_map.items())
        if len(v) >= 2
    }


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION INVARIANTS FINANCIERS
# ══════════════════════════════════════════════════════════════════════════════
def validate_invariants(signal: dict) -> list[str]:
    warns: list[str] = []
    mtf = signal.get("gps", {}).get("mtf_pct", 0)
    if not 0 <= (mtf or 0) <= 100:
        warns.append(f"mtf_pct hors [0,100]: {mtf}")
    atr = signal.get("gps", {}).get("atr_h4", 0)
    if atr is not None and isinstance(atr, (int, float)) and atr < 0:
        warns.append(f"atr_h4 négatif: {atr}")
    if signal.get("direction") not in ("Bullish", "Bearish"):
        warns.append(f"direction invalide: {signal.get('direction')!r}")
    rsi = signal.get("rsi", {}).get("rsi_h4")
    if rsi is not None:
        try:
            if not 0 <= float(rsi) <= 100:
                warns.append(f"rsi_h4 hors [0,100]: {rsi}")
        except (TypeError, ValueError):
            warns.append(f"rsi_h4 non numérique: {rsi!r}")
    return warns


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — Seuils RSI configurables
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ Seuils RSI")
    st.caption(
        "Logique mean-reversion : RSI bas = favorable pour Bullish "
        "(rebond S/R) · RSI haut = favorable pour Bearish."
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
    st.divider()
    st.caption("Bluestar Merge v2.1-OPT")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — UPLOAD
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("📁 Upload des 4 scanners JSON")

c1, c2 = st.columns(2)
with c1:
    gps_file   = st.file_uploader("📡 GPS Bluestar (Bluestar_GPS_*.json)",    type="json", key="gps")
    rsi_file   = st.file_uploader("📈 RSI Bluestar (RSI_Report_*.json)",      type="json", key="rsi")
with c2:
    sr_file    = st.file_uploader("🗺️ S/R Bluestar (sr_bluestar_*.json)",     type="json", key="sr")
    choch_file = st.file_uploader("⚡ CHoCH Bluestar (choch_pipeline_*.json)", type="json", key="choch")

st.info(
    "Utiliser l'**export JSON Merge** du scanner S/R pour avoir les 33 actifs complets."
)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — GÉNÉRATION
# ══════════════════════════════════════════════════════════════════════════════
st.divider()

if st.button("🔵  GÉNÉRER LE MERGED PIPELINE", type="primary", use_container_width=True):

    if not all([gps_file, rsi_file, sr_file, choch_file]):
        st.error("❌ Les 4 fichiers JSON sont obligatoires.")
        st.stop()

    # Assert pour mypy : st.stop() lève une exception Streamlit mais
    # mypy ne le sait pas — on garantit explicitement que les fichiers ne sont pas None.
    assert gps_file is not None and rsi_file is not None
    assert sr_file  is not None and choch_file is not None

    # ── LECTURE ──────────────────────────────────────────────────────────────
    try:
        gps_file.seek(0)
        gps_raw   = json.load(gps_file)
        rsi_file.seek(0)
        rsi_raw   = json.load(rsi_file)
        sr_file.seek(0)
        sr_raw    = json.load(sr_file)
        choch_file.seek(0)
        choch_raw = json.load(choch_file)
    except json.JSONDecodeError as e:
        st.error(f"❌ JSON invalide : {e}")
        st.stop()

    # ── NORMALISATION DES LISTES D'ENTRÉE ────────────────────────────────────
    # CHoCH — {"meta":{}, "signals":[…]}
    choch_signals = choch_raw.get("signals", []) if isinstance(choch_raw, dict) else choch_raw

    # GPS — liste directe ou wrapée
    gps_list = gps_raw if isinstance(gps_raw, list) else gps_raw.get("data", [])

    # RSI — ancien format (liste) ou nouveau format (meta + instruments)
    if isinstance(rsi_raw, list):
        rsi_list = rsi_raw
    elif isinstance(rsi_raw, dict) and "instruments" in rsi_raw:
        rsi_list = rsi_raw["instruments"]
    else:
        rsi_list = rsi_raw.get("data", [])

    # ── INDEX NORMALISÉS O(1) ─────────────────────────────────────────────────
    sr_index  = parse_sr_file(sr_raw)
    gps_index = {
        normalize_symbol(g.get("Paire", "")): g
        for g in gps_list if isinstance(g, dict)
    }
    rsi_index = {
        normalize_symbol(r.get("pair") or r.get("Devises") or ""): r
        for r in rsi_list if isinstance(r, dict)
    }

    # Diagnostics matching
    diag_no_gps, diag_no_rsi, diag_no_sr = [], [], []
    diag_warnings = {}

    # ── STRUCTURE DE SORTIE ───────────────────────────────────────────────────
    merged = {
        "meta": {
            "generated_at":   datetime.now(timezone.utc).isoformat(),
            "version":        "2.1-opt",
            "gps_version":    "4.1.1",
            "prompt_target":  "BLUESTAR DIRECT v7.1 / BLUESTAR LUNDI v6.1",
            "choch_version":  (choch_raw.get("meta", {}).get("scanner_version", "?")
                               if isinstance(choch_raw, dict) else "?"),
            "source_scanners":["gps", "rsi", "sr_bluestar", "choch"],
            "signals_count":  0,
        },
        "signals":             [],
        "gps_summary":         {"top_bullish": [], "top_bearish": []},
        "sr_hot_zones":        [],
        "correlation_groups":  {},  # [O1]
    }

    # ══════════════════════════════════════════════════════════════════════════
    # ENRICHISSEMENT — 1 signal CHoCH → données de 3 scanners
    # ══════════════════════════════════════════════════════════════════════════
    for signal in choch_signals:
        pair      = normalize_symbol(signal.get("pair", ""))
        direction = signal.get("direction", "Bullish")

        gps_match = gps_index.get(pair)
        rsi_match = rsi_index.get(pair)
        sr_match  = sr_index.get(pair)

        if not gps_match: diag_no_gps.append(pair)
        if not rsi_match: diag_no_rsi.append(pair)
        if not sr_match:  diag_no_sr.append(pair)

        n_matched = sum([bool(gps_match), bool(rsi_match), bool(sr_match)])
        enrich_status = "complet" if n_matched == 3 else ("partiel" if n_matched >= 1 else "minimal")

        # ── GPS ──────────────────────────────────────────────────────────────
        gps_ctx = {
            "mtf_pct": 0, "mtf_direction": "Neutral", "quality": None,
            "nc": 0, "age_d1": 0, "atr_h4": 0.0, "atr_daily": 0.0,
            "bias_monthly": None, "bias_weekly": None, "bias_daily": None,
            "bias_h4": None, "bias_h1": None, "bias_15m": None,
        }
        if gps_match:
            gps_ctx = parse_gps_entry(gps_match)

        # ── RSI ──────────────────────────────────────────────────────────────
        rsi_ctx = {
            "rsi_h1": None, "rsi_h4": None, "rsi_h4_status": "unknown",
            "rsi_daily": None, "rsi_weekly": None, "rsi_monthly": None,
            "div_h4": "Aucune", "div_daily": "Aucune", "div_weekly": "Aucune",
        }
        if rsi_match:
            rsi_ctx = parse_rsi_entry(rsi_match, direction)

        # ── S/R ──────────────────────────────────────────────────────────────
        zone_available = False
        nearest_zone   = None
        zone_badge     = "🔧 ZONE MANUELLE"
        tp_zones       = []
        price_ctx      = {}

        if sr_match:
            zone_available = True
            price_ctx      = sr_match["price_context"]
            # Zone d'entrée (dans la direction du signal)
            aligned = sr_match["buy_zones"] if direction == "Bullish" else sr_match["sell_zones"]
            if aligned:
                nearest_zone = aligned[0]
                d = nearest_zone["distance_pct"]
                zone_badge = (
                    "⚡ ZONE CHAUDE"   if d < 0.5 else
                    "⚠️ ZONE PROCHE"   if d < 1.5 else
                    "📍 ZONE DISTANTE"
                )
            # Zones TP (direction opposée — TP potentiels)
            tp_src   = sr_match["sell_zones"] if direction == "Bullish" else sr_match["buy_zones"]
            tp_zones = tp_src[:3]

        # ── HTF ALIGNED (spec v7.1 : bias_h4 == direction) ───────────────────
        bias_h4     = gps_ctx.get("bias_h4")
        htf_aligned = bool(bias_h4 and bias_h4 == direction)

        # ── ORDER BLOCK (proxy depuis CHoCH level / close_price) ─────────────
        level    = signal.get("level",       0) or 0
        close_px = signal.get("close_price", 0) or 0
        try:
            ob_top    = float(max(level, close_px)) if (level and close_px) else float(level or close_px)
            ob_bottom = float(min(level, close_px)) if (level and close_px) else float(level or close_px)
        except (TypeError, ValueError):
            ob_top = ob_bottom = 0.0

        # ── ASSEMBLAGE ───────────────────────────────────────────────────────
        enriched = {
            # Identité CHoCH
            "signal_id":             signal.get("signal_id"),
            "pair":                  pair,
            "pair_oanda":            signal.get("pair_oanda"),
            "timeframe":             signal.get("timeframe"),
            "type":                  signal.get("type"),
            "direction":             direction,
            "order":                 signal.get("order"),
            "is_choch":              signal.get("is_choch"),
            "choch_status":          signal.get("status"),          # "Fresh" / "Aged"
            "confluence_score":      signal.get("confluence_score"),
            # Prix
            "level":                 float(level)    if level    else None,
            "close_price":           float(close_px) if close_px else None,
            "current_price":         signal.get("current_price"),
            "distance_pct":          signal.get("distance_pct"),
            "distance_atr_multiple": signal.get("distance_atr_multiple"),
            "volatility":            signal.get("volatility"),
            "force":                 signal.get("force"),
            "bb_regime":             signal.get("bb_regime"),
            "session":               signal.get("session"),
            "signal_time":           signal.get("signal_time"),
            "candles_elapsed":       signal.get("candles_elapsed"),
            # GPS enrichi (V4.1.1 — tous les biais TF)
            "gps":   gps_ctx,
            # RSI enrichi (H1 + H4 + Daily + Weekly — dual format)
            "rsi":   rsi_ctx,
            # S/R enrichi
            "sr": {
                "price_context":          price_ctx,
                "nearest_aligned_zone":   nearest_zone,
                "tp_zones":               tp_zones,
                "zone_available":         zone_available,
                "zone_badge":             zone_badge,
                # Doublons explicites pour accès direct dans les formules SL/TP v7.1
                # ⚠️ Nommage confirmé par v7.1 : sur_support_level (pas sur_support_h4)
                "sur_support_level":      price_ctx.get("support_level")       if price_ctx else None,
                "sur_support_dist_pct":   price_ctx.get("support_dist_pct")    if price_ctx else None,
                "sur_resistance_level":   price_ctx.get("resistance_level")    if price_ctx else None,
                "sur_resistance_dist_pct":price_ctx.get("resistance_dist_pct") if price_ctx else None,
            },
            "order_block": {"top": ob_top, "bottom": ob_bottom},
            # Flags
            "htf_aligned": htf_aligned,
            # Qualité d'enrichissement
            "enrichment_quality": {
                "status":           enrich_status,
                "scanners_matched": n_matched,
                "gps_found":        bool(gps_match),
                "rsi_found":        bool(rsi_match),
                "sr_found":         bool(sr_match),
            },
            "data_warnings": [],
        }

        warns = validate_invariants(enriched)
        enriched["data_warnings"] = warns
        if warns:
            diag_warnings[pair] = warns

        merged["signals"].append(enriched)

    # ── GPS SUMMARY — [O3] Tri Quality→NC→mtf_pct (A+ nc=6 avant A nc=2) ──
    for g in gps_list:
        if not isinstance(g, dict): continue
        pct, dir_g = parse_mtf(g.get("MTF", ""))
        qual       = g.get("Quality", "B")
        nc_g       = int(g.get("NC", 0) or 0)
        if pct >= 85 and dir_g in ("Bullish", "Bearish"):
            entry = {
                "pair":    normalize_symbol(g.get("Paire", "")),
                "mtf_pct": pct,
                "quality": qual,
                "nc":      nc_g,
                "age_d1":  int(g.get("Age D1", 0) or 0),
                "bias_h4": g.get("4H"),
                "atr_h4":  safe_float(g.get("ATR H4")),
            }
            bucket = "top_bullish" if dir_g == "Bullish" else "top_bearish"
            merged["gps_summary"][bucket].append(entry)

    # [O3] Tri : A+ avant A, nc décroissant, puis score
    def _gps_sort(e):
        q = {"A+": 3, "A": 2, "B+": 1, "B": 0}.get(str(e["quality"]), 0)
        return (q, e["nc"], e["mtf_pct"])

    for bucket in ("top_bullish", "top_bearish"):
        merged["gps_summary"][bucket].sort(key=_gps_sort, reverse=True)
        merged["gps_summary"][bucket] = merged["gps_summary"][bucket][:5]

    # ── SR HOT ZONES ≤ 2% — [O5] expose tf_has_weekly + weighted_score ──────
    for sym, asset in sr_index.items():
        for z in asset["zones"]:
            if z["distance_pct"] < 2.0:
                merged["sr_hot_zones"].append({
                    "pair":          sym,
                    "signal":        z["signal"],
                    "level":         z["level"],
                    "score":         z["score"],
                    "weighted_score":z["weighted_score"],    # [O5]
                    "status":        z["status"],
                    "distance_pct":  z["distance_pct"],
                    "alert":         z["alert"],
                    "timeframes":    z["timeframes"],
                    "tf_nb":         z["tf_nb"],
                    "tf_has_weekly": z["tf_has_weekly"],     # [O5]
                })
    merged["sr_hot_zones"].sort(key=lambda x: x["distance_pct"])

    # ── CORRELATION GROUPS [O1] ───────────────────────────────────────────────
    merged["correlation_groups"] = build_correlation_groups(merged["signals"])

    merged["meta"]["signals_count"] = len(merged["signals"])

    # ── EXPORT ───────────────────────────────────────────────────────────────
    merged_json = json.dumps(merged, indent=2, ensure_ascii=False)

    n_complet = sum(1 for s in merged["signals"] if s["enrichment_quality"]["status"] == "complet")
    n_htf     = sum(1 for s in merged["signals"] if s["htf_aligned"])
    n_hot     = len(merged["sr_hot_zones"])
    n_corr    = len(merged["correlation_groups"])

    st.success(
        f"✅ **{len(merged['signals'])}** signal(s) — "
        f"**{n_complet}** complets · **{n_htf}** HTF alignés · "
        f"**{n_hot}** zones S/R ≤ 2% · **{n_corr}** devises en cluster"
    )

    dl_col, prev_col = st.columns([1, 2])
    with dl_col:
        st.download_button(
            "📥  Télécharger merged_pipeline.json",
            data=merged_json,
            file_name=f"merged_{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json",
            mime="application/json",
            use_container_width=True,
            type="primary",
        )
    with prev_col:
        with st.expander("Prévisualiser JSON"):
            st.code(merged_json[:4000] + "\n...", language="json")

    # ── RÉSUMÉ TRADER ─────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📊 Résumé du merge")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Signaux CHoCH",    len(merged["signals"]))
    m2.metric("Enrichis complet", n_complet)
    m3.metric("HTF alignés",      n_htf)
    m4.metric("Zones S/R ≤ 2%",  n_hot)
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
            st.caption("Normal si S/R ne couvre pas toutes les paires CHoCH.")

    if diag_warnings:
        with st.expander(f"🔴 Invariants violés ({len(diag_warnings)} paire(s))"):
            for pair_w, inv_warns in diag_warnings.items():
                st.markdown(f"**`{pair_w}`** : " + " · ".join(inv_warns))

    # Détail signaux
    if merged["signals"]:
        st.markdown("**Détail des signaux :**")
        for s in merged["signals"]:
            htf      = "✅" if s["htf_aligned"] else "⚠️"
            enr      = {"complet": "🟢", "partiel": "🟡", "minimal": "🔴"}.get(
                       s["enrichment_quality"]["status"], "⚪")
            fresh    = "🟢" if s.get("choch_status") == "Fresh" else "🟡"
            nz       = s["sr"]["nearest_aligned_zone"]
            zone_i   = f"Sc:{nz['score']:.0f} {nz['status']} {nz['alert']}" if nz else "—"
            nc_v     = int(s["gps"]["nc"] or 0)
            w_badge  = f" ⚡{len(s['data_warnings'])}w" if s["data_warnings"] else ""
            quality  = s["gps"]["quality"] or "?"
            st.markdown(
                f"- {fresh} `{s['pair']}` [{s['timeframe']}] **{s['direction']}** | "
                f"{htf} H4 | {enr} | {s['sr']['zone_badge']} [{zone_i}] | "
                f"RSI {s['rsi'].get('rsi_h4', '?')} ({s['rsi']['rsi_h4_status']}) | "
                f"GPS {quality} nc={nc_v:+d} {s['gps']['mtf_pct']}% | "
                f"Score {s.get('confluence_score', '?')}{w_badge}"
            )

    # Correlation groups [O1]
    if merged["correlation_groups"]:
        with st.expander(f"🔗 Clusters devises ({n_corr} devises avec ≥ 2 signaux)"):
            for ccy, entries in merged["correlation_groups"].items():
                dirs = set(e["direction"] for e in entries)
                flag = "✅ concordants" if len(dirs) == 1 else "⚠️ directions mixtes"
                pairs_str = " · ".join(
                    f"`{e['pair']}` {e['direction']} {e['quality'] or '?'}"
                    for e in entries
                )
                st.markdown(f"**{ccy}** · {len(entries)} signaux · {flag} : {pairs_str}")

    # Zones S/R hot [O5]
    if merged["sr_hot_zones"]:
        st.markdown("**Zones S/R ≤ 2% :**")
        for z in merged["sr_hot_zones"]:
            badge = "🔥" if z["alert"] == "ZONE CHAUDE" else "📍"
            wtag  = " **[W]**" if z["tf_has_weekly"] else ""
            st.markdown(
                f"- {badge} `{z['pair']}` **{z['signal']}** "
                f"@ **{z['level']:.5f}** | "
                f"Sc {z['score']:.0f} (↘{z['weighted_score']:.0f}) | "
                f"{z['status']} | {z['distance_pct']:.2f}% | "
                f"{z['timeframes']}{wtag}"
            )

    # GPS Summary
    if merged["gps_summary"]["top_bullish"] or merged["gps_summary"]["top_bearish"]:
        st.markdown("**GPS — Biais MTF ≥ 85% (Quality → NC → score) :**")
        gb1, gb2 = st.columns(2)
        with gb1:
            st.markdown("🟢 **Top Bullish**")
            for g in merged["gps_summary"]["top_bullish"]:
                st.markdown(
                    f"- `{g['pair']}` {g['mtf_pct']}% "
                    f"**{g['quality'] or '?'}** nc={int(g['nc'] or 0):+d} "
                    f"| 4H:{g['bias_h4']} | Age:{g['age_d1']}j"
                )
        with gb2:
            st.markdown("🔴 **Top Bearish**")
            for g in merged["gps_summary"]["top_bearish"]:
                st.markdown(
                    f"- `{g['pair']}` {g['mtf_pct']}% "
                    f"**{g['quality'] or '?'}** nc={int(g['nc'] or 0):+d} "
                    f"| 4H:{g['bias_h4']} | Age:{g['age_d1']}j"
                )
