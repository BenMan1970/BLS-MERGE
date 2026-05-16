import streamlit as st
import json
import re
from datetime import datetime, timezone

st.set_page_config(page_title="🔷 BLUESTAR MERGE v2.0", layout="wide")

st.title("🔷 BLUESTAR MERGE v2.0")
st.markdown(
    "*Pipeline de merge cross-scanner — BLUESTAR DIRECT v7*  \n"
    "*Calendrier économique : à joindre directement dans votre prompt LLM.*"
)
st.divider()

# ══════════════════════════════════════════════════════════════════
# HELPERS — FORMAT & PARSING
# ══════════════════════════════════════════════════════════════════

def normalize_symbol(sym: str) -> str:
    """
    Normalisation canonique : EUR_USD / EURUSD / eur/usd → EUR/USD.
    Couvre tous les formats observés dans les scanners GPS, RSI, CHoCH.
    """
    s = str(sym).upper().strip()
    # Retirer les séparateurs existants pour obtenir 6 caractères bruts
    s_clean = s.replace("_", "").replace("/", "").replace("-", "")
    if len(s_clean) == 6:
        return f"{s_clean[:3]}/{s_clean[3:]}"
    # Fallback : remplacer underscore par slash
    return str(sym).replace("_", "/").upper().strip()


def parse_price_context(ctx: str) -> dict:
    """
    Parsing complet du champ price_context du scanner Bluestar S/R.

    Formats couverts (observés sur les données réelles) :
      'SUR support: 0.86835 (-0.32%)  |  SUR resistance: 0.87236 (+0.14%)'
      'S proche: 1.61708 (-0.62%)  |  SUR resistance: 1.63109 (+0.24%)'
      'SUR resistance: 1.83956 (+0.13%)  |  SUR support: 1.83306 (-0.23%)'
      'Zone intermediaire'
      'Prix indisponible'

    Retourne un dict structuré avec niveaux ET distances (exploitables pour R:R).
    Le champ 'raw' conserve la chaîne originale pour audit.
    """
    result = {
        "raw":              ctx,
        "support_level":    None,
        "support_dist_pct": None,   # distance négative = sous le prix
        "support_tag":      None,   # "SUR support" ou "S proche"
        "resistance_level": None,
        "resistance_dist_pct": None,  # distance positive = au-dessus du prix
        "resistance_tag":   None,
        "is_intermediate":  False,
    }

    if not ctx or ctx in ("Zone intermediaire", "Prix indisponible", ""):
        result["is_intermediate"] = True
        return result

    # Pattern : TAG: niveau (±X.XX%)
    # Couvre "SUR support", "S proche", "SUR resistance", "R proche"
    _sup_pat = r'(SUR\s+support|S\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'
    _res_pat = r'(SUR\s+resistance|R\s+proche)[:\s]+([\d.]+)\s*\(([-+][\d.]+)%\)'

    sup_m = re.search(_sup_pat, ctx, re.I)
    res_m = re.search(_res_pat, ctx, re.I)

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
    Normalisation d'une zone du scanner S/R.

    Points critiques observés sur les données réelles :
    - 'level' est une STRING dans le JSON (ex: "0.86149") → cast float obligatoire
    - 'alert' peut être '' (chaîne vide) → normalisé
    - 'timeframes' séparé par ' + ' (avec espaces) → split correct
    - 'score' et 'distance_pct' sont déjà float → cast défensif quand même
    """
    # level : STRING dans le JSON Bluestar → float
    try:
        level = float(z.get("level", 0))
    except (TypeError, ValueError):
        level = 0.0

    # score : float dans le JSON
    try:
        score = float(z.get("score", 0))
    except (TypeError, ValueError):
        score = 0.0

    # distance_pct : float dans le JSON
    try:
        dist = float(z.get("distance_pct", 999))
    except (TypeError, ValueError):
        dist = 999.0

    # timeframes : 'Daily + H4 + Weekly' → ['Daily', 'H4', 'Weekly']
    tf_raw  = str(z.get("timeframes", ""))
    tf_list = [t.strip() for t in tf_raw.split("+") if t.strip()]
    tf_nb   = len(tf_list)
    tf_has_weekly = "Weekly" in tf_list
    tf_has_daily  = "Daily"  in tf_list
    tf_has_h4     = "H4"     in tf_list

    # Poids TF (cohérent avec le scanner Bluestar)
    tf_weight = sum([
        3 if "Weekly" in tf_list else 0,
        2 if "Daily"  in tf_list else 0,
        1 if "H4"     in tf_list else 0,
    ])

    # alert : peut être '' → normalisation
    alert_raw = z.get("alert", "") or ""
    a = alert_raw.strip().upper()
    if "CHAUDE" in a:
        alert = "ZONE CHAUDE"
    elif "PROCHE" in a:
        alert = "Proche"
    else:
        alert = ""

    # signal : nettoyage
    signal_raw = str(z.get("signal", ""))
    if "BUY" in signal_raw.upper():
        signal = "BUY ZONE"
    elif "SELL" in signal_raw.upper():
        signal = "SELL ZONE"
    else:
        signal = signal_raw

    # status
    status = str(z.get("status", "Testee"))
    STATUS_COEFF = {"Vierge": 1.0, "Testee": 0.8, "Role Reverse": 0.6}
    status_coeff = STATUS_COEFF.get(status, 0.8)

    return {
        "signal":          signal,
        "level":           round(level, 5),
        "score":           round(score, 1),
        "status":          status,
        "status_coeff":    status_coeff,
        "distance_pct":    round(dist, 3),
        "alert":           alert,
        "timeframes":      tf_raw,
        "tf_list":         tf_list,
        "tf_nb":           tf_nb,
        "tf_weight":       tf_weight,
        "tf_has_weekly":   tf_has_weekly,
        "tf_has_daily":    tf_has_daily,
        "tf_has_h4":       tf_has_h4,
        # Score pondéré par statut — utile pour le ranking LLM
        "weighted_score":  round(score * status_coeff, 1),
    }


def parse_sr_file(raw: dict | list) -> dict:
    """
    Parse le fichier JSON export du scanner Bluestar S/R.
    Retourne un index {symbol_normalisé: asset_enrichi}.
    """
    assets = raw.get("assets", []) if isinstance(raw, dict) else raw
    index  = {}
    for asset in assets:
        sym  = normalize_symbol(asset.get("symbol", ""))
        ctx  = parse_price_context(asset.get("price_context", ""))
        zones_raw = asset.get("zones", [])

        # Parser et trier toutes les zones par distance croissante
        zones_parsed = sorted(
            [parse_zone(z) for z in zones_raw],
            key=lambda z: z["distance_pct"]
        )

        # Séparer BUY / SELL
        buy_zones  = [z for z in zones_parsed if z["signal"] == "BUY ZONE"]
        sell_zones = [z for z in zones_parsed if z["signal"] == "SELL ZONE"]

        # Zone chaude (< 0.5%) si existante
        hot_zones = [z for z in zones_parsed if z["distance_pct"] < 0.5]

        index[sym] = {
            "symbol":        sym,
            "price_context": ctx,
            "zones":         zones_parsed,
            "buy_zones":     buy_zones,
            "sell_zones":    sell_zones,
            "hot_zones":     hot_zones,
            "zones_count":   len(zones_parsed),
        }
    return index


def parse_mtf(mtf_str: str) -> tuple[int, str]:
    """
    Parse le champ MTF du scanner GPS.
    Multi-formats : '(85%) Bullish', 'Bullish 92%', '85% Bearish', 'BULLISH(78%)'
    """
    if not mtf_str:
        return 0, "Neutral"
    s = str(mtf_str).strip()

    pct = 0
    for pattern in [
        r'\((\d+(?:\.\d+)?)%\)',
        r'(\d+(?:\.\d+)?)%',
        r'(\d+(?:\.\d+)?)\s*(?:Bullish|Bearish|Neutral)',
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


def rsi_status(rsi_val, direction: str) -> str:
    """
    Statut RSI contextualisé (logique mean-reversion pour CHoCH sur S/R).
    Seuils configurables via sidebar.
    """
    if rsi_val is None:
        return "unknown"
    try:
        r = float(rsi_val)
    except (TypeError, ValueError):
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


def validate_invariants(signal: dict) -> list[str]:
    """Vérifie les invariants financiers post-assemblage. Retourne la liste d'avertissements."""
    w = []
    mtf = signal.get("gps", {}).get("mtf_pct", 0)
    if not isinstance(mtf, (int, float)) or not (0 <= mtf <= 100):
        w.append(f"mtf_pct hors [0,100]: {mtf}")
    atr = signal.get("gps", {}).get("atr_h4", 0)
    if atr is not None and isinstance(atr, (int, float)) and atr < 0:
        w.append(f"atr_h4 négatif: {atr}")
    dist = signal.get("distance_pct")
    if dist is not None and isinstance(dist, (int, float)) and dist < 0:
        w.append(f"distance_pct négatif: {dist}")
    if signal.get("direction") not in ("Bullish", "Bearish"):
        w.append(f"direction invalide: {signal.get('direction')!r}")
    rsi = signal.get("rsi", {}).get("rsi_h4")
    if rsi is not None:
        try:
            if not (0 <= float(rsi) <= 100):
                w.append(f"rsi_h4 hors [0,100]: {rsi}")
        except (TypeError, ValueError):
            w.append(f"rsi_h4 non numérique: {rsi}")
    return w


# ══════════════════════════════════════════════════════════════════
# SIDEBAR — Seuils RSI
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ Seuils RSI")
    st.caption("Logique mean-reversion : RSI bas = favorable pour Bullish (rebond S/R).")
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

col1, col2 = st.columns(2)
with col1:
    gps_file   = st.file_uploader("Scanner GPS",       type="json", key="gps")
    rsi_file   = st.file_uploader("Scanner RSI",       type="json", key="rsi")
with col2:
    sr_file    = st.file_uploader("Scanner S/R Bluestar (export JSON Merge)", type="json", key="sr")
    choch_file = st.file_uploader("Scanner CHoCH/BOS", type="json", key="choch")

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

    choch_signals = choch_raw.get("signals", []) if isinstance(choch_raw, dict) else choch_raw

    # ── INDEX NORMALISÉS ──
    sr_index  = parse_sr_file(sr_raw)   # parsing complet S/R

    gps_list  = gps_raw if isinstance(gps_raw, list) else gps_raw.get("data", gps_raw.get("signals", []))
    rsi_list  = rsi_raw if isinstance(rsi_raw, list) else rsi_raw.get("data", rsi_raw.get("signals", []))

    gps_index = {normalize_symbol(g.get("Paire",   "")): g for g in gps_list if isinstance(g, dict)}
    rsi_index = {normalize_symbol(r.get("Devises", "")): r for r in rsi_list if isinstance(r, dict)}

    # Diagnostics
    diag_no_gps, diag_no_rsi, diag_no_sr = [], [], []
    diag_warnings = {}

    # ── STRUCTURE DE SORTIE ──
    merged = {
        "meta": {
            "generated_at":    datetime.now(timezone.utc).isoformat(),
            "version":         "2.0",
            "source_scanners": ["gps", "rsi", "sr_bluestar", "choch"],
            "signals_count":   0,
            "note":            "Calendrier économique à joindre séparément dans le prompt LLM.",
        },
        "signals":      [],
        "gps_summary":  {"top_bullish": [], "top_bearish": []},
        "sr_hot_zones": [],
    }

    # ── ENRICHISSEMENT SIGNAUX CHOCH ──
    for signal in choch_signals:
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
        enrichment_status = "complet" if scanners_matched == 3 else "partiel" if scanners_matched >= 1 else "minimal"

        # ── GPS ──
        mtf_pct, mtf_dir = 0, "Neutral"
        gps_quality = None
        age_d1 = atr_h4 = 0
        bias_h4 = None
        if gps_match:
            mtf_pct, mtf_dir = parse_mtf(gps_match.get("MTF", ""))
            gps_quality = gps_match.get("Quality")
            try:   age_d1 = int(gps_match.get("Age D1", 0))
            except: age_d1 = 0
            try:   atr_h4 = float(gps_match.get("ATR H4", 0))
            except: atr_h4 = 0.0
            bias_h4 = gps_match.get("4H")

        # ── RSI ──
        rsi_h4_val = None
        rsi_h4_status = "unknown"
        div_h4 = div_daily = "Aucune"
        if rsi_match:
            rsi_h4_val    = rsi_match.get("RSI_H4")
            rsi_h4_status = rsi_status(rsi_h4_val, direction)
            div_h4        = rsi_match.get("DIV_H4",    "Aucune")
            div_daily     = rsi_match.get("DIV_Daily", "Aucune")

        # ── S/R Bluestar — cœur du traitement ──
        zone_available = False
        nearest_zone   = None
        zone_badge     = "🔧 ZONE MANUELLE"
        tp_zones       = []
        price_ctx      = {}
        sur_support    = None
        sur_resistance = None

        if sr_match:
            zone_available = True
            price_ctx      = sr_match["price_context"]
            sur_support    = price_ctx.get("support_level")
            sur_resistance = price_ctx.get("resistance_level")

            # Zone alignée la plus proche
            aligned_zones = sr_match["buy_zones"] if direction == "Bullish" else sr_match["sell_zones"]
            if aligned_zones:
                nearest_zone = aligned_zones[0]   # déjà triées par distance
                d = nearest_zone["distance_pct"]
                zone_badge = (
                    "⚡ ZONE CHAUDE"  if d < 0.5 else
                    "⚠️ ZONE PROCHE"  if d < 1.5 else
                    "📍 ZONE DISTANTE"
                )

            # Zones TP (direction opposée), les 3 plus proches
            tp_source = sr_match["sell_zones"] if direction == "Bullish" else sr_match["buy_zones"]
            tp_zones  = tp_source[:3]

        # ── HTF aligned ──
        htf_aligned = bool(bias_h4 and direction == bias_h4)

        # ── Order Block ──
        level    = signal.get("level", 0) or 0
        close_px = signal.get("close_price", 0) or 0
        try:
            ob_top    = float(max(level, close_px)) if (level and close_px) else float(level or close_px)
            ob_bottom = float(min(level, close_px)) if (level and close_px) else float(level or close_px)
        except (TypeError, ValueError):
            ob_top = ob_bottom = 0.0

        # ── Assemblage ──
        enriched = {
            "signal_id":             signal.get("signal_id"),
            "pair":                  pair_norm,
            "pair_raw":              pair,
            "timeframe":             signal.get("timeframe"),
            "type":                  signal.get("type"),
            "direction":             direction,
            "is_choch":              signal.get("is_choch"),
            "status":                signal.get("status"),
            "candles_elapsed":       signal.get("candles_elapsed"),
            "confluence_score":      signal.get("confluence_score"),
            "level":                 float(level) if level else None,
            "close_price":           float(close_px) if close_px else None,
            "current_price":         signal.get("current_price"),
            "distance_pct":          signal.get("distance_pct"),
            "distance_atr_multiple": signal.get("distance_atr_multiple"),
            "session":               signal.get("session"),
            "signal_time":           signal.get("signal_time"),
            "gps": {
                "mtf_pct":       mtf_pct,
                "mtf_direction": mtf_dir,
                "quality":       gps_quality,
                "age_d1":        age_d1,
                "atr_h4":        atr_h4,
                "bias_h4":       bias_h4,
            },
            "rsi": {
                "rsi_h4":        rsi_h4_val,
                "rsi_h4_status": rsi_h4_status,
                "div_h4":        div_h4,
                "div_daily":     div_daily,
            },
            "sr": {
                "price_context":        price_ctx,
                "nearest_aligned_zone": nearest_zone,
                "tp_zones":             tp_zones,
                "zone_available":       zone_available,
                "zone_badge":           zone_badge,
                "sur_support_h4":       sur_support,
                "sur_support_dist_pct": price_ctx.get("support_dist_pct") if price_ctx else None,
                "sur_resistance_h4":    sur_resistance,
                "sur_resistance_dist_pct": price_ctx.get("resistance_dist_pct") if price_ctx else None,
            },
            "order_block": {"top": ob_top, "bottom": ob_bottom},
            "htf_aligned":  htf_aligned,
            "enrichment_quality": {
                "status":           enrichment_status,
                "scanners_matched": scanners_matched,
                "gps_found":        bool(gps_match),
                "rsi_found":        bool(rsi_match),
                "sr_found":         bool(sr_match),
            },
            "data_warnings": [],
        }

        # Validation invariants
        warnings = validate_invariants(enriched)
        enriched["data_warnings"] = warnings
        if warnings:
            diag_warnings[pair_norm] = warnings

        merged["signals"].append(enriched)

    # ── GPS SUMMARY ──
    for g in gps_list:
        if not isinstance(g, dict): continue
        pct, direction = parse_mtf(g.get("MTF", ""))
        if pct >= 85 and direction in ("Bullish", "Bearish"):
            entry = {
                "pair":    normalize_symbol(g.get("Paire", "")),
                "mtf_pct": pct,
                "quality": g.get("Quality"),
                "age_d1":  int(g.get("Age D1", 0)) if g.get("Age D1") else 0,
                "bias_h4": g.get("4H"),
            }
            key = "top_bullish" if direction == "Bullish" else "top_bearish"
            merged["gps_summary"][key].append(entry)

    for key in ("top_bullish", "top_bearish"):
        merged["gps_summary"][key].sort(key=lambda x: x["mtf_pct"], reverse=True)
        merged["gps_summary"][key] = merged["gps_summary"][key][:5]

    # ── SR HOT ZONES — depuis l'index S/R parsé ──
    for sym, asset in sr_index.items():
        for z in asset["zones"]:
            if z["distance_pct"] < 2.0:
                merged["sr_hot_zones"].append({
                    "pair":            sym,
                    "signal":          z["signal"],
                    "level":           z["level"],
                    "score":           z["score"],
                    "weighted_score":  z["weighted_score"],
                    "status":          z["status"],
                    "distance_pct":    z["distance_pct"],
                    "alert":           z["alert"],
                    "timeframes":      z["timeframes"],
                    "tf_nb":           z["tf_nb"],
                    "tf_has_weekly":   z["tf_has_weekly"],
                })
    merged["sr_hot_zones"].sort(key=lambda x: x["distance_pct"])
    merged["meta"]["signals_count"] = len(merged["signals"])

    # ── EXPORT ──
    merged_json_str = json.dumps(merged, indent=2, ensure_ascii=False)

    st.success(
        f"✅ **{len(merged['signals'])}** signal(s) enrichi(s) · "
        f"**{len(merged['sr_hot_zones'])}** zones S/R ≤ 2% · "
        f"**{sum(1 for s in merged['signals'] if s['enrichment_quality']['status'] == 'complet')}** enrichissements complets"
    )

    c1, c2 = st.columns([1, 2])
    with c1:
        st.download_button(
            "📥 Télécharger merged_pipeline.json",
            data=merged_json_str,
            file_name=f"merged_{datetime.now(timezone.utc):%Y%m%d_%H%M}UTC.json",
            mime="application/json",
            use_container_width=True,
        )
    with c2:
        with st.expander("Prévisualiser JSON"):
            st.code(merged_json_str[:3000] + "\n...", language="json")

    # ── RÉSUMÉ ──
    st.divider()
    st.subheader("Résumé")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Signaux",         len(merged["signals"]))
    m2.metric("Enrichis complet",sum(1 for s in merged["signals"] if s["enrichment_quality"]["status"] == "complet"))
    m3.metric("HTF alignés",     sum(1 for s in merged["signals"] if s["htf_aligned"]))
    m4.metric("Zones S/R ≤ 2%", len(merged["sr_hot_zones"]))
    m5.metric("Avertissements",  len(diag_warnings))

    # Diagnostics matching
    all_missing = set(diag_no_gps + diag_no_rsi + diag_no_sr)
    if all_missing:
        with st.expander(f"⚠️ {len(all_missing)} paire(s) avec données manquantes"):
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
            st.caption("Vérifier la cohérence des formats de symboles entre scanners.")

    # Avertissements invariants
    if diag_warnings:
        with st.expander(f"🔴 Avertissements invariants ({len(diag_warnings)} paire(s))"):
            for pair, warns in diag_warnings.items():
                st.markdown(f"**`{pair}`** : " + " | ".join(warns))

    # Détail signaux
    if merged["signals"]:
        st.markdown("**Signaux enrichis :**")
        for s in merged["signals"]:
            htf    = "✅" if s["htf_aligned"] else "⚠️"
            enrich = {"complet": "🟢", "partiel": "🟡", "minimal": "🔴"}.get(
                s["enrichment_quality"]["status"], "⚪")
            nz     = s["sr"]["nearest_aligned_zone"]
            zinfo  = f"Sc:{nz['score']:.0f} {nz['status']} {nz['alert']}" if nz else "—"
            warns  = f" ⚡{len(s['data_warnings'])}w" if s["data_warnings"] else ""
            st.markdown(
                f"- `{s['pair']}` {htf} HTF | {enrich} Enrich | "
                f"{s['sr']['zone_badge']} [{zinfo}] | "
                f"MTF {s['gps']['mtf_pct']}% | Score {s['confluence_score']}{warns}"
            )

    # Aperçu zones S/R hot
    if merged["sr_hot_zones"]:
        st.markdown("**Zones S/R ≤ 2% (toutes paires) :**")
        for z in merged["sr_hot_zones"]:
            badge = "🔥" if z["alert"] == "ZONE CHAUDE" else "📍"
            w_tag = "W" if z["tf_has_weekly"] else "  "
            st.markdown(
                f"- {badge} `{z['pair']}` {z['signal']} "
                f"@ **{z['level']:.5f}** | "
                f"Sc:{z['score']:.0f} (pond.:{z['weighted_score']:.0f}) | "
                f"{z['status']} | {z['distance_pct']:.2f}% | "
                f"{z['timeframes']} [{w_tag}]"
            )
