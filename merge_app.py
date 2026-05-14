import streamlit as st
import json
import re
from datetime import datetime, timedelta

st.set_page_config(page_title="🔷 BLUESTAR MERGE v1.0", layout="wide")

st.title("🔷 BLUESTAR MERGE v1.0")
st.markdown("*Application de merge cross-scanner pour BLUESTAR DIRECT v7.0*")
st.divider()

# ───────────────────────────────────────────
# SECTION 1 : UPLOAD DES 4 JSON
# ───────────────────────────────────────────
st.subheader("📁 1. Upload des scanners JSON")

col1, col2 = st.columns(2)
with col1:
    gps_file = st.file_uploader("Scanner GPS (.json)", type="json", key="gps")
    rsi_file = st.file_uploader("Scanner RSI (.json)", type="json", key="rsi")
with col2:
    sr_file = st.file_uploader("Scanner S/R (.json)", type="json", key="sr")
    choch_file = st.file_uploader("Scanner CHoCH/BOS (.json)", type="json", key="choch")

# ───────────────────────────────────────────
# SECTION 2 : CALENDRIER MANUEL
# ───────────────────────────────────────────
st.divider()
st.subheader("📅 2. Calendrier Economique (High Impact)")
st.markdown("Saisissez les evenements a venir. Le calcul `hours_from_now` se fait automatiquement.")

# Date/heure actuelle pour le calcul
now = datetime.now()
st.caption(f"Heure systeme actuelle : {now.strftime('%Y-%m-%d %H:%M')} — utilisee pour calculer les delais")

n_events = st.number_input("Nombre d'evenements a saisir", min_value=0, max_value=15, value=3)

events = []
for i in range(int(n_events)):
    with st.container(border=True):
        cols = st.columns([1.2, 1, 1, 2, 1, 1])
        with cols[0]:
            date_str = st.text_input(f"Date", value=now.strftime("%Y-%m-%d"), key=f"cal_d_{i}")
        with cols[1]:
            time_str = st.text_input(f"Heure", placeholder="13:30", key=f"cal_t_{i}")
        with cols[2]:
            currency = st.selectbox(f"Devise", ["USD","EUR","GBP","JPY","CAD","AUD","NZD","CHF"], key=f"cal_c_{i}")
        with cols[3]:
            event_name = st.text_input(f"Nom de l'event", placeholder="Core CPI m/m", key=f"cal_e_{i}")
        with cols[4]:
            impact = st.selectbox(f"Impact", ["High", "Medium", "Low"], index=0, key=f"cal_i_{i}")
        with cols[5]:
            prev = st.text_input(f"Prevision", placeholder="0.3%", key=f"cal_p_{i}")

        if date_str and time_str:
            try:
                event_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                hours_from_now = int((event_dt - now).total_seconds() / 3600)
                st.caption(f"T-{hours_from_now}h")
                events.append({
                    "date": date_str,
                    "time": time_str,
                    "currency": currency,
                    "event": event_name,
                    "impact": impact,
                    "forecast": prev,
                    "hours_from_now": hours_from_now
                })
            except ValueError:
                st.error("Format invalide (YYYY-MM-DD HH:MM)")

# ───────────────────────────────────────────
# SECTION 3 : GENERATION DU MERGE
# ───────────────────────────────────────────
st.divider()

if st.button("🔵 GENERER LE MERGED PIPELINE", type="primary", use_container_width=True):

    # Verification des fichiers
    if not all([gps_file, rsi_file, sr_file, choch_file]):
        st.error("Les 4 fichiers JSON sont obligatoires.")
        st.stop()

    # ── LECTURE ──
    gps_data = json.load(gps_file)
    rsi_data = json.load(rsi_file)
    sr_raw = json.load(sr_file)
    choch_raw = json.load(choch_file)

    sr_data = sr_raw.get("assets", []) if isinstance(sr_raw, dict) else sr_raw
    choch_signals = choch_raw.get("signals", []) if isinstance(choch_raw, dict) else choch_raw

    # ── STRUCTURE DE SORTIE ──
    merged = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "version": "7.0",
            "source_scanners": ["gps", "rsi", "sr", "choch"],
            "event_count": len(events)
        },
        "signals": [],
        "gps_summary": {"top_bullish": [], "top_bearish": []},
        "sr_hot_zones": [],
        "calendar_future_events": events
    }

    # ── HELPERS ──
    def parse_mtf(mtf_str):
        if not mtf_str:
            return 0, "Neutral"
        m = re.search(r'\((\d+)%\)', str(mtf_str))
        pct = int(m.group(1)) if m else 0
        direction = "Bullish" if "Bullish" in str(mtf_str) else "Bearish" if "Bearish" in str(mtf_str) else "Neutral"
        return pct, direction

    def rsi_status(rsi_val, direction):
        if rsi_val is None:
            return "unknown"
        r = float(rsi_val)
        if direction == "Bullish":
            if r < 40: return "favorable"
            elif r < 60: return "neutre"
            elif r < 68: return "overbought"
            elif r < 72: return "tension"
            else: return "extreme_overbought"
        else:  # Bearish
            if r > 60: return "favorable"
            elif r > 40: return "neutre"
            elif r > 32: return "oversold"
            elif r > 28: return "tension"
            else: return "extreme_oversold"

    def calendar_flags(pair, events_list):
        currencies = pair.replace("_", "/").split("/")
        suspend = False
        badge = None
        for e in events_list:
            if e["currency"] in currencies and e["impact"] == "High":
                if e["hours_from_now"] <= 24:
                    suspend = True
                elif 24 < e["hours_from_now"] <= 48:
                    badge = "⏰ EVENT PROCHE 24-48H"
        return suspend, badge

    # ── ENRICHISSEMENT DES SIGNAUX CHoCH ──
    for signal in choch_signals:
        pair = signal.get("pair", "")
        direction = signal.get("direction", "Bullish")

        # Matching
        gps_match = next((g for g in gps_data if g.get("Paire") == pair), None)
        rsi_match = next((r for r in rsi_data if r.get("Devises") == pair), None)
        sr_match = next((s for s in sr_data if s.get("symbol") == pair), None)

        # GPS parsing
        mtf_pct, mtf_dir = 0, "Neutral"
        gps_quality = None
        age_d1 = None
        atr_h4 = None
        bias_h4 = None
        if gps_match:
            mtf_pct, mtf_dir = parse_mtf(gps_match.get("MTF"))
            gps_quality = gps_match.get("Quality")
            try:
                age_d1 = int(gps_match.get("Age D1", 0))
            except:
                age_d1 = 0
            try:
                atr_h4 = float(gps_match.get("ATR H4", 0))
            except:
                atr_h4 = 0.0
            bias_h4 = gps_match.get("4H")

        # RSI
        rsi_h4_val = None
        rsi_h4_status = "unknown"
        div_h4 = "Aucune"
        div_daily = "Aucune"
        if rsi_match:
            rsi_h4_val = rsi_match.get("RSI_H4")
            rsi_h4_status = rsi_status(rsi_h4_val, direction)
            div_h4 = rsi_match.get("DIV_H4", "Aucune")
            div_daily = rsi_match.get("DIV_Daily", "Aucune")

        # S/R
        zone_available = False
        nearest_zone = None
        zone_badge = "🔧 ZONE MANUELLE"
        sur_support = None
        sur_resistance = None

        if sr_match:
            zone_available = True
            zones = sr_match.get("zones", [])
            for z in zones:
                z_signal = z.get("signal", "")
                if (direction == "Bullish" and "BUY" in z_signal) or \
                   (direction == "Bearish" and "SELL" in z_signal):
                    nearest_zone = {
                        "level": float(z.get("level", 0)),
                        "distance_pct": float(z.get("distance_pct", 999))
                    }
                    dist = nearest_zone["distance_pct"]
                    if dist < 0.5:
                        zone_badge = "⚡ ZONE CHAUDE"
                    else:
                        zone_badge = "⚠️ ZONE PROCHE"
                    break
            # Price context parsing
            ctx = sr_match.get("price_context", "")
            sup_m = re.search(r'support[:\s]+([\d.]+)', ctx, re.I)
            res_m = re.search(r'resistance[:\s]+([\d.]+)', ctx, re.I)
            if sup_m:
                sur_support = float(sup_m.group(1))
            if res_m:
                sur_resistance = float(res_m.group(1))

        # Calendar
        cal_suspend, cal_badge = calendar_flags(pair, events)

        # HTF aligned
        htf_aligned = False
        if bias_h4:
            htf_aligned = (direction == bias_h4)

        # Order Block (proxy depuis signal)
        level = signal.get("level", 0)
        close_px = signal.get("close_price", 0)
        ob_top = max(level, close_px) if level and close_px else level or close_px
        ob_bottom = min(level, close_px) if level and close_px else level or close_px

        # Assemblage
        enriched = {
            "signal_id": signal.get("signal_id"),
            "pair": pair,
            "timeframe": signal.get("timeframe"),
            "type": signal.get("type"),
            "direction": direction,
            "order": signal.get("order"),
            "is_bullish": signal.get("is_bullish"),
            "trend": signal.get("trend"),
            "is_choch": signal.get("is_choch"),
            "status": signal.get("status"),
            "candles_elapsed": signal.get("candles_elapsed"),
            "confluence_score": signal.get("confluence_score"),
            "level": level,
            "close_price": close_px,
            "current_price": signal.get("current_price"),
            "distance_pct": signal.get("distance_pct"),
            "distance_atr_multiple": signal.get("distance_atr_multiple"),
            "volatility": signal.get("volatility"),
            "force": signal.get("force"),
            "bb_regime": signal.get("bb_regime"),
            "session": signal.get("session"),
            "signal_time": signal.get("signal_time"),
            "gps": {
                "mtf_pct": mtf_pct,
                "mtf_direction": mtf_dir,
                "quality": gps_quality,
                "age_d1": age_d1,
                "atr_h4": atr_h4,
                "bias_h4": bias_h4
            },
            "rsi": {
                "rsi_h4": rsi_h4_val,
                "rsi_h4_status": rsi_h4_status,
                "div_h4": div_h4,
                "div_daily": div_daily
            },
            "sr": {
                "nearest_aligned_zone": nearest_zone,
                "zone_available": zone_available,
                "zone_badge": zone_badge,
                "sur_support_h4": sur_support,
                "sur_resistance_h4": sur_resistance
            },
            "order_block": {
                "top": ob_top,
                "bottom": ob_bottom
            },
            "fair_value_gap": {
                "top": None,
                "bottom": None
            },
            "calendar_suspend": cal_suspend,
            "calendar_badge": cal_badge,
            "htf_aligned": htf_aligned
        }

        merged["signals"].append(enriched)

    # ── GPS SUMMARY (Top 5) ──
    for g in gps_data:
        pair = g.get("Paire", "")
        mtf_str = g.get("MTF", "")
        pct, direction = parse_mtf(mtf_str)
        if pct >= 85 and direction in ["Bullish", "Bearish"]:
            entry = {
                "pair": pair,
                "mtf_pct": pct,
                "quality": g.get("Quality"),
                "age_d1": int(g.get("Age D1", 0)) if g.get("Age D1") else 0,
                "bias_h4": g.get("4H")
            }
            if direction == "Bullish":
                merged["gps_summary"]["top_bullish"].append(entry)
            else:
                merged["gps_summary"]["top_bearish"].append(entry)

    merged["gps_summary"]["top_bullish"].sort(key=lambda x: x["mtf_pct"], reverse=True)
    merged["gps_summary"]["top_bearish"].sort(key=lambda x: x["mtf_pct"], reverse=True)
    merged["gps_summary"]["top_bullish"] = merged["gps_summary"]["top_bullish"][:5]
    merged["gps_summary"]["top_bearish"] = merged["gps_summary"]["top_bearish"][:5]

    # ── SR HOT ZONES (< 0.5%) ──
    for asset in sr_data:
        symbol = asset.get("symbol", "")
        for z in asset.get("zones", []):
            try:
                dist = float(z.get("distance_pct", 999))
                if dist < 0.5:
                    merged["sr_hot_zones"].append({
                        "pair": symbol,
                        "signal": z.get("signal"),
                        "level": z.get("level"),
                        "distance_pct": dist,
                        "timeframes": z.get("timeframes", "")
                    })
            except:
                continue

    # ── EXPORT ──
    merged_json_str = json.dumps(merged, indent=2, ensure_ascii=False)

    st.success(f"Merge genere : {len(merged['signals'])} signal(x) enrichi(s)")

    col_dl, col_preview = st.columns([1, 2])
    with col_dl:
        st.download_button(
            "Telecharger merged_pipeline.json",
            data=merged_json_str,
            file_name=f"merged_pipeline_{datetime.now():%Y%m%d_%H%M}.json",
            mime="application/json",
            use_container_width=True
        )

    with col_preview:
        with st.expander("Previsualiser le JSON (debut)"):
            preview = merged_json_str[:2500] + "\n..."
            st.code(preview, language="json")

    # Resume pour le trader
    st.divider()
    st.subheader("Resume du merge")

    rcol1, rcol2, rcol3, rcol4 = st.columns(4)
    rcol1.metric("Signaux CHoCH", len(choch_signals))
    rcol2.metric("Signaux enrichis", len(merged["signals"]))
    rcol3.metric("Zones chaudes S/R", len(merged["sr_hot_zones"]))
    rcol4.metric("Events calendrier", len(events))

    if merged["signals"]:
        st.markdown("**Detail des signaux :**")
        for s in merged["signals"]:
            pair = s["pair"]
            conv = "✅" if s["htf_aligned"] else "⚠️"
            cal = "🚫" if s["calendar_suspend"] else "✅"
            zone = s["sr"]["zone_badge"]
            st.markdown(f"- `{pair}` {conv} HTF | {cal} Cal | {zone} | Score {s['confluence_score']} | MTF {s['gps']['mtf_pct']}%")
