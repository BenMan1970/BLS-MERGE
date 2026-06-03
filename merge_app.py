"""
BLUESTAR MERGE v3.4.2 — Production-grade Streamlit application.
Multi-scanner JSON merge engine with auto-detection, canonical pivot model,
heuristic fallback, full pipeline diagnostics, and hardened against malformed
input, DoS, and partial failures.

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
    eliminating ~40% of arithmetic ops on the model side an...s JSON pour démarrer.")
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
