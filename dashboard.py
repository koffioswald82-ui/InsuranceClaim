"""AXA Claims Lakehouse — Streamlit Dashboard.

Reads Delta tables from lake/ (local) or exports/ (Streamlit Cloud).
Developed by Oswald Jaures KOFFI.
"""
import glob
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

os.environ.setdefault("HADOOP_HOME", "C:/hadoop")

BASE_DIR = Path(__file__).parent
LAKE     = BASE_DIR / "lake"
EXPORTS  = BASE_DIR / "exports"

USE_EXPORTS = not (LAKE / "silver").exists()

st.set_page_config(
    page_title="AXA Claims Lakehouse",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Data loading ───────────────────────────────────────────────────────────────

def _read_delta(rel_path: str) -> pd.DataFrame:
    files = [
        f for f in glob.glob(str(LAKE / rel_path / "**" / "*.parquet"), recursive=True)
        if "_delta_log" not in f
    ]
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _read_export(name: str) -> pd.DataFrame:
    path = EXPORTS / f"{name}.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _load_insee() -> dict:
    for candidate in [
        BASE_DIR / "data" / "external" / "insee_regions.csv",
        EXPORTS / "insee_regions.csv",
    ]:
        if candidate.exists():
            df = pd.read_csv(candidate, dtype={"code": str}, encoding="utf-8")
            return dict(zip(df["code"].str.strip(), df["nom"]))
    return {}


@st.cache_data(ttl=3600, show_spinner="Loading data...")
def load_data():
    if USE_EXPORTS:
        claims = _read_export("claims")
        fraud  = _read_export("fraud_assessment")
        kpis   = _read_export("portfolio_kpis")
        churn  = _read_export("churn_scores")
    else:
        claims = _read_delta("silver/claims")
        fraud  = _read_delta("gold/fraud_assessment")
        kpis   = _read_delta("gold/portfolio_kpis")
        churn  = _read_delta("gold/churn_scores")
    claims["date_sinistre"] = pd.to_datetime(claims["date_sinistre"], errors="coerce")
    insee = _load_insee()
    return claims, fraud, kpis, churn, insee


def _load_report_html() -> str:
    for candidate in [
        BASE_DIR / "reports" / "quality_report.html",
        EXPORTS / "quality_report.html",
    ]:
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    return "<p>Report not available. Run <code>generate_report.py</code> first.</p>"


def _add_region_nom(df: pd.DataFrame, insee: dict, col: str = "region_code") -> pd.DataFrame:
    df = df.copy()
    df["region_nom"] = df[col].astype(str).str.strip().map(insee).fillna(df[col].astype(str))
    return df


claims_raw, fraud_raw, kpis_raw, churn_raw, INSEE = load_data()

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## Filters")

    produits = sorted(claims_raw["produit"].dropna().unique().tolist())
    sel_produits = st.multiselect("Product", produits, default=produits)

    statuts = sorted(claims_raw["statut"].dropna().unique().tolist())
    sel_statuts = st.multiselect("Claim Status", statuts, default=statuts)

    years = sorted(claims_raw["date_sinistre"].dt.year.dropna().unique().astype(int).tolist())
    if len(years) >= 2:
        sel_years = st.select_slider("Years", options=years, value=(min(years), max(years)))
    else:
        sel_years = (min(years, default=2020), max(years, default=2025))

    regions_all = sorted(claims_raw["region_code"].astype(str).dropna().unique().tolist())
    region_labels = {r: INSEE.get(r, r) for r in regions_all}
    sel_regions = st.multiselect(
        "Region", options=regions_all, default=regions_all,
        format_func=lambda r: region_labels.get(r, r),
    )

    st.divider()
    mode = "Cloud (exports/)" if USE_EXPORTS else "Local (lake/)"
    st.caption(f"Source: {mode}")
    st.caption(f"Refreshed: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    st.divider()
    st.markdown(
        "<div style='font-size:12px;color:#6b7280;text-align:center'>"
        "Developed by<br><b>Oswald Jaures KOFFI</b><br>"
        "AXA Claims Lakehouse · PySpark 3.5.1"
        "</div>",
        unsafe_allow_html=True,
    )

# Apply filters
mask = (
    claims_raw["produit"].isin(sel_produits)
    & claims_raw["statut"].isin(sel_statuts)
    & claims_raw["date_sinistre"].dt.year.between(sel_years[0], sel_years[1])
    & claims_raw["region_code"].astype(str).isin(sel_regions)
)
claims = _add_region_nom(claims_raw[mask].copy(), INSEE)
fraud_ids = set(claims["claim_id"].dropna())
fraud = fraud_raw[fraud_raw["claim_id"].isin(fraud_ids)].copy()
fraud = fraud.merge(
    claims[["claim_id", "region_code", "region_nom"]], on="claim_id", how="left"
)

# Global KPIs
n_claims    = len(claims)
n_policies  = claims["policy_id"].nunique()
fraud_rate  = float(fraud["is_fraud_suspected"].mean() * 100) if len(fraud) else 0.0
avg_amt     = claims["montant_declare"].mean()
avg_delay   = claims["delai_declaration_jours"].mean()
avg_lr_kpis = float(kpis_raw["loss_ratio"].mean() * 100) if len(kpis_raw) else 0.0

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("AXA Claims Lakehouse — Dashboard")
st.caption(
    f"{n_claims:,} claims  |  {len(fraud):,} fraud assessments  |  "
    f"{len(churn_raw):,} clients scored  |  "
    f"*Developed by **Oswald Jaures KOFFI***"
)

# ── Tabs ───────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Quality Report", "Overview", "Fraud Detection",
    "Portfolio", "Geography", "Churn",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Quality Report
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    st.subheader("Data Quality Report")
    st.caption(
        "Static report generated by generate_report.py. "
        "Re-run after each pipeline execution to refresh."
    )
    components.html(_load_report_html(), height=920, scrolling=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Overview
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Claims",            f"{n_claims:,}")
    c2.metric("Unique Policies",   f"{n_policies:,}")
    c3.metric("Fraud Rate",        f"{fraud_rate:.1f}%")
    c4.metric("Avg Loss Ratio",    f"{avg_lr_kpis:.1f}%")
    c5.metric("Avg Claim Amount",  f"{avg_amt:,.0f} €" if pd.notna(avg_amt) else "-")
    c6.metric("Avg Filing Delay",  f"{avg_delay:.0f} d" if pd.notna(avg_delay) else "-")

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Claims by Status")
        df_s = claims["statut"].value_counts().reset_index().rename(columns={"count": "nb"})
        fig = px.bar(df_s, x="statut", y="nb", color="statut",
                     labels={"statut": "Status", "nb": "Claims"},
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Claims by Product")
        df_p = claims["produit"].value_counts().reset_index().rename(columns={"count": "nb"})
        fig = px.bar(df_p, x="nb", y="produit", orientation="h",
                     color="nb", color_continuous_scale="Blues",
                     labels={"produit": "", "nb": "Claims"})
        fig.update_layout(height=300, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Geographic Distribution")
    col_c, col_d = st.columns(2)

    with col_c:
        reg_cnt = (claims.groupby("region_nom").size().reset_index(name="nb")
                   .sort_values("nb", ascending=True))
        fig = px.bar(reg_cnt, x="nb", y="region_nom", orientation="h",
                     color="nb", color_continuous_scale="Teal",
                     labels={"region_nom": "", "nb": "Claims"}, text="nb")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=420, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_d:
        reg_amt = (claims.groupby("region_nom")["montant_declare"].mean()
                   .reset_index(name="avg_amount").sort_values("avg_amount", ascending=True))
        fig = px.bar(reg_amt, x="avg_amount", y="region_nom", orientation="h",
                     color="avg_amount", color_continuous_scale="Purples",
                     labels={"region_nom": "", "avg_amount": "Avg Claim Amount (€)"},
                     text=reg_amt["avg_amount"].map(lambda x: f"{x:,.0f} €"))
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Monthly Claims Trend")
    monthly = (
        claims.assign(month=claims["date_sinistre"].dt.to_period("M").astype(str))
        .groupby("month")
        .agg(nb=("claim_id", "count"), amount=("montant_declare", "sum"))
        .reset_index().sort_values("month")
    )
    fig = px.line(monthly, x="month", y="nb", markers=True, line_shape="spline",
                  labels={"month": "Month", "nb": "Claims"})
    fig.update_layout(height=260, margin=dict(t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Fraud Detection
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    n_suspects = int(fraud["is_fraud_suspected"].sum()) if len(fraud) else 0
    avg_score  = float(fraud["fraud_score"].mean()) if len(fraud) else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Claims Assessed",  f"{len(fraud):,}")
    c2.metric("Suspected Fraud",  f"{n_suspects:,}", f"{fraud_rate:.1f}% of total")
    c3.metric("Avg Fraud Score",  f"{avg_score:.1f} / 100")

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Fraud Score Distribution")
        bins   = [0, 25, 50, 75, 100]
        labels = ["0-25 (low)", "26-50 (medium)", "51-75 (high)", "76-100 (critical)"]
        fraud = fraud.copy()
        fraud["score_bin"] = pd.cut(
            fraud["fraud_score"], bins=bins, labels=labels, include_lowest=True
        )
        score_df = (fraud["score_bin"].value_counts()
                    .reset_index().rename(columns={"count": "nb"})
                    .sort_values("score_bin"))
        fig = px.bar(score_df, x="score_bin", y="nb", color="score_bin",
                     color_discrete_sequence=["#22c55e", "#f59e0b", "#ef4444", "#7c3aed"],
                     labels={"score_bin": "Score Range", "nb": "Claims"})
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Fraud Rate by Product")
        fraud_prod = (
            fraud.groupby("produit")
            .agg(total=("claim_id", "count"), suspects=("is_fraud_suspected", "sum"))
            .assign(rate=lambda d: d["suspects"] / d["total"] * 100)
            .reset_index().sort_values("rate", ascending=False)
        )
        fig = px.bar(fraud_prod, x="rate", y="produit", orientation="h",
                     color="rate", color_continuous_scale="Reds",
                     text=fraud_prod["rate"].map(lambda x: f"{x:.1f}%"),
                     labels={"rate": "Fraud Rate (%)", "produit": ""})
        fig.update_layout(height=300, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Geographic Fraud Analysis")
    col_c, col_d = st.columns(2)

    with col_c:
        fraud_reg = (
            fraud.dropna(subset=["region_nom"])
            .groupby("region_nom")
            .agg(total=("claim_id", "count"), suspects=("is_fraud_suspected", "sum"))
            .assign(rate=lambda d: d["suspects"] / d["total"] * 100)
            .reset_index().sort_values("rate", ascending=True)
        )
        fig = px.bar(fraud_reg, x="rate", y="region_nom", orientation="h",
                     color="rate", color_continuous_scale="Reds",
                     text=fraud_reg["rate"].map(lambda x: f"{x:.1f}%"),
                     labels={"region_nom": "", "rate": "Fraud Rate (%)"},
                     title="Fraud Rate by Region")
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=40, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_d:
        fraud_reg_score = (
            fraud.dropna(subset=["region_nom"])
            .groupby("region_nom")["fraud_score"].mean()
            .reset_index(name="avg_score").sort_values("avg_score", ascending=True)
        )
        fig = px.bar(fraud_reg_score, x="avg_score", y="region_nom", orientation="h",
                     color="avg_score", color_continuous_scale="Oranges",
                     text=fraud_reg_score["avg_score"].map(lambda x: f"{x:.1f}"),
                     labels={"region_nom": "", "avg_score": "Avg Fraud Score"},
                     title="Average Fraud Score by Region")
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=40, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Fraud Rule Triggers")
    flag_cols = [c for c in fraud.columns if c.startswith("flag_")]
    if flag_cols:
        flags_df = fraud[flag_cols].sum().reset_index()
        flags_df.columns = ["rule", "triggers"]
        flags_df = flags_df.sort_values("triggers", ascending=False)
        fig = px.bar(flags_df, x="rule", y="triggers",
                     color="triggers", color_continuous_scale="Oranges",
                     text="triggers",
                     labels={"rule": "Rule", "triggers": "Trigger Count"})
        fig.update_layout(height=280, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Portfolio
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    lr_prod = (
        claims.groupby("produit")
        .agg(losses=("montant_declare", "sum"), premiums=("prime_annuelle", "sum"))
        .assign(loss_ratio=lambda d: d["losses"] / d["premiums"].replace(0, float("nan")) * 100)
        .reset_index().sort_values("loss_ratio", ascending=False)
        .dropna(subset=["loss_ratio"])
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Loss Ratio by Product")
        fig = px.bar(lr_prod, x="produit", y="loss_ratio",
                     color="loss_ratio",
                     color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444"],
                     text=lr_prod["loss_ratio"].map(lambda x: f"{x:.0f}%"),
                     labels={"produit": "Product", "loss_ratio": "Loss Ratio (%)"})
        fig.add_hline(y=100, line_dash="dash", line_color="red",
                      annotation_text="100% threshold", annotation_position="top right")
        fig.update_layout(height=340, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Avg Claim Amount by Type")
        type_df = (
            claims.groupby("type_sinistre")
            .agg(avg_amount=("montant_declare", "mean"), nb=("claim_id", "count"))
            .reset_index().sort_values("avg_amount", ascending=False)
        )
        fig = px.bar(type_df, x="avg_amount", y="type_sinistre", orientation="h",
                     color="avg_amount", color_continuous_scale="Blues",
                     text=type_df["avg_amount"].map(lambda x: f"{x:,.0f} €"),
                     labels={"type_sinistre": "", "avg_amount": "Avg Amount (€)"})
        fig.update_layout(height=340, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Regional Portfolio Analysis")
    col_c, col_d = st.columns(2)

    with col_c:
        kpis_reg = _add_region_nom(kpis_raw.copy(), INSEE)
        lr_reg = (
            kpis_reg.groupby("region_nom")["loss_ratio"].mean()
            .reset_index(name="avg_lr")
            .assign(lr_pct=lambda d: d["avg_lr"] * 100)
            .sort_values("lr_pct", ascending=True)
        )
        fig = px.bar(lr_reg, x="lr_pct", y="region_nom", orientation="h",
                     color="lr_pct",
                     color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444"],
                     text=lr_reg["lr_pct"].map(lambda x: f"{x:.0f}%"),
                     labels={"region_nom": "", "lr_pct": "Loss Ratio (%)"},
                     title="Avg Loss Ratio by Region")
        fig.add_vline(x=100, line_dash="dash", line_color="red")
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=40, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_d:
        heat = claims.groupby(["region_nom", "produit"]).size().reset_index(name="nb")
        heat_pivot = heat.pivot(index="region_nom", columns="produit", values="nb").fillna(0)
        fig = px.imshow(heat_pivot, color_continuous_scale="Blues", aspect="auto",
                        text_auto=True,
                        labels={"color": "Claims", "x": "Product", "y": "Region"},
                        title="Claims Heatmap: Region x Product")
        fig.update_layout(height=420, margin=dict(t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Claim Amount Distribution")
    p99 = claims["montant_declare"].quantile(0.99)
    fig = px.histogram(
        claims[claims["montant_declare"] < p99],
        x="montant_declare", nbins=60, color="produit",
        barmode="overlay", opacity=0.7,
        labels={"montant_declare": "Declared Amount (€)", "count": "Claims"})
    fig.update_layout(height=280, margin=dict(t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)

    if len(kpis_raw):
        st.subheader("Aggregated Portfolio KPIs")
        display_cols = ["region_code", "canal_vente", "nb_polices", "nb_sinistres",
                        "loss_ratio", "delai_moyen_declaration"]
        display_cols = [c for c in display_cols if c in kpis_raw.columns]
        kpis_show = kpis_raw[display_cols].copy()
        if "region_code" in kpis_show.columns:
            kpis_show.insert(0, "region",
                             kpis_show["region_code"].astype(str).map(INSEE)
                             .fillna(kpis_show["region_code"].astype(str)))
            kpis_show = kpis_show.drop(columns=["region_code"])
        if "loss_ratio" in kpis_show.columns:
            kpis_show["loss_ratio"] = (kpis_show["loss_ratio"] * 100).round(1).astype(str) + " %"
        st.dataframe(kpis_show.rename(columns={
            "canal_vente": "channel", "nb_polices": "policies",
            "nb_sinistres": "claims", "delai_moyen_declaration": "avg_filing_delay_d",
        }), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Geography
# ══════════════════════════════════════════════════════════════════════════════

with tab5:
    st.caption(
        "Cross-dimensional analysis: region × claim type and region × product — "
        "identifies risk concentration zones and subscription dynamics."
    )

    st.subheader("Claim Exposure by Type and Region")

    geo_type = (
        claims.groupby(["region_nom", "type_sinistre"])
        .agg(total_amount=("montant_declare", "sum"), nb=("claim_id", "count"))
        .reset_index()
    )

    col_a, col_b = st.columns(2)
    with col_a:
        pivot_mt = (geo_type.pivot(index="region_nom", columns="type_sinistre",
                                   values="total_amount").fillna(0).astype(int))
        fig = px.imshow(pivot_mt, color_continuous_scale="YlOrRd", aspect="auto",
                        text_auto=".2s",
                        labels={"color": "Amount (€)", "x": "Claim Type", "y": "Region"},
                        title="Total Claim Amount (Region x Type)")
        fig.update_layout(height=460, margin=dict(t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        reg_order = (geo_type.groupby("region_nom")["total_amount"]
                     .sum().sort_values(ascending=True).index.tolist())
        fig = px.bar(geo_type, x="total_amount", y="region_nom",
                     color="type_sinistre", orientation="h", barmode="stack",
                     category_orders={"region_nom": reg_order},
                     labels={"total_amount": "Total Amount (€)", "region_nom": "",
                             "type_sinistre": "Claim Type"},
                     title="Total Amount by Region, Breakdown by Type")
        fig.update_layout(height=460, margin=dict(t=40, b=0), legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

    col_c, col_d = st.columns(2)
    with col_c:
        pivot_nb = (geo_type.pivot(index="region_nom", columns="type_sinistre", values="nb")
                    .fillna(0).astype(int))
        fig = px.imshow(pivot_nb, color_continuous_scale="Blues", aspect="auto",
                        text_auto=True,
                        labels={"color": "Claims", "x": "Claim Type", "y": "Region"},
                        title="Number of Claims (Region x Type)")
        fig.update_layout(height=420, margin=dict(t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_d:
        top_type = (
            geo_type.sort_values("total_amount", ascending=False)
            .groupby("region_nom").first().reset_index()
            [["region_nom", "type_sinistre", "total_amount", "nb"]]
            .rename(columns={"type_sinistre": "dominant_risk",
                              "total_amount": "amount", "nb": "claims"})
            .sort_values("amount", ascending=False)
        )
        st.markdown("**Dominant Risk per Region**")
        st.dataframe(
            top_type.assign(amount=top_type["amount"].map(lambda x: f"{x:,.0f} €")),
            use_container_width=True, hide_index=True,
        )

    st.divider()
    st.subheader("Insurance Subscription by Product and Region")

    geo_prod = (
        claims.groupby(["region_nom", "produit"])
        .agg(nb_claims=("claim_id", "count"),
             total_premium=("prime_annuelle", "sum"),
             total_losses=("montant_declare", "sum"))
        .reset_index()
    )
    reg_order_prod = (geo_prod.groupby("region_nom")["total_premium"]
                      .sum().sort_values(ascending=True).index.tolist())

    col_e, col_f = st.columns(2)
    with col_e:
        pivot_prime = (geo_prod.pivot(index="region_nom", columns="produit",
                                      values="total_premium").fillna(0).astype(int))
        fig = px.imshow(pivot_prime, color_continuous_scale="Greens", aspect="auto",
                        text_auto=".2s",
                        labels={"color": "Premium (€)", "x": "Product", "y": "Region"},
                        title="Annual Premium Volume (Region x Product)")
        fig.update_layout(height=420, margin=dict(t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_f:
        fig = px.bar(geo_prod, x="total_premium", y="region_nom",
                     color="produit", orientation="h", barmode="stack",
                     category_orders={"region_nom": reg_order_prod},
                     labels={"total_premium": "Annual Premium (€)", "region_nom": "",
                             "produit": "Product"},
                     title="Premiums by Region, Breakdown by Product")
        fig.update_layout(height=420, margin=dict(t=40, b=0), legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Loss Ratio by Region and Product")
    geo_lr = (
        claims.groupby(["region_nom", "produit"])
        .agg(losses=("montant_declare", "sum"), premiums=("prime_annuelle", "sum"))
        .assign(lr=lambda d: d["losses"] / d["premiums"].replace(0, float("nan")) * 100)
        .reset_index().dropna(subset=["lr"])
    )
    pivot_lr = geo_lr.pivot(index="region_nom", columns="produit", values="lr").fillna(float("nan")).round(1)
    fig = px.imshow(pivot_lr,
                    color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444"],
                    aspect="auto", text_auto=".1f", zmin=0, zmax=150,
                    labels={"color": "Loss Ratio (%)", "x": "Product", "y": "Region"},
                    title="Loss Ratio % (Region x Product) — red = high risk")
    fig.update_layout(height=420, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — Churn
# ══════════════════════════════════════════════════════════════════════════════

with tab6:
    churn = _add_region_nom(churn_raw.copy(), INSEE)

    avg_churn  = float(churn["churn_score"].mean()) if len(churn) else 0.0
    n_critique = int((churn["churn_segment"] == "critique").sum())

    c1, c2, c3 = st.columns(3)
    c1.metric("Scored Clients",    f"{len(churn):,}")
    c2.metric("Avg Churn Score",   f"{avg_churn:.1f} / 100")
    c3.metric("Critical Segment",  f"{n_critique:,}",
              f"{n_critique / len(churn) * 100:.1f}%" if len(churn) else "-")

    st.divider()
    SEG_COLORS = {"critique": "#7c3aed", "eleve": "#ef4444",
                  "moyen": "#f59e0b",    "faible": "#22c55e"}

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Churn Segment Distribution")
        seg_df = churn["churn_segment"].value_counts().reset_index()
        seg_df.columns = ["segment", "nb"]
        fig = px.pie(seg_df, values="nb", names="segment", hole=0.45,
                     color="segment", color_discrete_map=SEG_COLORS)
        fig.update_layout(height=320, margin=dict(t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Avg Churn Score by Client Segment")
        seg_score = (
            churn.groupby("segment")["churn_score"].mean()
            .reset_index().rename(columns={"churn_score": "avg_score"})
            .sort_values("avg_score", ascending=False)
        )
        fig = px.bar(seg_score, x="segment", y="avg_score",
                     color="avg_score", color_continuous_scale="RdYlGn_r",
                     text=seg_score["avg_score"].map(lambda x: f"{x:.1f}"),
                     labels={"segment": "Client Segment", "avg_score": "Avg Churn Score"})
        fig.update_layout(height=320, margin=dict(t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Geographic Churn Analysis")
    col_c, col_d = st.columns(2)

    with col_c:
        churn_reg = (
            churn.groupby("region_nom")["churn_score"].mean()
            .reset_index(name="avg_score").sort_values("avg_score", ascending=True)
        )
        fig = px.bar(churn_reg, x="avg_score", y="region_nom", orientation="h",
                     color="avg_score", color_continuous_scale="RdYlGn_r",
                     text=churn_reg["avg_score"].map(lambda x: f"{x:.1f}"),
                     labels={"region_nom": "", "avg_score": "Avg Churn Score"},
                     title="Avg Churn Score by Region")
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=40, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_d:
        crit_reg = (
            churn.groupby("region_nom")
            .apply(lambda g: (g["churn_segment"] == "critique").sum() / len(g) * 100)
            .reset_index(name="pct_critical").sort_values("pct_critical", ascending=True)
        )
        fig = px.bar(crit_reg, x="pct_critical", y="region_nom", orientation="h",
                     color="pct_critical", color_continuous_scale="Purples",
                     text=crit_reg["pct_critical"].map(lambda x: f"{x:.1f}%"),
                     labels={"region_nom": "", "pct_critical": "% Critical Clients"},
                     title="% Clients in Critical Segment by Region")
        fig.update_traces(textposition="outside")
        fig.update_layout(height=420, margin=dict(t=40, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Churn Score Distribution by Segment")
    fig = px.box(churn, x="churn_segment", y="churn_score",
                 color="churn_segment", color_discrete_map=SEG_COLORS,
                 category_orders={"churn_segment": ["faible", "moyen", "eleve", "critique"]},
                 labels={"churn_segment": "Segment", "churn_score": "Churn Score"})
    fig.update_layout(height=300, showlegend=False, margin=dict(t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)

# ── Footer ─────────────────────────────────────────────────────────────────────

st.markdown(
    "<div style='margin-top:40px;text-align:center;font-size:12px;color:#9ca3af'>"
    "AXA Claims Lakehouse &nbsp;·&nbsp; PySpark 3.5.1 &nbsp;·&nbsp; Delta Lake 3.1.0 "
    "&nbsp;·&nbsp; Developed by <b>Oswald Jaures KOFFI</b>"
    "</div>",
    unsafe_allow_html=True,
)
