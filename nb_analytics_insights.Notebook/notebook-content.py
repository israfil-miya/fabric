# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9d65ce6c-ba84-4650-9668-115a27ff14a7",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "14530c94-9dc4-4ef1-a320-081555064f5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "29b14a15-b502-4b45-8d4e-a0b8ee212b0f"
# META         },
# META         {
# META           "id": "9d65ce6c-ba84-4650-9668-115a27ff14a7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NYC Mobility, Air Quality & Macroeconomic Insights
# Interactive notebook visualizations over `lh_gold` and `lh_silver` answering the four key analytical questions and summarizing data quality.

# PARAMETERS CELL ********************

MONTHS_JSON = "[]"
SAVE_CHARTS_TO_ONELAKE = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import re
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

plt.rcParams["figure.facecolor"] = "white"
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.alpha"] = 0.25

CHART_DIR = "/lakehouse/default/Files/analytics/charts"


def resolve_months(months_json):
    parsed = json.loads(months_json)
    if not isinstance(parsed, list):
        raise ValueError("MONTHS_JSON must be a JSON array")
    if parsed:
        for m in parsed:
            if not isinstance(m, str) or not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", m):
                raise ValueError(f"Invalid month: {m}")
        return sorted(set(parsed))
    rows = (
        spark.table("dbo.fact_taxi_daily")
        .join(spark.table("dbo.dim_date").select("date_key", "year_month"), "date_key")
        .select("year_month")
        .distinct()
        .orderBy("year_month")
        .collect()
    )
    if not rows:
        raise ValueError("No months found in dbo.fact_taxi_daily")
    return [r["year_month"] for r in rows]


def save_chart(fig, filename):
    if not SAVE_CHARTS_TO_ONELAKE:
        return
    try:
        os.makedirs(CHART_DIR, exist_ok=True)
        out_path = os.path.join(CHART_DIR, filename)
        fig.savefig(out_path, dpi=160, bbox_inches="tight")
        print(f"Saved chart: {out_path}")
    except Exception as exc:
        print(f"Chart file save skipped ({exc}); displaying inline only.")


MONTHS = resolve_months(MONTHS_JSON)
print("Analyzing months:", MONTHS)

dim_date = spark.table("dbo.dim_date").filter(F.col("year_month").isin(MONTHS))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Section 1: Mobility Dashboard — Daily Trips, Revenue (Gross vs Net), Zero-Distance Share & Top Zones

taxi_daily_pdf = (
    spark.table("dbo.fact_taxi_daily")
    .join(dim_date.select("date_key", "full_date", "day_name", "is_weekend", "year_month"), "date_key")
    .orderBy("full_date")
    .toPandas()
)
taxi_daily_pdf["full_date"] = pd.to_datetime(taxi_daily_pdf["full_date"])
taxi_daily_pdf["trips_7d_ma"] = taxi_daily_pdf["trip_count"].rolling(7, min_periods=1).mean()

top_zones_pdf = (
    spark.table("dbo.fact_taxi_hourly_zone").alias("hz")
    .join(dim_date.select("date_key"), "date_key")
    .join(
        spark.table("dbo.dim_taxi_zone").alias("z"),
        F.col("hz.PULocationID") == F.col("z.location_id"),
        "left",
    )
    .groupBy("hz.PULocationID", "z.borough", "z.zone_name")
    .agg(
        F.sum("hz.trip_count").alias("total_trips"),
        F.round(F.sum("hz.total_amount_usd"), 2).alias("gross_revenue_usd"),
    )
    .orderBy(F.desc("total_trips"))
    .limit(12)
    .toPandas()
)
top_zones_pdf["label"] = (
    top_zones_pdf["zone_name"].fillna("Unknown") + " (" + top_zones_pdf["borough"].fillna("?") + ")"
)

total_trips = int(taxi_daily_pdf["trip_count"].sum())
zero_dist_trips = int(taxi_daily_pdf["zero_distance_trips"].sum())
gross_rev = float(taxi_daily_pdf["total_amount_usd"].sum())
net_rev = float(taxi_daily_pdf["net_total_amount_usd"].sum())
print(
    f"Mobility summary ({MONTHS[0]}..{MONTHS[-1]}): total_trips={total_trips:,} | "
    f"zero_distance={zero_dist_trips:,} ({zero_dist_trips * 100.0 / total_trips:.2f}%) | "
    f"gross_nonnegative=${gross_rev:,.2f} | signed_net=${net_rev:,.2f}"
)

fig, axes = plt.subplots(1, 3, figsize=(20, 5.5))

axes[0].plot(taxi_daily_pdf["full_date"], taxi_daily_pdf["trip_count"] / 1000.0, alpha=0.45, label="Daily trips")
axes[0].plot(taxi_daily_pdf["full_date"], taxi_daily_pdf["trips_7d_ma"] / 1000.0, linewidth=2.2, label="7-day avg")
axes[0].set_title("Daily NYC Yellow Taxi Trips (Thousands)")
axes[0].set_ylabel("Trips (k)")
axes[0].xaxis.set_major_locator(mdates.MonthLocator())
axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
axes[0].legend()

axes[1].plot(taxi_daily_pdf["full_date"], taxi_daily_pdf["total_amount_usd"] / 1e6, label="Gross nonnegative USD")
axes[1].plot(taxi_daily_pdf["full_date"], taxi_daily_pdf["net_total_amount_usd"] / 1e6, linestyle="--", label="Signed net USD")
axes[1].set_title("Daily Taxi Revenue: Gross Nonnegative vs. Signed Net")
axes[1].set_ylabel("USD (Millions)")
axes[1].xaxis.set_major_locator(mdates.MonthLocator())
axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
axes[1].legend()

top_plot = top_zones_pdf.iloc[::-1]
axes[2].barh(top_plot["label"], top_plot["total_trips"] / 1e3, color="#2a6f97")
axes[2].set_title("Top 12 Pickup Zones by Trip Volume")
axes[2].set_xlabel("Trips (Thousands)")

fig.tight_layout()
save_chart(fig, "01_mobility_overview.png")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Section 2: Air Quality Dashboard — 8-Monitor Network PM2.5 & Sensor Coverage

aq_citywide_pdf = (
    spark.table("dbo.fact_air_quality_daily_citywide")
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
    .orderBy("local_date")
    .toPandas()
)
aq_citywide_pdf["local_date"] = pd.to_datetime(aq_citywide_pdf["local_date"])

sensor_cov_pdf = (
    spark.table("dbo.fact_air_quality_daily_sensor").alias("f")
    .filter(F.date_format("f.local_date", "yyyy-MM").isin(MONTHS))
    .join(spark.table("dbo.dim_air_quality_sensor").alias("s"), "sensor_id")
    .groupBy("f.sensor_id", "s.location_name")
    .agg(
        F.sum("f.valid_hour_count").alias("valid_hours"),
        F.sum("f.expected_hour_count").alias("expected_hours"),
        F.sum("f.quarantined_hour_count").alias("quarantined_hours"),
        F.round(F.avg("f.avg_pm25_unflagged"), 2).alias("mean_pm25"),
    )
    .withColumn("valid_coverage_pct", F.round(F.col("valid_hours") * 100.0 / F.col("expected_hours"), 2))
    .orderBy("f.sensor_id")
    .toPandas()
)
sensor_cov_pdf["label"] = sensor_cov_pdf["sensor_id"].astype(str) + " - " + sensor_cov_pdf["location_name"]

reliable_days = int(aq_citywide_pdf["network_mean_pm25"].notna().sum())
total_days = int(len(aq_citywide_pdf))
print(
    f"Air quality network summary: {reliable_days}/{total_days} days met >=4 eligible sensors at >=75% coverage; "
    f"mean network PM2.5 = {aq_citywide_pdf['network_mean_pm25'].mean():.2f} ug/m3"
)

fig, axes = plt.subplots(1, 2, figsize=(17, 5.5))

axes[0].fill_between(
    aq_citywide_pdf["local_date"],
    aq_citywide_pdf["min_sensor_avg_pm25"],
    aq_citywide_pdf["max_sensor_avg_pm25"],
    alpha=0.2,
    color="#e76f51",
    label="Eligible sensor min-max range",
)
axes[0].plot(
    aq_citywide_pdf["local_date"],
    aq_citywide_pdf["network_mean_pm25"],
    color="#d62828",
    linewidth=2.0,
    label="Network mean PM2.5",
)
axes[0].plot(
    aq_citywide_pdf["local_date"],
    aq_citywide_pdf["network_median_pm25"],
    color="#457b9d",
    linestyle="--",
    linewidth=1.5,
    label="Network median PM2.5",
)
axes[0].set_title("Selected 8-Monitor Network Daily PM2.5 (ug/m3)\n(Gaps = days below 4-sensor / 75% eligibility threshold)")
axes[0].set_ylabel("PM2.5 (ug/m3)")
axes[0].xaxis.set_major_locator(mdates.MonthLocator())
axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
axes[0].legend()

cov_plot = sensor_cov_pdf.sort_values("valid_coverage_pct")
axes[1].barh(cov_plot["label"], cov_plot["valid_coverage_pct"], color="#2a9d8f")
axes[1].axvline(75.0, color="#e63946", linestyle="--", label="75% daily eligibility threshold")
axes[1].set_xlim(0, 105)
axes[1].set_title("Valid Unflagged Hour Coverage (%) by AirNow Sensor")
axes[1].set_xlabel("Coverage (%)")
axes[1].legend()

fig.tight_layout()
save_chart(fig, "02_air_quality_overview.png")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Section 3: Key Questions #1 & #2 — Mobility vs. Air Quality (Daily & Hourly Co-Movement)

combined_pdf = (
    spark.table("dbo.fact_taxi_air_quality_daily")
    .join(dim_date.select("date_key", "full_date", "is_weekend"), "date_key")
    .orderBy("full_date")
    .toPandas()
)
reliable_combined = combined_pdf.dropna(subset=["network_mean_pm25"]).copy()
pearson_r = reliable_combined["trip_count"].corr(reliable_combined["network_mean_pm25"], method="pearson")
spearman_r = reliable_combined["trip_count"].corr(reliable_combined["network_mean_pm25"], method="spearman")
print(
    f"Daily correlation across {len(reliable_combined)} reliable days: "
    f"Pearson r = {pearson_r:.3f}, Spearman rho = {spearman_r:.3f}"
)

taxi_hourly_pdf = (
    spark.table("dbo.fact_taxi_hourly_zone")
    .join(dim_date.select("date_key"), "date_key")
    .groupBy("date_key", "pickup_hour")
    .agg(F.sum("trip_count").alias("hourly_trips"))
    .groupBy("pickup_hour")
    .agg(F.avg("hourly_trips").alias("avg_hourly_trips"))
    .orderBy("pickup_hour")
    .toPandas()
)

aq_hourly_pdf = (
    spark.table("dbo.fact_air_quality_hourly_citywide")
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
    .groupBy("local_hour")
    .agg(
        F.avg("hourly_network_mean_pm25").alias("avg_hourly_pm25"),
        F.count("hourly_network_mean_pm25").alias("reliable_hours"),
    )
    .orderBy("local_hour")
    .toPandas()
)

hourly_merged = pd.merge(taxi_hourly_pdf, aq_hourly_pdf, left_on="pickup_hour", right_on="local_hour", how="inner")

fig, axes = plt.subplots(1, 2, figsize=(17, 5.5))

wkday = reliable_combined[~reliable_combined["is_weekend"]]
wkend = reliable_combined[reliable_combined["is_weekend"]]
axes[0].scatter(wkday["trip_count"] / 1e3, wkday["network_mean_pm25"], alpha=0.75, label="Weekday", color="#1d3557")
axes[0].scatter(wkend["trip_count"] / 1e3, wkend["network_mean_pm25"], alpha=0.75, label="Weekend", color="#e76f51")
axes[0].set_title(f"Daily Taxi Volume vs. Network Mean PM2.5\n(n={len(reliable_combined)} reliable days, Pearson r={pearson_r:.3f})")
axes[0].set_xlabel("Daily Trips (Thousands)")
axes[0].set_ylabel("Network Mean PM2.5 (ug/m3)")
axes[0].legend()

ax_left = axes[1]
ax_right = ax_left.twinx()
l1 = ax_left.plot(
    hourly_merged["pickup_hour"],
    hourly_merged["avg_hourly_trips"] / 1e3,
    color="#264653",
    marker="o",
    linewidth=2.0,
    label="Avg hourly taxi trips (k)",
)
l2 = ax_right.plot(
    hourly_merged["pickup_hour"],
    hourly_merged["avg_hourly_pm25"],
    color="#d62828",
    marker="s",
    linestyle="--",
    linewidth=2.0,
    label="Avg hourly network PM2.5 (ug/m3)",
)
ax_left.set_title("Diurnal Profile: Taxi Demand vs. Network PM2.5 by Hour\n(8-monitor network estimate; does not imply zone-level causality)")
ax_left.set_xlabel("Local Hour (America/New_York)")
ax_left.set_xticks(range(0, 24, 2))
ax_left.set_ylabel("Avg Hourly Taxi Trips (Thousands)")
ax_right.set_ylabel("Avg Network PM2.5 (ug/m3)")
lines = l1 + l2
ax_left.legend(lines, [l.get_label() for l in lines], loc="upper left")

fig.tight_layout()
save_chart(fig, "03_mobility_vs_air_quality.png")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Section 4: Key Questions #3 & #4 — Economics: USD vs. EUR Revenue, FX Carry-Forward & Annual GDP

econ_daily_pdf = (
    spark.table("dbo.fact_taxi_daily").alias("t")
    .join(dim_date.select("date_key", "full_date", "year_month").alias("d"), "date_key")
    .join(
        spark.table("dbo.fact_fx_daily").filter(F.col("currency_pair") == "EUR/USD").alias("fx"),
        "date_key",
    )
    .select(
        F.col("d.full_date").alias("full_date"),
        F.col("d.year_month").alias("year_month"),
        F.col("t.revenue_trip_count"),
        F.col("t.total_amount_usd"),
        (F.col("t.total_amount_usd") * F.col("fx.eur_per_usd_rate")).alias("total_amount_eur"),
        (F.col("t.total_amount_usd") / F.col("t.revenue_trip_count")).alias("avg_rev_per_trip_usd"),
        (
            (F.col("t.total_amount_usd") * F.col("fx.eur_per_usd_rate")) / F.col("t.revenue_trip_count")
        ).alias("avg_rev_per_trip_eur"),
        F.col("fx.usd_per_eur_rate"),
        F.col("fx.is_carry_forward_rate"),
    )
    .orderBy("full_date")
    .toPandas()
)
econ_daily_pdf["full_date"] = pd.to_datetime(econ_daily_pdf["full_date"])

gdp_pdf = (
    spark.table("dbo.fact_gdp_annual")
    .orderBy("year", "country_iso3")
    .toPandas()
)

print(
    f"Avg revenue per eligible trip: ${econ_daily_pdf['avg_rev_per_trip_usd'].mean():.2f} USD | "
    f"EUR {econ_daily_pdf['avg_rev_per_trip_eur'].mean():.2f} | "
    f"ECB rate range: {econ_daily_pdf['usd_per_eur_rate'].min():.4f}..{econ_daily_pdf['usd_per_eur_rate'].max():.4f} USD/EUR"
)

fig, axes = plt.subplots(1, 3, figsize=(20, 5.5))

axes[0].plot(econ_daily_pdf["full_date"], econ_daily_pdf["avg_rev_per_trip_usd"], label="USD / trip", color="#1d3557")
axes[0].plot(econ_daily_pdf["full_date"], econ_daily_pdf["avg_rev_per_trip_eur"], label="EUR / trip (ECB rate)", color="#2a9d8f")
axes[0].set_title("Daily Avg Revenue per Eligible Trip (USD vs. EUR)")
axes[0].set_ylabel("Amount per Trip")
axes[0].xaxis.set_major_locator(mdates.MonthLocator())
axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
axes[0].legend()

pub = econ_daily_pdf[~econ_daily_pdf["is_carry_forward_rate"]]
cf = econ_daily_pdf[econ_daily_pdf["is_carry_forward_rate"]]
axes[1].plot(econ_daily_pdf["full_date"], econ_daily_pdf["usd_per_eur_rate"], color="#6c757d", alpha=0.5)
axes[1].scatter(pub["full_date"], pub["usd_per_eur_rate"], s=14, color="#264653", label="Published business day")
axes[1].scatter(cf["full_date"], cf["usd_per_eur_rate"], s=14, color="#f4a261", label="Weekend/holiday carry-forward")
axes[1].set_title("ECB Daily Exchange Rate (USD per 1 EUR)")
axes[1].set_ylabel("USD per 1 EUR")
axes[1].xaxis.set_major_locator(mdates.MonthLocator())
axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
axes[1].legend()

for iso, grp in gdp_pdf.groupby("country_iso3"):
    axes[2].plot(grp["year"], grp["gdp_usd"] / 1e12, marker="o", linewidth=2.0, label=f"{iso} ({grp['country_name'].iloc[0]})")
axes[2].set_title("World Bank Annual GDP Context (NY.GDP.MKTP.CD)\n(Long-run macro backdrop; 5-mo taxi/AQ window is short-term)")
axes[2].set_ylabel("GDP (Trillions USD)")
axes[2].set_xlabel("Year")
axes[2].legend()

fig.tight_layout()
save_chart(fig, "04_economics_fx_and_gdp.png")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Section 5: Data Quality & Reconciliation Audit Summary

taxi_audit_pdf = (
    spark.table("lh_silver.dbo.silver_taxi_quality_audit")
    .filter(F.col("source_year_month").isin(MONTHS))
    .orderBy("source_year_month")
    .toPandas()
)
aq_audit_pdf = (
    spark.table("lh_silver.dbo.silver_air_quality_quality_audit")
    .filter(F.col("local_year_month").isin(MONTHS))
    .orderBy("local_year_month")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 5.0))

x = range(len(taxi_audit_pdf))
axes[0].bar(x, taxi_audit_pdf["silver_zero_distance_count"] / 1e3, label="Zero distance")
axes[0].bar(
    x,
    taxi_audit_pdf["silver_negative_amount_count"] / 1e3,
    bottom=taxi_audit_pdf["silver_zero_distance_count"] / 1e3,
    label="Negative fare/total",
)
axes[0].bar(
    x,
    (taxi_audit_pdf["silver_unusual_distance_gt_100_count"] + taxi_audit_pdf["silver_unusual_duration_gt_480_count"]) / 1e3,
    bottom=(taxi_audit_pdf["silver_zero_distance_count"] + taxi_audit_pdf["silver_negative_amount_count"]) / 1e3,
    label="Unusual distance (>100mi) / duration (>8h)",
)
axes[0].bar(
    x,
    taxi_audit_pdf["quarantine_total_count"] / 1e3,
    bottom=(
        taxi_audit_pdf["silver_zero_distance_count"]
        + taxi_audit_pdf["silver_negative_amount_count"]
        + taxi_audit_pdf["silver_unusual_distance_gt_100_count"]
        + taxi_audit_pdf["silver_unusual_duration_gt_480_count"]
    ) / 1e3,
    label="Quarantined (invalid timeline / out-of-month)",
)
axes[0].set_xticks(list(x))
axes[0].set_xticklabels(taxi_audit_pdf["source_year_month"])
axes[0].set_title("NYC Taxi Preserved Quality Flags & Quarantine (Thousands)")
axes[0].set_ylabel("Records (Thousands)")
axes[0].legend()

x2 = range(len(aq_audit_pdf))
axes[1].bar(x2, aq_audit_pdf["accepted_count"], label="Accepted (CONFIRMED_UNFLAGGED)", color="#2a9d8f")
axes[1].bar(x2, aq_audit_pdf["quarantined_count"], bottom=aq_audit_pdf["accepted_count"], label="Quarantined (FLAGGED / MISSING / UNKNOWN)", color="#e76f51")
axes[1].set_xticks(list(x2))
axes[1].set_xticklabels(aq_audit_pdf["local_year_month"])
axes[1].set_title("OpenAQ Hourly Disposition by Month (diff = 0)")
axes[1].set_ylabel("Hourly Sensor Observations")
axes[1].legend()

fig.tight_layout()
save_chart(fig, "05_data_quality_audit.png")
plt.show()

print("Taxi Reconciliation Diffs:", dict(zip(taxi_audit_pdf["source_year_month"], taxi_audit_pdf["reconciliation_diff"])))
print("OpenAQ Reconciliation Diffs:", dict(zip(aq_audit_pdf["local_year_month"], aq_audit_pdf["reconciliation_diff"])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
