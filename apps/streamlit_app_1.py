"""
Streamlit dashboard to visualize NYC 311 data from the Gold layer.

This application connects to a Spark session to read aggregated data
from Delta tables and presents it as interactive charts. It showcases:
- Top complaint types over time.
- Complaint volume by borough over time.
"""
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import streamlit as st
from constant import constants
from utils.spark_utils import get_spark_session

def plot_top_n_chart(st, spark, path: str, group_by_cols: list, pivot_col: str, title: str):
    """
    Reads a gold Delta table, calculates the top 3 items per year, and plots a monthly bar chart.

    This function encapsulates the logic to:
    1. Read aggregated data from a specified Delta Lake path.
    2. Determine the top 3 entities (e.g., complaint types, boroughs) for each year based on total count.
    3. Filter the monthly data to include only these top 3 entities.
    4. Pivot the data to a suitable format for charting.
    5. Display a bar chart in the Streamlit app.

    Args:
        st: The Streamlit module object.
        spark: The active SparkSession.
        path: Path to the gold Delta table.
        group_by_cols: Columns to group by for yearly ranking (e.g., ["year", "complaint_type"]).
        pivot_col: The column to use for pivoting the data for the chart (e.g., "complaint_type").
        title: The title for the Streamlit subheader.
    """
    st.subheader(title)
    if not os.path.exists(path):
        st.warning(f"Gold data not found at `{path}`. Please run the Gold layer processing first.")
        return

    df = spark.read.format("delta").load(path)

    if df.rdd.isEmpty():
        st.info(f"No data available in the '{os.path.basename(path)}' table to display.")
        return

    # Create a 'year_month' column for time-series plotting (e.g., "2023-01").
    df = df.withColumn(
        "year_month",
        F.concat_ws("-", df["year"].cast("string"), F.lpad(df["month"].cast("string"), 2, "0"))
    )

    # Step 1: Aggregate total counts per item per year to find the top items annually.
    yearly_totals_df = df.groupBy(*group_by_cols).agg(F.sum("count").alias("total_count"))

    # Step 2: Use a window function to rank items within each year and filter for the top 3.
    window_spec = Window.partitionBy("year").orderBy(F.desc("total_count"))
    top_n_ranked_df = (
        yearly_totals_df.withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 3)
        .select(*group_by_cols)
    )

    # Step 3: Join back with the original monthly data to retain only the top items for each year.
    filtered_df = df.join(top_n_ranked_df, on=group_by_cols)

    # Step 4: Aggregate monthly counts for the top items and convert to Pandas for plotting.
    pandas_df = (
        filtered_df.groupBy("year_month", pivot_col)
        .agg(F.sum("count").alias("count"))
        .orderBy("year_month")
        .toPandas()
    )

    # Step 5: Pivot the data to get it into the right shape for a multi-bar chart,
    # with each item as a separate column.
    pivot_df = pandas_df.pivot_table(
        index="year_month",
        columns=pivot_col,
        values="count",
        fill_value=0
    )

    # Step 6: Plot the final bar chart in Streamlit.
    st.bar_chart(pivot_df)

def main() -> None:
    """The main entry point for the Streamlit dashboard application."""
    st.set_page_config(page_title="NYC 311 Analysis", layout="wide")

    # Initialize SparkSession
    spark = get_spark_session(constants.STREAMLIT_APP_1_NAME)

    st.title("🗽 NYC 311 Service Request Analysis")
    st.write(
        "This dashboard visualizes aggregated data from the NYC 311 dataset, "
        "processed through a Delta Lake pipeline. The charts below show the top 3 "
        "items per year, trended by month."
    )

    # --- Top Complaint Types Chart ---
    plot_top_n_chart(
        st,
        spark,
        path=constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS,
        group_by_cols=["year", "complaint_type"],
        pivot_col="complaint_type",
        title="Top 3 Complaint Types by Month"
    )

    # --- Complaints by Borough Chart ---
    plot_top_n_chart(
        st,
        spark,
        path=constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH,
        group_by_cols=["year", "borough"],
        pivot_col="borough",
        title="Top 3 Boroughs by Complaint Volume"
    )

    spark.stop()


if __name__ == "__main__":
    main()
