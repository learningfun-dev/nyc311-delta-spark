'''
    Streamlit Dashboard
'''
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.pip_utils import configure_spark_with_delta_pip
import streamlit as st
from constant import constants

def main():
    '''
        the main entry point for the application
    '''

    # Initialize SparkSession
    spark = (
    SparkSession
    .builder.master("spark://localhost:7077")
    .appName(constants.STREAMLIT_APP_NAME)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    log_step = """ ********************************************************************************
            4.Streamlit Dashboard
            ********************************************************************************
        """
    print(log_step)

    st.title("NYC 311 Complaints Dashboard")

    # Top complaint bar chart
    st.subheader("Top 3 Complaint Types per Month")
    if not os.path.exists(constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH):
        st.write("Gold Delta lake for Top Complaint Types doesn't exists.")
    else:
        top_complaints_df = ( spark.read
            .format("delta")
            .load(constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS)
        )

        if top_complaints_df.rdd.isEmpty():
            st.write("No data available for Top Complaint Types.")
        else:
            # Add year_month column
            top_complaints_df = top_complaints_df.withColumn(
                "year_month",
                F.concat_ws("-", top_complaints_df["year"].cast("string"), F.lpad(top_complaints_df["month"].cast("string"), 2, "0"))
            )

            # Step 1: Aggregate total counts per complaint_type per year
            yearly_totals_df = (
                                top_complaints_df.groupBy("year", "complaint_type")
                                    .agg(F.sum("count").alias("total_count"))
                                )

            # Step 2: Window function to get top 3 complaint types per year
            window_spec = Window.partitionBy("year").orderBy(F.desc("total_count"))
            top_complaints_df_ranked = (yearly_totals_df.withColumn("rank"
                                                        ,F.row_number().over(window_spec))
                                                        .filter(F.col("rank") <= 3)
                                                        .select("year", "complaint_type"))

            # Step 3: Join to filter original monthly data
            filtered_df = top_complaints_df.join(top_complaints_df_ranked, on=["year", "complaint_type"])

            # Step 4: Convert to Pandas for Streamlit plotting
            filtered_pd = filtered_df.groupBy("year_month", "complaint_type") \
                .agg(F.sum("count").alias("count")) \
                .orderBy("year_month") \
                .toPandas()

            # Step 5: Pivot for bar chart
            pivot_df = filtered_pd.pivot_table(
                index="year_month",
                columns="complaint_type",
                values="count",
                fill_value=0
            )

            # Step 6: Plot
            st.bar_chart(pivot_df)

    st.subheader("Top 3 Borough Complaints by Month")
    if not os.path.exists(constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH):
        st.write("Gold Delta lake for Complaints by Borough doesn't exists.")
    else:
        # Visualize Complaints by Borough
        by_borough_df = ( spark.read
            .format("delta")
            .load(constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH)
        )
       # Add year_month column
        by_borough_df = by_borough_df.withColumn(
                "year_month",
                F.concat_ws("-", by_borough_df["year"].cast("string"), F.lpad(by_borough_df["month"].cast("string"), 2, "0"))
            )

        # Step 1: Aggregate total counts per borough per year
        yearly_totals_df = by_borough_df.groupBy("year", "borough") \
            .agg(F.sum("count").alias("total_count"))

        # Step 2: Window function to get top 3 complaint types per year
        window_spec = Window.partitionBy("year").orderBy(F.desc("total_count"))
        top_borough_df_ranked = (yearly_totals_df.withColumn("rank"
                                                        , F.row_number().over(window_spec))
                                                        .filter(F.col("rank") <= 3)
                                                        .select("year", "borough"))

        # Step 3: Join to filter original monthly data
        filtered_df = by_borough_df.join(top_borough_df_ranked
                                                 , on=["year", "borough"])

        # Step 4: Convert to Pandas for Streamlit plotting
        filtered_pd = ( 
                filtered_df.groupBy("year_month", "borough")
                .agg(F.sum("count").alias("count"))
                .orderBy("year_month")
                .toPandas()
        )

        # Step 5: Pivot for bar chart
        pivot_df = filtered_pd.pivot_table(
            index="year_month",
            columns="borough",
            values="count",
            fill_value=0
        )

        # Step 6: Plot
        st.bar_chart(pivot_df)
        # Stop the SparkSession

    spark.stop()


if __name__ == "__main__":
    main()
