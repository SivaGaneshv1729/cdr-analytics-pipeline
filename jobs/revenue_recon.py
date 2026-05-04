import argparse
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    input_path = "file:///data/cdr_data.csv"
    output_path = f"file:///output/revenue_reconciliation/{run_id}"

    spark = SparkSession.builder.appName("revenue_recon").getOrCreate()

    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_record_count = df.count()

    # Calculate total revenue
    result_df = df.agg(spark_sum("charge_amount").alias("total_revenue"))

    output_record_count = 1

    # Write output as CSV
    result_df.write.csv(output_path, header=False, mode="overwrite")

    # Generate Manifest
    manifest = {
        "job_name": "revenue_reconciliation",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": input_record_count,
        "output_record_count": output_record_count,
        "status": "SUCCESS"
    }

    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    
    fs = FileSystem.get(Configuration())
    manifest_path = Path(f"{output_path}/_MANIFEST.json")
    out = fs.create(manifest_path)
    out.write(bytearray(json.dumps(manifest, indent=2), "utf-8"))
    out.close()

    spark.stop()

if __name__ == "__main__":
    main()
