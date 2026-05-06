import argparse
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    input_path = "file:///data/cdr_data.csv"
    output_path = f"hdfs://namenode:8020/output/top_callers_by_spend/{run_id}"

    spark = SparkSession.builder.appName("top_callers").getOrCreate()

    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_record_count = df.count()

    from pyspark.sql.functions import rand, ceil

    # Salting to handle data skew (distributes whale caller records across 10 reducers)
    df = df.withColumn("salt", ceil(rand() * 10))

    # Phase 1: Partial Aggregation (grouped by caller and salt)
    partial_df = df.groupBy("caller_id", "salt") \
        .agg(spark_sum("charge_amount").alias("partial_spend"))

    # Phase 2: Final Aggregation (grouped by caller only)
    result_df = partial_df.groupBy("caller_id") \
        .agg(spark_sum("partial_spend").alias("total_spend")) \
        .orderBy(col("total_spend").desc()) \
        .limit(100)

    output_record_count = result_df.count()

    # Write output as CSV
    result_df.write.csv(output_path, header=False, mode="overwrite")

    # Generate Manifest
    manifest = {
        "job_name": "top_callers_by_spend",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": input_record_count,
        "output_record_count": output_record_count,
        "status": "SUCCESS"
    }

    # Write manifest using JVM FileSystem
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    
    URI = sc._gateway.jvm.java.net.URI
    fs = FileSystem.get(URI.create(output_path), Configuration())
    manifest_path = Path(f"{output_path}/_MANIFEST.json")
    out = fs.create(manifest_path)
    out.write(bytearray(json.dumps(manifest, indent=2), "utf-8"))
    out.close()

    spark.stop()

if __name__ == "__main__":
    main()
