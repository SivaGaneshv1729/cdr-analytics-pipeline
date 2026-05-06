import argparse
import json
import datetime
import math
from collections import defaultdict
from pyspark.sql import SparkSession

def custom_partitioner(key):
    # Simple hash partitioner based on string key
    return hash(key) % 10

def detect_anomalies(iterator):
    # Process all records in this partition
    # Because of custom_partitioner, all records for a given caller_id are guaranteed
    # to be in the same partition.
    records_by_caller = defaultdict(list)
    
    for caller_id, record in iterator:
        records_by_caller[caller_id].append(record)
        
    for caller_id, records in records_by_caller.items():
        n = len(records)
        if n <= 1:
            continue
            
        total_duration = sum(r['duration_sec'] for r in records)
        mean_duration = total_duration / n
        
        # Calculate population standard deviation
        variance = sum((r['duration_sec'] - mean_duration) ** 2 for r in records) / n
        stddev = math.sqrt(variance)
        
        # Standard deviation could be 0 if all calls have exact same duration
        if stddev == 0:
            continue
            
        for r in records:
            if abs(r['duration_sec'] - mean_duration) > 3 * stddev:
                # Output schema: caller_id,call_timestamp,duration_sec,user_mean_duration,user_stddev
                yield f"{caller_id},{r['timestamp']},{r['duration_sec']},{mean_duration:.2f},{stddev:.2f}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    input_path = "file:///data/cdr_data.csv"
    output_path = f"hdfs://namenode:8020/output/anomalous_call_detection/{run_id}"

    spark = SparkSession.builder.appName("anomalous_calls").getOrCreate()
    sc = spark.sparkContext

    # Read CSV natively using DataFrames for optimal performance
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_record_count = df.count()

    # Convert to Key-Value RDD for the Custom Partitioner
    parsed_rdd = df.rdd.map(
        lambda row: (row["caller_id"], {"duration_sec": row["duration_sec"], "timestamp": row["timestamp"]})
    )

    # Apply Custom Partitioner
    # 10 partitions
    partitioned_rdd = parsed_rdd.partitionBy(10, custom_partitioner)

    # Process each partition to find anomalies
    anomalies_rdd = partitioned_rdd.mapPartitions(detect_anomalies)

    output_record_count = anomalies_rdd.count()

    # Write output
    anomalies_rdd.saveAsTextFile(output_path)

    # Generate Manifest
    manifest = {
        "job_name": "anomalous_call_detection",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": input_record_count,
        "output_record_count": output_record_count,
        "status": "SUCCESS"
    }

    # Write manifest
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
