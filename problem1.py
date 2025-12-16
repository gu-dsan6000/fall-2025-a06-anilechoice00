#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
Analyzes the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, lit
import sys


def create_spark_session(master_url, net_id):
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("Problem1-LogLevelDistribution") \
        .master(master_url) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze log level distribution')
    parser.add_argument('master', help='Spark master URL (e.g., spark://172.31.x.x:7077)')
    parser.add_argument('--net-id', required=True, help='Your net ID for S3 bucket')
    args = parser.parse_args()

    # Create Spark session
    print(f"Creating Spark session with master: {args.master}")
    spark = create_spark_session(args.master, args.net_id)

    try:
        # Construct S3 path
        s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"
        print(f"Reading log files from: {s3_path}")

        # Read all log files
        logs_df = spark.read.text(s3_path)

        # Cache the data for better performance
        logs_df.cache()

        # Count total lines
        total_lines = logs_df.count()
        print(f"Total log lines read: {total_lines:,}")

        # Extract log levels using regex
        # Pattern matches: INFO, WARN, ERROR, or DEBUG
        logs_parsed = logs_df.select(
            col('value').alias('log_entry'),
            regexp_extract('value', r'\s(INFO|WARN|ERROR|DEBUG)\s', 1).alias('log_level')
        )

        # Filter out lines without log levels
        logs_with_levels = logs_parsed.filter(col('log_level') != '')

        # Count lines with log levels
        total_with_levels = logs_with_levels.count()
        print(f"Total lines with log levels: {total_with_levels:,}")

        # 1. Generate counts by log level
        print("\nGenerating log level counts...")
        level_counts = logs_with_levels.groupBy('log_level') \
            .agg(count('*').alias('count')) \
            .orderBy(col('count').desc())

        # Save counts to CSV
        level_counts.coalesce(1).write.mode('overwrite').csv(
            'problem1_counts_temp', header=True
        )

        # Move the CSV file to the correct location
        import os
        import glob
        temp_files = glob.glob('problem1_counts_temp/part-*.csv')
        if temp_files:
            os.rename(temp_files[0], 'problem1_counts.csv')
            os.system('rm -rf problem1_counts_temp')

        # 2. Generate random sample
        print("Generating random sample...")
        sample_df = logs_with_levels.sample(False, 0.001, seed=42).limit(10)

        sample_df.coalesce(1).write.mode('overwrite').csv(
            'problem1_sample_temp', header=True
        )

        temp_files = glob.glob('problem1_sample_temp/part-*.csv')
        if temp_files:
            os.rename(temp_files[0], 'problem1_sample.csv')
            os.system('rm -rf problem1_sample_temp')

        # 3. Generate summary statistics
        print("Generating summary statistics...")

        # Collect counts for summary
        counts_data = level_counts.collect()
        unique_levels = len(counts_data)

        # Calculate percentages
        summary_lines = []
        summary_lines.append(f"Total log lines processed: {total_lines:,}")
        summary_lines.append(f"Total lines with log levels: {total_with_levels:,}")
        summary_lines.append(f"Unique log levels found: {unique_levels}")
        summary_lines.append("")
        summary_lines.append("Log level distribution:")

        for row in counts_data:
            level = row['log_level']
            cnt = row['count']
            percentage = (cnt / total_with_levels) * 100
            summary_lines.append(f"  {level:8s}: {cnt:10,} ({percentage:6.2f}%)")

        # Write summary to file
        with open('problem1_summary.txt', 'w') as f:
            f.write('\n'.join(summary_lines))

        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        for line in summary_lines:
            print(line)
        print("="*60)

        print("\n✓ Problem 1 completed successfully!")
        print("Generated files:")
        print("  - problem1_counts.csv")
        print("  - problem1_sample.csv")
        print("  - problem1_summary.txt")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
