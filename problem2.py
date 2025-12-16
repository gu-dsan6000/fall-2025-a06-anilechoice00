#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
Analyzes cluster usage patterns to understand which clusters are most heavily used over time.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, min as spark_min, max as spark_max,
    count, to_timestamp, input_file_name
)
import sys
import os
import glob


def create_spark_session(master_url, net_id):
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("Problem2-ClusterUsageAnalysis") \
        .master(master_url) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def process_spark_data(spark, s3_path):
    """Process Spark logs and generate timeline and cluster summary data."""
    print(f"Reading log files from: {s3_path}")

    # Read all log files
    logs_df = spark.read.text(s3_path)

    # Add file path to extract application ID
    logs_df = logs_df.withColumn('file_path', input_file_name())

    # Extract application ID and cluster ID from file path
    # Path format: s3a://bucket/data/application_1485248649253_0052/container_...
    logs_df = logs_df.withColumn(
        'application_id',
        regexp_extract('file_path', r'application_(\d+_\d+)', 0)
    ).withColumn(
        'cluster_id',
        regexp_extract('file_path', r'application_(\d+)_\d+', 1)
    ).withColumn(
        'app_number',
        regexp_extract('file_path', r'application_\d+_(\d+)', 1)
    )

    # Extract timestamp from log lines
    # Log format: YY/MM/DD HH:MM:SS
    logs_df = logs_df.withColumn(
        'timestamp_str',
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', 1)
    )

    # Filter rows with valid timestamps
    logs_with_time = logs_df.filter(col('timestamp_str') != '')

    # Convert to timestamp
    logs_with_time = logs_with_time.withColumn(
        'timestamp',
        to_timestamp('timestamp_str', 'yy/MM/dd HH:mm:ss')
    )

    # Group by application to get start and end times
    print("Aggregating application timelines...")
    app_timeline = logs_with_time.groupBy('cluster_id', 'application_id', 'app_number') \
        .agg(
            spark_min('timestamp').alias('start_time'),
            spark_max('timestamp').alias('end_time')
        ) \
        .orderBy('cluster_id', 'app_number')

    # Generate cluster summary
    print("Generating cluster summary...")
    cluster_summary = app_timeline.groupBy('cluster_id') \
        .agg(
            count('application_id').alias('num_applications'),
            spark_min('start_time').alias('cluster_first_app'),
            spark_max('end_time').alias('cluster_last_app')
        ) \
        .orderBy(col('num_applications').desc())

    return app_timeline, cluster_summary


def save_csv(df, temp_name, final_name):
    """Save DataFrame to CSV with proper naming."""
    df.coalesce(1).write.mode('overwrite').csv(temp_name, header=True)
    temp_files = glob.glob(f'{temp_name}/part-*.csv')
    if temp_files:
        os.rename(temp_files[0], final_name)
        os.system(f'rm -rf {temp_name}')


def generate_statistics(cluster_summary_df):
    """Generate and save statistics text file."""
    # Collect data
    cluster_data = cluster_summary_df.collect()

    total_clusters = len(cluster_data)
    total_apps = sum(row['num_applications'] for row in cluster_data)
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

    lines = []
    lines.append(f"Total unique clusters: {total_clusters}")
    lines.append(f"Total applications: {total_apps}")
    lines.append(f"Average applications per cluster: {avg_apps:.2f}")
    lines.append("")
    lines.append("Most heavily used clusters:")

    for row in cluster_data:
        cluster_id = row['cluster_id']
        num_apps = row['num_applications']
        lines.append(f"  Cluster {cluster_id}: {num_apps} applications")

    # Write to file
    with open('problem2_stats.txt', 'w') as f:
        f.write('\n'.join(lines))

    print("\n" + "="*60)
    print("CLUSTER USAGE STATISTICS")
    print("="*60)
    for line in lines:
        print(line)
    print("="*60)


def generate_visualizations():
    """Generate visualizations using Seaborn."""
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns

    print("\nGenerating visualizations...")

    # Read the CSV files
    cluster_summary = pd.read_csv('problem2_cluster_summary.csv')
    timeline = pd.read_csv('problem2_timeline.csv')

    # Convert timestamps
    timeline['start_time'] = pd.to_datetime(timeline['start_time'])
    timeline['end_time'] = pd.to_datetime(timeline['end_time'])
    timeline['duration_seconds'] = (timeline['end_time'] - timeline['start_time']).dt.total_seconds()

    # 1. Bar Chart: Applications per cluster
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")

    ax = sns.barplot(
        data=cluster_summary,
        x='cluster_id',
        y='num_applications',
        hue='cluster_id',
        palette='viridis',
        legend=False
    )

    # Add value labels on bars
    for i, v in enumerate(cluster_summary['num_applications']):
        ax.text(i, v + 1, str(v), ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.title('Number of Applications per Cluster', fontsize=14, fontweight='bold')
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Applications', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('problem2_bar_chart.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated problem2_bar_chart.png")

    # 2. Density Plot: Job duration for largest cluster
    # Find the largest cluster
    largest_cluster = cluster_summary.loc[cluster_summary['num_applications'].idxmax(), 'cluster_id']
    largest_cluster_data = timeline[timeline['cluster_id'] == largest_cluster].copy()

    plt.figure(figsize=(12, 6))

    # Filter out zero or negative durations
    largest_cluster_data = largest_cluster_data[largest_cluster_data['duration_seconds'] > 0]

    # Create histogram with KDE
    ax = sns.histplot(
        data=largest_cluster_data,
        x='duration_seconds',
        kde=True,
        bins=30,
        color='steelblue',
        edgecolor='black',
        alpha=0.7
    )

    # Use log scale for x-axis to handle skewed data
    ax.set_xscale('log')

    plt.title(
        f'Job Duration Distribution for Largest Cluster (n={len(largest_cluster_data)})',
        fontsize=14,
        fontweight='bold'
    )
    plt.xlabel('Duration (seconds, log scale)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('problem2_density_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated problem2_density_plot.png")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze cluster usage patterns')
    parser.add_argument('master', nargs='?', default=None,
                       help='Spark master URL (e.g., spark://172.31.x.x:7077)')
    parser.add_argument('--net-id', help='Your net ID for S3 bucket')
    parser.add_argument('--skip-spark', action='store_true',
                       help='Skip Spark processing and regenerate visualizations from existing CSVs')
    args = parser.parse_args()

    if args.skip_spark:
        # Skip Spark processing, just regenerate visualizations
        print("Skipping Spark processing, regenerating visualizations from existing CSVs...")

        if not os.path.exists('problem2_timeline.csv') or not os.path.exists('problem2_cluster_summary.csv'):
            print("Error: CSV files not found. Run without --skip-spark first.")
            sys.exit(1)

        generate_visualizations()
        print("\n✓ Visualizations regenerated successfully!")
        return

    # Normal Spark processing
    if not args.master or not args.net_id:
        print("Error: master URL and --net-id are required when not using --skip-spark")
        parser.print_help()
        sys.exit(1)

    # Create Spark session
    print(f"Creating Spark session with master: {args.master}")
    spark = create_spark_session(args.master, args.net_id)

    try:
        # Construct S3 path
        s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"

        # Process data
        app_timeline, cluster_summary = process_spark_data(spark, s3_path)

        # Save timeline CSV
        print("Saving timeline data...")
        save_csv(app_timeline, 'problem2_timeline_temp', 'problem2_timeline.csv')

        # Save cluster summary CSV
        print("Saving cluster summary...")
        save_csv(cluster_summary, 'problem2_cluster_summary_temp', 'problem2_cluster_summary.csv')

        # Generate statistics
        print("Generating statistics...")
        generate_statistics(cluster_summary)

        print("\n✓ Spark processing completed successfully!")

    except Exception as e:
        print(f"Error during Spark processing: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()

    # Generate visualizations
    try:
        generate_visualizations()

        print("\n✓ Problem 2 completed successfully!")
        print("Generated files:")
        print("  - problem2_timeline.csv")
        print("  - problem2_cluster_summary.csv")
        print("  - problem2_stats.txt")
        print("  - problem2_bar_chart.png")
        print("  - problem2_density_plot.png")

    except Exception as e:
        print(f"Error during visualization: {e}", file=sys.stderr)
        print("Note: CSV files were generated successfully. You can regenerate visualizations with --skip-spark")
        raise


if __name__ == "__main__":
    main()
