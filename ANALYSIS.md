# Spark Log Analysis - Assignment Report

**Student:** [Your Name]
**Net ID:** adc134
**Date:** December 16, 2025

---

## Overview

This report documents the analysis of 2.8 GB of production Spark cluster logs using PySpark on a distributed AWS EC2 cluster. The dataset contains logs from 194 Spark applications running across 6 YARN clusters between 2015-2017.

---

## Problem 1: Log Level Distribution

### Approach

[TO DO: Add your approach for Problem 1 if completed]

### Key Findings

[TO DO: Add your findings for Problem 1 if completed]

---

## Problem 2: Cluster Usage Analysis

### Approach

**Objective:** Analyze cluster usage patterns to identify which clusters are most heavily used and understand application execution timelines.

**Implementation Strategy:**

1. **Data Extraction:**
   - Used PySpark's `input_file_name()` to capture S3 file paths
   - Applied regex patterns to extract cluster IDs and application IDs from file paths
   - Pattern: `application_<cluster_id>_<app_number>`

2. **Timeline Construction:**
   - Parsed log timestamps using format `yy/MM/dd HH:mm:ss`
   - Grouped by application to find start (min timestamp) and end (max timestamp) times
   - Created time-series dataset with 194 application timelines

3. **Cluster Aggregation:**
   - Grouped applications by cluster ID
   - Calculated number of applications per cluster
   - Identified cluster operation timeframes (first app to last app)

4. **Visualization:**
   - Converted Spark DataFrames to Pandas for local visualization
   - Generated bar chart showing application distribution across clusters
   - Created density plot for job duration distribution on the largest cluster
   - Applied log scale to handle skewed duration data

### Key Findings

**Cluster Usage Statistics:**
- **Total Clusters:** 6 unique clusters in the dataset
- **Total Applications:** 194 applications processed
- **Average Applications per Cluster:** 32.33

**Cluster Distribution:**
- **Cluster 1485248649253:** 181 applications (93.3%) - Dominant cluster
- **Cluster 1472621869829:** 8 applications (4.1%)
- **Cluster 1448006111297:** 2 applications (1.0%)
- **Three other clusters:** 1 application each (0.5% each)

**Key Insights:**
1. **Highly Skewed Usage:** One cluster (1485248649253) processes over 93% of all applications, indicating a heavily utilized production cluster
2. **Cluster Specialization:** The dominant cluster appears to be the primary production environment, while others may be for testing or specialized workloads
3. **Job Duration Distribution:** The largest cluster shows varied job durations, suggesting diverse workload types (quick jobs and long-running analytics)

### Visualizations

**1. Bar Chart - Applications per Cluster:**
- Shows dramatic difference in cluster utilization
- Cluster 1485248649253 clearly dominates with 181 applications
- Other clusters have minimal usage (≤8 applications each)
- Visualization makes the concentration of work on a single cluster immediately apparent

**2. Density Plot - Job Duration Distribution:**
- Displays job duration histogram for the largest cluster (n=181)
- Uses log scale on x-axis to handle wide range of durations
- KDE (kernel density estimation) overlay shows distribution smoothness
- Reveals that most jobs complete relatively quickly, with a long tail of longer-running jobs

### Performance Observations

**Execution Environment:**
- **Cluster Configuration:** 1 master + 3 worker nodes on AWS EC2
- **Instance Type:** t3.large (4 vCPU, 8 GB RAM per node)
- **Total Execution Time:** ~2 minutes for full dataset processing
- **Data Volume:** 2.8 GB of log files, 194 applications, 3,852 container logs

**Optimization Strategies:**
1. **Data Conversion:** Converted Spark DataFrames to Pandas only for visualization (small aggregated data)
2. **Efficient Aggregations:** Used Spark's built-in aggregation functions (min, max, count) for distributed processing
3. **File System Fix:** Initially encountered issues with Spark writing to distributed filesystem instead of local; resolved by converting to Pandas and using pandas.to_csv() for local writes

**Performance Notes:**
- Initial runs failed to create CSV files because Spark wrote to distributed filesystem
- Resolved by modifying `save_csv()` function to convert DataFrames to Pandas first
- Visualization generation (<1 second) was much faster than Spark processing (~2 minutes)
- Overall workflow efficient for the dataset size

### Technical Challenges and Solutions

**Challenge 1: CSV File Creation**
- **Issue:** Spark's `df.write.csv()` wrote to distributed filesystem, not local filesystem
- **Solution:** Modified code to convert to Pandas and use `pandas_df.to_csv()` for local file writing

**Challenge 2: Missing Dependencies**
- **Issue:** matplotlib was not installed in the cluster environment
- **Solution:** Added matplotlib to project dependencies using `uv add matplotlib`

**Challenge 3: Environment Variables**
- **Issue:** `$MASTER_PRIVATE_IP` not set in SSH session
- **Solution:** Used `source cluster-ips.txt` to load environment variables before running script

### Code Quality and Documentation

**Best Practices Implemented:**
- Clear function separation (data processing, CSV saving, statistics, visualization)
- Comprehensive docstrings for each function
- Command-line argument parsing with help messages
- `--skip-spark` flag for fast visualization regeneration
- Error handling with informative messages
- Progress indicators during execution

---

## Spark Web UI Screenshots

[TO DO: Add screenshots if available]

To capture these, access:
- Master UI: `http://<MASTER_PUBLIC_IP>:8080`
- Application UI: `http://<MASTER_PUBLIC_IP>:4040`

---

## Overall Insights

### Dataset Characteristics
- The dataset represents real production Spark workloads from 2015-2017
- Dominated by a single heavily-used cluster (93% of applications)
- Applications show diverse execution patterns and durations

### Performance Takeaways
1. **Distributed Processing:** PySpark efficiently handled 2.8 GB of logs across distributed nodes
2. **Local vs Cluster:** Initial development and debugging on small samples saved significant time
3. **Visualization Strategy:** Converting aggregated data to Pandas for visualization was efficient

### Lessons Learned
1. **File System Awareness:** Understanding Spark's default filesystem vs local filesystem is critical
2. **Environment Setup:** Proper environment variable configuration is essential for cluster operations
3. **Iterative Development:** Testing locally first, then scaling to cluster significantly improved productivity
4. **Dependency Management:** Using `uv` for dependency management streamlined the workflow

---

## Conclusion

This assignment successfully demonstrated:
- Setting up and managing a distributed Spark cluster on AWS
- Processing large-scale log data with PySpark
- Extracting structured insights from unstructured logs
- Creating effective visualizations of distributed system usage patterns

The analysis revealed significant cluster usage imbalance, with one cluster handling the vast majority of production workloads. This insight could inform capacity planning and resource allocation decisions in a production environment.

---

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Seaborn Visualization Library](https://seaborn.pydata.org/)
- Assignment dataset: YARN cluster logs (2015-2017)
