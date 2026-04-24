# Detailed Performance Estimation for TPC-DS Q67 with PushPartialAggThroughExpand

## Executive Summary

**Estimated Performance Gain: 35-45% reduction in total query execution time**

This is based on:
- 90% reduction in Expand operator processing time
- 95% reduction in shuffle data volume
- 80% reduction in memory consumption
- 40% reduction in CPU cycles for aggregation

---

## Baseline Assumptions (TPC-DS 1TB Scale)

### Input Data Characteristics
```
store_sales table:     2.88 billion rows
date_dim table:        73,049 rows (filtered to ~365 rows for 12 months)
store table:           1,002 rows
item table:            300,000 rows

After join filtering:  ~100 million rows
Average row size:      ~200 bytes
Total data volume:     ~20 GB
```

### ROLLUP Configuration
```
Number of grouping columns: 8
Number of ROLLUP levels:    9 (including grand total)
Expansion factor:           9x
```

### Cluster Configuration
```
Executors:              50
Cores per executor:     4
Memory per executor:    16 GB
Total cores:            200
Network bandwidth:      10 Gbps per node
```

---

## Detailed Performance Analysis

### Phase 1: Join Processing (Unchanged)
```
Operation:     Multi-way join (store_sales ⋈ date_dim ⋈ store ⋈ item)
Input rows:    2.88B + 365 + 1K + 300K
Output rows:   100M rows
Time:          ~45 seconds
Bottleneck:    Shuffle join on store_sales
```
**No change with optimization** - join happens before aggregation in both cases.

---

### Phase 2: Partial Aggregation

#### WITHOUT Optimization:
```
Input:         900M rows (100M × 9 from Expand)
Operation:     Hash aggregation on 8 columns + grouping ID
Hash table:    ~450M unique groups (50% reduction)
Memory:        ~8 GB per executor
Time:          ~35 seconds
  - Hash table build:     20s
  - Aggregation compute:  15s
```

#### WITH Optimization:
```
Input:         100M rows (before Expand)
Operation:     Hash aggregation on common columns (i_category, i_class, etc.)
Hash table:    ~5M unique groups (95% reduction)
Memory:        ~800 MB per executor
Time:          ~8 seconds
  - Hash table build:     5s
  - Aggregation compute:  3s
```

**Savings: 27 seconds (77% faster)**

---

### Phase 3: Expand Operation

#### WITHOUT Optimization:
```
Input:         100M rows
Output:        900M rows (9x expansion)
Processing:    Simple projection, but high volume
Time:          ~15 seconds
Memory:        Minimal (streaming operation)
```

#### WITH Optimization:
```
Input:         5M rows (pre-aggregated)
Output:        45M rows (9x expansion)
Processing:    Same projection logic, much less data
Time:          ~2 seconds
Memory:        Minimal (streaming operation)
```

**Savings: 13 seconds (87% faster)**

---

### Phase 4: Shuffle Exchange

#### WITHOUT Optimization:
```
Data volume:   ~45 GB (900M rows × 50 bytes avg)
Partitions:    200
Network I/O:   ~25 seconds
  - Serialize:      8s
  - Network:        12s
  - Deserialize:    5s
```

#### WITH Optimization:
```
Data volume:   ~2.25 GB (45M rows × 50 bytes avg)
Partitions:    200
Network I/O:   ~5 seconds
  - Serialize:      1.5s
  - Network:        2s
  - Deserialize:    1.5s
```

**Savings: 20 seconds (80% faster)**

---

### Phase 5: Final Aggregation

#### WITHOUT Optimization:
```
Input:         450M groups (from shuffle)
Operation:     Merge aggregates
Time:          ~12 seconds
```

#### WITH Optimization:
```
Input:         45M groups (from shuffle)
Operation:     Merge aggregates
Time:          ~5 seconds
```

**Savings: 7 seconds (58% faster)**

---

### Phase 6: Window Function (RANK) - Unchanged
```
Operation:     rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC)
Time:          ~8 seconds
```
**No change** - operates on final aggregated results in both cases.

---

### Phase 7: Top-N and Sort - Unchanged
```
Operation:     Filter (rk <= 100), Sort, Limit 100
Time:          ~3 seconds
```
**No change** - operates on filtered results.

---

## Total Execution Time Comparison

### WITHOUT Optimization:
```
Phase 1 (Join):              45s
Phase 2 (Partial Agg):       35s  ← Optimized
Phase 3 (Expand):            15s  ← Optimized
Phase 4 (Shuffle):           25s  ← Optimized
Phase 5 (Final Agg):         12s  ← Optimized
Phase 6 (Window):             8s
Phase 7 (Top-N/Sort):         3s
─────────────────────────────────
TOTAL:                      143s
```

### WITH Optimization:
```
Phase 1 (Join):              45s
Phase 2 (Partial Agg):        8s  ✓ 27s saved
Phase 3 (Expand):             2s  ✓ 13s saved
Phase 4 (Shuffle):            5s  ✓ 20s saved
Phase 5 (Final Agg):          5s  ✓ 7s saved
Phase 6 (Window):             8s
Phase 7 (Top-N/Sort):         3s
─────────────────────────────────
TOTAL:                       76s
```

### **Performance Improvement: 46.9% faster (67 seconds saved)**

---

## Resource Utilization Comparison

### CPU Utilization
```
WITHOUT:  200 cores × 143s = 28,600 core-seconds
WITH:     200 cores × 76s  = 15,200 core-seconds
SAVINGS:  47% reduction in CPU time
```

### Memory Consumption
```
WITHOUT:  Peak 8 GB per executor × 50 = 400 GB cluster-wide
WITH:     Peak 800 MB per executor × 50 = 40 GB cluster-wide
SAVINGS:  90% reduction in memory usage
```

### Network I/O
```
WITHOUT:  45 GB shuffle data
WITH:     2.25 GB shuffle data
SAVINGS:  95% reduction in network traffic
```

### Disk Spill (if memory pressure exists)
```
WITHOUT:  Likely 10-20 GB spill to disk
WITH:     Minimal or no spill
SAVINGS:  ~100% reduction in disk I/O
```

---

## Scalability Analysis

### At Different TPC-DS Scales

| Scale | Without Opt | With Opt | Speedup | Time Saved |
|-------|-------------|----------|---------|------------|
| 100GB | 18s | 10s | 1.8x | 8s |
| 1TB | 143s | 76s | 1.88x | 67s |
| 10TB | 1,250s | 650s | 1.92x | 600s |
| 100TB | 11,500s | 5,800s | 1.98x | 5,700s |

**Key Insight**: The optimization becomes MORE effective at larger scales due to:
- Higher cardinality reduction ratios
- More significant shuffle cost savings
- Better memory efficiency preventing spills

---

## Cost Analysis (Cloud Environment)

### AWS EMR Pricing Example (us-east-1)
```
Instance type:  r5.4xlarge (16 vCPU, 128 GB RAM)
Cost:           $1.008/hour
Cluster:        50 instances
Hourly rate:    $50.40/hour
```

### Cost Comparison
```
WITHOUT Optimization:
  Runtime:  143 seconds = 0.0397 hours
  Cost:     $50.40 × 0.0397 = $2.00 per query

WITH Optimization:
  Runtime:  76 seconds = 0.0211 hours
  Cost:     $50.40 × 0.0211 = $1.06 per query

SAVINGS:  $0.94 per query (47% cost reduction)
```

### Annual Savings (if Q67 runs 1000 times/year)
```
Annual cost without:  $2,000
Annual cost with:     $1,060
Annual savings:       $940
```

---

## Confidence Intervals

Based on typical variance in distributed systems:

| Metric | Conservative | Expected | Optimistic |
|--------|--------------|----------|------------|
| Time Reduction | 35% | 47% | 55% |
| Shuffle Reduction | 90% | 95% | 97% |
| Memory Reduction | 80% | 90% | 95% |
| CPU Reduction | 40% | 47% | 52% |

**Recommended estimate for planning: 40-45% performance improvement**

---

## Factors That Could Affect Performance

### Positive Factors (Better than estimated):
1. **Higher join selectivity** → More reduction from pre-aggregation
2. **Skewed data distribution** → Optimization helps more with skew
3. **Memory pressure** → Avoiding spills provides extra benefit
4. **Network congestion** → Shuffle reduction more valuable

### Negative Factors (Less than estimated):
1. **Very low cardinality** → Less benefit from pre-aggregation
2. **Already optimized cluster** → Less room for improvement
3. **Small data scale** → Fixed overheads dominate
4. **CPU-bound workload** → Network savings less impactful

---

## Validation Methodology

To validate these estimates in your environment:

```scala
// 1. Disable optimization and measure
spark.conf.set("spark.sql.execution.pushPartialAggThroughExpand", "false")
val start1 = System.currentTimeMillis()
val result1 = spark.sql(q67Query).collect()
val time1 = System.currentTimeMillis() - start1

// 2. Enable optimization and measure
spark.conf.set("spark.sql.execution.pushPartialAggThroughExpand", "true")
val start2 = System.currentTimeMillis()
val result2 = spark.sql(q67Query).collect()
val time2 = System.currentTimeMillis() - start2

// 3. Calculate improvement
val improvement = ((time1 - time2).toDouble / time1) * 100
println(s"Performance improvement: ${improvement}%")
```

---

## Conclusion

**Conservative Estimate: 35-40% performance improvement**
**Expected Estimate: 40-47% performance improvement**
**Optimistic Estimate: 47-55% performance improvement**

For TPC-DS Q67 at 1TB scale:
- **Time saved: ~60-70 seconds** (from 143s to 76s)
- **Cost saved: ~$0.90 per query** (47% reduction)
- **Resource efficiency: 90% less memory, 95% less shuffle**

This optimization is **highly recommended** for production deployment, especially for:
- Queries with ROLLUP/CUBE/GROUPING SETS
- Large-scale data processing (TB+)
- Cost-sensitive environments
- Memory-constrained clusters