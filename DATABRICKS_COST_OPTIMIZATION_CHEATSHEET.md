# ⚡ Databricks Cost-Effective Optimization Cheat Sheet

> **Goal:** Choose the smallest possible cluster that finishes the job within SLA.

---

## 🎯 Decision Order (Most Important)

```
Workload Type → Data Size → SLA → Cost
```

**Cluster sizing depends more on WORKLOAD TYPE than data size.**

---

## 📊 Quick Cluster Sizing Guide

### 🔄 Batch / ETL (Spark Jobs)

| Data Size | Recommended Cluster | Notes |
|-----------|-------------------|-------|
| ≤ 10 GB | Single Node (4–8 GB RAM) | Start here |
| 10–50 GB | 2–4 workers, 4–8 cores | Most common |
| 50–200 GB | 4–8 workers, 8–16 cores | Add autoscaling |
| 200 GB–1 TB | 8–16 workers, autoscaling | Tune shuffle |
| > 1 TB | 16+ workers | Optimize partitions |

💡 **Most ETL jobs are memory-bound, not CPU-bound.**

---

### 📈 SQL / BI Analytics

| Data Size | Cluster | Settings |
|-----------|---------|----------|
| ≤ 50 GB | 2–4 workers | Photon ON |
| 50–500 GB | 4–8 workers | Photon ON |
| > 500 GB | 8–16 workers | Photon ON + Cache |

✅ **Prefer Photon ON**  
✅ **Fewer, larger nodes**  
✅ **Cache only frequently queried tables**

---

### 🔁 Streaming (Structured Streaming)

| Throughput | Cluster | Notes |
|------------|---------|-------|
| Low | 2 workers | Stable cluster |
| Medium | 4–6 workers | Avoid autoscaling |
| High | 8–12 workers | Scale horizontally |

⚠️ **Small, stable clusters**  
⚠️ **Avoid autoscaling unless traffic is unpredictable**

---

### 🤖 ML Training

| Workload | Cluster | Best Practice |
|----------|---------|---------------|
| Small models | 2–4 workers | Job clusters only |
| Large models | 4–8 memory-optimized | Separate training/inference |
| GPU | GPU instances | Use only when proven necessary |

✅ **Separate training and inference clusters**  
✅ **Use job clusters only**  
✅ **Prefer fewer, powerful nodes**

---

## 🔀 Autoscaling Rules

### ✅ Use Autoscaling When:
- Workload is spiky
- Data size varies daily
- Unpredictable traffic patterns

### ❌ Avoid Autoscaling When:
- Small, predictable batch jobs
- Streaming with stable throughput
- Fixed-size daily ETL

**Best Practice:** `min = expected load, max = 2× expected load`

---

## 💸 Cost Killers (Avoid These)

| ❌ Mistake | Impact |
|------------|--------|
| Always-on all-purpose clusters | High idle costs |
| Over-partitioning | Too many small tasks |
| Too many small workers | Inefficient resource use |
| ML jobs on ETL clusters | Wrong instance types |
| Caching everything | Unnecessary memory usage |

---

## 💰 Cost-Saving Power Moves

| ✅ Action | Benefit |
|-----------|---------|
| Use Job Clusters (auto-terminate) | No idle costs |
| Turn Photon ON for SQL/ETL | 2–5× faster, lower cost |
| Use Spot instances for batch | 50–90% savings |
| Repartition to 2–4× total cores | Optimal parallelism |
| Enable auto-termination (10–30 min) | Prevents idle waste |

---

## 🏆 Golden Rule

```
Start Small → Measure → Scale Only If Needed
```

**Most Databricks jobs are over-provisioned by default.**

---

## 📋 Quick Checklist

- [ ] Identified workload type (Batch/SQL/Streaming/ML)
- [ ] Started with smallest recommended cluster
- [ ] Enabled Photon for SQL/ETL workloads
- [ ] Configured auto-termination
- [ ] Used job clusters (not all-purpose)
- [ ] Set autoscaling only if needed
- [ ] Repartitioned to 2–4× cores
- [ ] Measured performance before scaling up

---

## 🎓 Pro Tips

1. **Memory > CPU**: Most Spark jobs are memory-bound
2. **Horizontal > Vertical**: Scale out, not up
3. **Job Clusters**: Always use for scheduled jobs
4. **Photon**: Default ON for SQL/ETL
5. **Spot Instances**: Use for fault-tolerant batch jobs
6. **Partitioning**: 128MB–200MB per partition ideal
7. **Caching**: Only cache what you query repeatedly

---

**Remember:** Start small, measure, then scale. Most clusters are over-provisioned! 🚀
