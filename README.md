# 🧠 Merchant Pattern Detector (PySpark + S3 + PostgreSQL)

This project provides a scalable pipeline to detect abnormal transaction patterns in merchant-customer interactions using PySpark. It processes transaction data in chunks, analyzes patterns, uploads detection results to Amazon S3, and tracks processing state using a PostgreSQL database.

---

## 🔍 Key Features

- ✅ Spark-based large-scale data processing  
- 📦 Chunk-wise data handling and deduplication  
- 📊 Pattern detection:
  - High customer frequency (Pattern 1)
  - Low average amount with high volume (Pattern 2)
  - Gender imbalance in traffic (Pattern 3)
- ☁️ S3 integration for chunk storage and detection output
- 🛢️ PostgreSQL tracking of processed chunks
- 💾 Outputs CSV files for detected patterns

---
