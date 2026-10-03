from pyspark.sql import functions as F, Window

def split_valid_rejected(df, key, rules, null_cols=None):
    """Dedup on `key`, apply rejection rules; returns (valid, rejected, metrics)."""
    w = Window.partitionBy(*key).orderBy(F.col("_ingested_at"))
    f = df.withColumn("_rn", F.row_number().over(w))
    all_rules = {"duplicate_key": F.col("_rn") > 1, **rules}
    reason = F.concat_ws("; ", *[F.when(c, F.lit(r)) for r, c in all_rules.items()])
    f = f.withColumn("_reject_reason", reason).cache()
    valid = f.filter(F.col("_reject_reason") == "").drop("_rn", "_reject_reason")
    rejected = f.filter(F.col("_reject_reason") != "").drop("_rn")
    nulls = F.lit(False)
    for c in (null_cols or key):
        nulls = nulls | F.col(c).isNull()
    total, rej = f.count(), rejected.count()
    metrics = {"input_records": total, "valid_records": total - rej, "rejected_records": rej,
               "duplicate_records": f.filter(F.col("_rn") > 1).count(),
               "null_records": f.filter(nulls).count()}
    return valid, rejected, metrics
