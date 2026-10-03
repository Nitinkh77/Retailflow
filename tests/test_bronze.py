import pytest
pytest.importorskip("pyspark")
from backend.pipeline.spark_session import get_spark
from backend.pipeline.bronze import run_bronze, SOURCES

def test_bronze_ingests_all_sources_and_adds_metadata(tmp_path):
    spark = get_spark("test")
    raw = tmp_path / "raw"; raw.mkdir()
    for fname in SOURCES.values():
        (raw / fname).write_text("id,val\n1,a\n2,b\n")
    stats = run_bronze(spark, raw, tmp_path / "bronze")
    assert set(stats) == set(SOURCES) and all(n == 2 for n in stats.values())
    df = spark.read.parquet(str(tmp_path / "bronze" / "orders"))
    assert {"id", "val", "_ingested_at", "_source_file"} <= set(df.columns)
    assert df.first()["_source_file"] == SOURCES["orders"]

def test_bronze_reports_missing_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_bronze(get_spark("test"), tmp_path, tmp_path / "bronze")
