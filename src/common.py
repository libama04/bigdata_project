"""Shared configuration: dataset source, storage layout, S3 access.

Storage root is resolved in this order:
  1. DATA_ROOT   (any fsspec URI or local directory, used for local tests)
  2. s3://$S3_BUCKET/$S3_PREFIX   (S3_PREFIX defaults to "bigdata_project")

On Onyxia the S3 credentials and endpoint are already exported in every
service (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
AWS_S3_ENDPOINT), so only S3_BUCKET has to be set.
"""
import os
import sys

DATASET_NAME = "Regularite mensuelle TGV par liaisons (AQST)"
DATASET_PAGE = "https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/"
DATASET_URL = (
    "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/"
    "regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B"
)
RAW_FILE = "regularite-mensuelle-tgv-aqst.csv"

# Size limits imposed by the project brief (1 MB .. 5 MB of raw data).
MIN_RAW_BYTES = 1_048_576
MAX_RAW_BYTES = 5_000_000


def root() -> str:
    explicit = os.environ.get("DATA_ROOT")
    if explicit:
        return explicit.rstrip("/")
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        sys.exit("Set S3_BUCKET (your Onyxia bucket) or DATA_ROOT (local directory).")
    prefix = os.environ.get("S3_PREFIX", "bigdata_project").strip("/")
    return f"s3://{bucket}/{prefix}"


def is_s3() -> bool:
    return root().startswith("s3://")


def path(*parts: str) -> str:
    """Path under the storage root, e.g. path('raw', RAW_FILE)."""
    return "/".join([root(), *parts])


def spark_path(*parts: str) -> str:
    """Same as path() but with the s3a:// scheme expected by Hadoop."""
    p = path(*parts)
    return "s3a://" + p[len("s3://"):] if p.startswith("s3://") else p


RAW = ("raw", RAW_FILE)
RAW_META = ("raw", "_ingest_metadata.json")
SILVER = ("processed", "silver", "tgv_clean")
QUALITY = ("processed", "quality_report", "quality_report.json")
GOLD_ROUTE = ("processed", "gold", "route_punctuality")
GOLD_YEAR = ("processed", "gold", "yearly_punctuality")
GOLD_CAUSES = ("processed", "gold", "delay_causes_by_year")
VERIFICATION = ("processed", "verification", "verification_report.json")
BENCHMARK = ("processed", "benchmark", "benchmark.csv")


def s3_endpoint_url():
    endpoint = os.environ.get("AWS_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        return None
    return endpoint if endpoint.startswith("http") else f"https://{endpoint}"


def filesystem():
    """fsspec filesystem matching the storage root."""
    import fsspec

    if is_s3():
        import s3fs

        return s3fs.S3FileSystem(client_kwargs={"endpoint_url": s3_endpoint_url()})
    return fsspec.filesystem("file")


def spark_session(app_name: str):
    """SparkSession; S3A is configured from the Onyxia environment variables."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.session.timeZone", "UTC"
    )
    endpoint = s3_endpoint_url()
    if is_s3() and endpoint:
        token = os.environ.get("AWS_SESSION_TOKEN")
        provider = (
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            if token
            else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        conf = {
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": provider,
            "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        }
        if token:
            conf["spark.hadoop.fs.s3a.session.token"] = token
        for key, value in conf.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def banner(step: str) -> None:
    """Trace line with who/where/when, so that logs can be used as evidence."""
    from datetime import datetime, timezone

    who = os.environ.get("KUBERNETES_NAMESPACE") or os.environ.get("USER", "?")
    host = os.environ.get("HOSTNAME", "?")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"=== {step} | namespace/user={who} | host={host} | {now} | root={root()} ===", flush=True)
