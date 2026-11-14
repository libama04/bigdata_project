"""Step 1 - download the raw CSV from SNCF Open Data and store it, untouched, on S3.

The source is refreshed every month, so the raw object is a dated snapshot:
the step is skipped when the object already exists (use --force to refresh).

  python src/01_ingest_raw.py            # download -> s3://$S3_BUCKET/bigdata_project/raw/
  python src/01_ingest_raw.py --force    # overwrite the snapshot
  python src/01_ingest_raw.py --from-file local.csv   # upload an existing snapshot
"""
import argparse
import hashlib
import json
import sys
import urllib.request
from datetime import datetime, timezone

import common as c


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="overwrite existing snapshot")
    parser.add_argument("--from-file", help="upload this local file instead of downloading")
    args = parser.parse_args()

    c.banner("01 ingest raw")
    fs = c.filesystem()
    raw_path, meta_path = c.path(*c.RAW), c.path(*c.RAW_META)

    if fs.exists(raw_path) and not args.force:
        print(f"Raw snapshot already present, skipping: {raw_path}")
        print(fs.cat(meta_path).decode() if fs.exists(meta_path) else "(no metadata)")
        return

    if args.from_file:
        with open(args.from_file, "rb") as f:
            data = f.read()
        source = f"file:{args.from_file}"
    else:
        request = urllib.request.Request(c.DATASET_URL, headers={"User-Agent": "bigdata-project"})
        with urllib.request.urlopen(request, timeout=120) as response:
            data = response.read()
        source = c.DATASET_URL

    size = len(data)
    if not c.MIN_RAW_BYTES <= size <= c.MAX_RAW_BYTES:
        sys.exit(f"Raw size {size} bytes is outside the allowed 1-5 MB range, aborting.")

    fs.makedirs(c.path("raw"), exist_ok=True)
    with fs.open(raw_path, "wb") as f:
        f.write(data)

    metadata = {
        "dataset": c.DATASET_NAME,
        "page": c.DATASET_PAGE,
        "source": source,
        "license": "Open Database License (ODbL) - SNCF Voyageurs",
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "size_bytes": size,
        "size_mb": round(size / 1_000_000, 3),
        "physical_lines": data.count(b"\n"),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    with fs.open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Stored {raw_path}")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
