import os
import json
from pathlib import Path
from typing import Optional


class LocalS3Backend:
    """Filesystem-based replacement for S3."""

    def __init__(self, base_dir: str = "local_s3"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)

    def _path(self, bucket: str, key: str) -> Path:
        return self.base_dir / bucket / key

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs):
        file_path = self._path(Bucket, Key)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(Body)
        return {"LocalPath": str(file_path)}

    def get_object(self, Bucket: str, Key: str):
        file_path = self._path(Bucket, Key)
        if not file_path.exists():
            raise FileNotFoundError(f"Local S3 object {Bucket}/{Key} not found")
        with open(file_path, "rb") as f:
            return {"Body": f.read()}

    def list_objects(self, Bucket: str, Prefix: str = ""):
        bucket_dir = self.base_dir / Bucket
        objects = []

        if not bucket_dir.exists():
            return {"Contents": []}

        for root, _, files in os.walk(bucket_dir):
            for fname in files:
                full = Path(root) / fname
                key = str(full.relative_to(bucket_dir))
                if Prefix in key:
                    objects.append({"Key": key, "Size": full.stat().st_size})

        return {"Contents": objects}

    def delete_object(self, Bucket: str, Key: str):
        file_path = self._path(Bucket, Key)
        if file_path.exists():
            file_path.unlink()
