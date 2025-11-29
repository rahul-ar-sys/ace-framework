"""
S3 Handler — supports both AWS S3 (via boto3) and local filesystem emulation.

All public methods are preserved exactly so the rest of the system continues to
work without modification.
"""

import os
import logging
import json
import mimetypes

import boto3
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, List

from config.settings import get_aws_config

logger = logging.getLogger(__name__)


class S3Handler:
    """Unified storage handler with AWS + local-filesystem S3 emulation."""

    def __init__(self):
        self.aws = get_aws_config()

        # determine mode
        self.local_mode = self.aws.env == "local"
        self.root = self.aws.local_root
        print(f"DEBUG: S3Handler initialized. env={self.aws.env}, local_mode={self.local_mode}")

        if self.local_mode:
            logger.info(f"S3Handler running in LOCAL mode. Root: {self.root}")
        else:
            logger.info("S3Handler running in AWS mode.")

            # Create AWS boto3 client
            self.s3 = boto3.client(
                "s3",
                region_name=self.aws.region,
                aws_access_key_id=self.aws.aws_access_key,
                aws_secret_access_key=self.aws.aws_secret_key,
                aws_session_token=self.aws.aws_session_token,
            )

    # ================================================================
    # LOCAL FILESYSTEM HELPERS
    # ================================================================

    def _fs_path(self, bucket: str, key: str) -> str:
        """Map bucket/key → local filesystem path."""
        safe_key = key.lstrip("/")
        return os.path.join(self.root, bucket, safe_key)

    def _ensure_local_dir(self, path: str):
        """Ensure directory exists for file path."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def _write_file_fs(self, path: str, data: bytes):
        """Write bytes into local filesystem."""
        self._ensure_local_dir(path)
        with open(path, "wb") as f:
            f.write(data)

    def _read_file_fs(self, path: str) -> bytes:
        """Read bytes from local filesystem."""
        with open(path, "rb") as f:
            return f.read()

    # ================================================================
    # PUBLIC METHODS — Do NOT change their signatures
    # ================================================================

    def download_csv(self, bucket: str, key: str) -> bytes:
        """Return CSV file bytes from local FS or AWS S3."""
        if self.local_mode:
            path = self._fs_path(bucket, key)
            logger.info(f"[LOCAL] Download CSV: {path}")
            return self._read_file_fs(path)

        logger.info(f"[AWS] Download CSV: s3://{bucket}/{key}")
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def upload_json(self, bucket: str, key: str, data: Dict[str, Any]):
        """Upload a JSON object."""
        encoded = json.dumps(data, indent=2).encode("utf-8")

        if self.local_mode:
            path = self._fs_path(bucket, key)
            logger.info(f"[LOCAL] Upload JSON: {path}")
            self._write_file_fs(path, encoded)
            return

        logger.info(f"[AWS] Upload JSON: s3://{bucket}/{key}")
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=encoded,
            ContentType="application/json",
        )

    def upload_bytes(self, bucket: str, key: str, data: bytes, content_type: str | None = None):
        """Upload raw bytes."""
        if self.local_mode:
            path = self._fs_path(bucket, key)
            logger.info(f"[LOCAL] Upload BYTES: {path}")
            self._write_file_fs(path, data)
            return

        logger.info(f"[AWS] Upload BYTES: s3://{bucket}/{key}")
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
        )

    def upload_file(self, bucket: str, key: str, filepath: str):
        """Upload a file from disk."""
        with open(filepath, "rb") as f:
            data = f.read()

        mime = mimetypes.guess_type(filepath)[0] or "application/octet-stream"

        self.upload_bytes(bucket, key, data, mime)

    def move_processed_file(self, bucket: str, key: str):
        """
        Move a file from "uploads/" to "processed/".
        Works in both AWS and local filesystem.
        """
        new_key = key.replace("uploads/", "processed/")

        if self.local_mode:
            src = self._fs_path(bucket, key)
            dst = self._fs_path(bucket, new_key)
            logger.info(f"[LOCAL] Move file: {src} → {dst}")
            self._ensure_local_dir(dst)
            os.rename(src, dst)
            return

        logger.info(f"[AWS] Move file: s3://{bucket}/{key} → s3://{bucket}/{new_key}")

        # Copy → delete
        self.s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=new_key,
        )
        self.s3.delete_object(Bucket=bucket, Key=key)

    def check_file_exists(self, bucket: str, key: str) -> bool:
        """Return True if file exists."""
        if self.local_mode:
            path = self._fs_path(bucket, key)
            return os.path.exists(path)

        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def get_file_metadata(self, bucket: str, key: str) -> Dict[str, Any] | None:
        """Return metadata for file."""
        if self.local_mode:
            path = self._fs_path(bucket, key)
            if not os.path.exists(path):
                return None
            stat = os.stat(path)
            return {
                "size": stat.st_size,
                "modified": stat.st_mtime,
            }

        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            return response
        except ClientError:
            return None

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """List S3-style object keys under a bucket/prefix."""
        if self.local_mode:
            base = os.path.join(self.root, bucket)
            prefix_path = os.path.join(base, prefix)

            results = []
            for root_dir, _, files in os.walk(prefix_path):
                for f in files:
                    rel = os.path.relpath(os.path.join(root_dir, f), base)
                    results.append(rel)
            return results

        response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [item["Key"] for item in response.get("Contents", [])]

    def list_csv_files(self, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
        """List CSV files under a bucket/prefix."""
        all_keys = self.list_objects(bucket, prefix)
        csv_files = [key for key in all_keys if key.lower().endswith(".csv")]

        results = []
        for key in csv_files:
            metadata = self.get_file_metadata(bucket, key) or {}
            results.append(
                {
                    "key": key,
                    "size": metadata.get("ContentLength") or metadata.get("size"),
                    "last_modified": metadata.get("LastModified") or metadata.get("modified"),
                }
            )
        return results
    
    def upload_submission(self, submission) -> str:
            

            batch_id = submission.batch_id or "UNKNOWN_BATCH"
            sub_id = submission.submission_id

            bucket = self.aws.results_bucket
            key = f"batches/{batch_id}/submissions/{sub_id}.json"

            json_bytes = submission.model_dump_json(indent=2).encode("utf-8")

            # ------------------------------------------------------------
            # LOCAL MODE
            # ------------------------------------------------------------
            if self.local_mode:
                path = self._fs_path(bucket, key)
                logger.info(f"[LOCAL] Upload SubmissionResult → {path}")

                self._ensure_local_dir(path)
                self._write_file_fs(path, json_bytes)

                return path  # local filesystem path

            # ------------------------------------------------------------
            # AWS MODE
            # ------------------------------------------------------------
            logger.info(f"[AWS] Upload SubmissionResult → s3://{bucket}/{key}")

            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_bytes,
                ContentType="application/json",
                Metadata={
                    "submission_id": sub_id,
                    "batch_id": batch_id,
                    "student_id": submission.student_id,
                },
            )

            return f"s3://{bucket}/{key}"

    def upload_text(self, bucket: str, key: str, text: str):
        """Upload text content."""
        if self.local_mode:
            path = self._fs_path(bucket, key)
            logger.info(f"[LOCAL] Upload TEXT: {path}")
            self._ensure_local_dir(path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            return

        logger.info(f"[AWS] Upload TEXT: s3://{bucket}/{key}")
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType="text/plain",
        )
