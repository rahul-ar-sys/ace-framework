"""Result collector for ACE Framework Aggregator Service."""

import json
import logging
import time
import os
from typing import List, Dict, Optional
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

from config.models.core_models import SubmissionResult
from config.settings import get_aws_config

logger = logging.getLogger(__name__)


class ResultCollector:
    """
    Collects processed submission results for aggregation.

    - Fetches results JSON files from S3 (results bucket)
    - Optionally validates structure
    - Converts into `SubmissionResult` models
    """

    def __init__(self):
        """Initialize result collector with AWS clients."""
        self.aws_config = get_aws_config()
        self.s3_client = boto3.client("s3", region_name=self.aws_config.region)
        self.dynamodb_client = boto3.client("dynamodb", region_name=self.aws_config.region)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def collect_results(self, batch_id: str) -> List[SubmissionResult]:
        """
        Collect all processed submission results for a batch.
        Looks for JSON files in:
            s3://<results_bucket>/batches/<batch_id>/submissions/
        Or locally in:
            local_s3/results/
        """
        start_time = time.time()
        results = []

        logger.info(f"Collecting submission results for batch {batch_id}")

        if self.aws_config.env == "local":
            # Local filesystem mode
            local_dir = "local_s3/results"
            if not os.path.exists(local_dir):
                logger.warning(f"Local results directory {local_dir} does not exist.")
                return []
            
            for filename in os.listdir(local_dir):
                if filename.endswith(".json") and filename.startswith(f"{batch_id}_"):
                    try:
                        with open(os.path.join(local_dir, filename), "r", encoding="utf-8") as f:
                            data = json.load(f)
                            results.append(SubmissionResult(**data))
                    except Exception as e:
                        logger.error(f"Failed to load local result {filename}: {e}")
        else:
            # S3 mode
            prefix = f"batches/{batch_id}/submissions/"
            try:
                # ✅ Ensure bucket is a valid string
                bucket = self.aws_config.results_bucket or ""
                if not bucket:
                    raise ValueError("AWS results bucket not configured in environment.")

                # Step 1: List all result files (with pagination)
                s3_objects = self._list_result_files(bucket, prefix)

                # Step 2: Download and parse each result file
                for obj in s3_objects:
                    result = self._load_submission_result(bucket, obj["Key"])
                    if result:
                        results.append(result)

            except Exception as e:
                logger.error(f"Error collecting results for batch {batch_id}: {e}")

        logger.info(f"Finished collecting results for batch {batch_id} in {time.time() - start_time:.2f}s")
        return results


    # -------------------------------------------------------------------------
    # S3 Utilities
    # -------------------------------------------------------------------------

    def _list_result_files(self, bucket: str, prefix: str) -> List[Dict[str, str]]:
        """List all submission result JSON files for the batch (with pagination)."""
        results = []
        continuation_token = None

        try:
            while True:
                kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token

                response = self.s3_client.list_objects_v2(**kwargs)
                for obj in response.get("Contents", []):
                    if obj["Key"].lower().endswith(".json"):
                        results.append(obj)

                if response.get("IsTruncated"):
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break

            logger.debug(f"Found {len(results)} result files under {prefix}")
            return results

        except ClientError as e:
            logger.error(f"Error listing result files: {e}")
            raise
        except EndpointConnectionError:
            logger.error("Network error connecting to S3. Check region or VPC endpoint.")
            raise

    def _load_submission_result(self, bucket: str, key: str) -> Optional[SubmissionResult]:
        """Download and parse a single submission result from S3."""
        for attempt in range(2):  # Retry once
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                data = response["Body"].read().decode("utf-8")
                json_data = json.loads(data)

                # Lightweight validation
                if "submission_id" not in json_data:
                    logger.warning(f"Invalid result file: missing submission_id ({key})")
                    return None

                submission_result = SubmissionResult(**json_data)
                logger.debug(f"Loaded submission result for {submission_result.submission_id}")
                return submission_result

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in result file {key}: {e}")
                return None
            except ClientError as e:
                if attempt == 0:
                    logger.warning(f"S3 read error for {key}, retrying... ({e})")
                    continue
                logger.error(f"Failed to load submission result {key}: {e}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error loading result {key}: {e}")
                return None
        return None

    # -------------------------------------------------------------------------
    # DynamoDB Fallback (optional)
    # -------------------------------------------------------------------------

    def get_from_dynamodb(self, table_name: str, batch_id: str) -> List[SubmissionResult]:
        """Fetch submission results from DynamoDB if S3 is unavailable."""
        results = []
        try:
            response = self.dynamodb_client.query(
                TableName=table_name,
                IndexName="batch_id-index",
                KeyConditionExpression="batch_id = :b",
                ExpressionAttributeValues={":b": {"S": batch_id}},
            )
            for item in response.get("Items", []):
                try:
                    json_data = self._convert_dynamodb_item(item)
                    results.append(SubmissionResult(**json_data))
                except Exception as e:
                    logger.warning(f"Skipping invalid DynamoDB record: {e}")
            return results
        except ClientError as e:
            logger.error(f"Failed to query DynamoDB results for batch {batch_id}: {e}")
            return []

    def _convert_dynamodb_item(self, item: Dict) -> Dict:
        """Convert DynamoDB item to standard dict."""
        result = {}
        for key, value in item.items():
            if "S" in value:
                result[key] = value["S"]
            elif "N" in value:
                result[key] = float(value["N"])
            elif "BOOL" in value:
                result[key] = value["BOOL"]
            elif "L" in value:
                result[key] = [self._convert_dynamodb_item(v) for v in value["L"]]
            elif "M" in value:
                result[key] = self._convert_dynamodb_item(value["M"])
        return result
