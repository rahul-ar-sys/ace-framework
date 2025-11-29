"""Configuration loader for interpreter service ."""

import json
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

from config.models.core_models import (
    ArtifactType,
    InstitutionConfig,
    RoutingConfig,
    ACEDimension,
)
from config.settings import get_aws_config

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Loads routing and institution configurations from S3 or DynamoDB."""

    def __init__(self):
        """Initialize config loader."""
        self.aws_config = get_aws_config()
        self.s3_client = boto3.client("s3", region_name=self.aws_config.region)
        self.dynamodb_client = boto3.client("dynamodb", region_name=self.aws_config.region)

        # Simple in-memory cache
        self._config_cache: Dict[str, Any] = {}
        self._cache_ttl = 300  # seconds (5 min)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_institution_config(self, institution_id: str) -> Optional[InstitutionConfig]:
        """Load configuration for a specific institution."""
        cache_key = f"institution_{institution_id}"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]

        try:
            # Try S3 first
            cfg = self._load_from_s3(f"configs/institutions/{institution_id}.json")
            if cfg:
                obj = InstitutionConfig(**cfg)
                self._config_cache[cache_key] = obj
                return obj

            # Fallback: DynamoDB
            cfg = self._load_from_dynamodb("ace-institution-configs", institution_id)
            if cfg:
                obj = InstitutionConfig(**cfg)
                self._config_cache[cache_key] = obj
                return obj

            logger.warning("No config found for institution %s", institution_id)
            return None

        except Exception as e:
            logger.exception("Error loading institution config %s: %s", institution_id, e)
            return None

    def get_routing_config(
        self, institution_id: str, artifact_type: ArtifactType
    ) -> Optional[RoutingConfig]:
        """Return routing config for a specific artifact type."""
        institution = self.get_institution_config(institution_id)
        if not institution:
            return self._get_default_routing_config(artifact_type)

        routing_cfgs = institution.routing_configs

        # Normalize lookup across both enum and string keys
        if isinstance(routing_cfgs, dict):
            for key, cfg in routing_cfgs.items():
                if str(getattr(key, "value", key)) == artifact_type.value:
                    return cfg

        # Default fallback
        return self._get_default_routing_config(artifact_type)


    def get_default_institution_config(self) -> InstitutionConfig:
        """Return the default institution config (cached)."""
        cache_key = "institution_default"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]

        try:
            cfg = self._load_from_s3("configs/default_institution.json")
            if cfg:
                obj = InstitutionConfig(**cfg)
                self._config_cache[cache_key] = obj
                return obj
        except Exception as e:
            logger.warning("Default config not found in S3: %s", e)

        obj = self._create_default_config()
        self._config_cache[cache_key] = obj
        return obj

    def save_institution_config(self, config: InstitutionConfig) -> bool:
        """Persist institution config to S3."""
        try:
            key = f"configs/institutions/{config.institution_id}.json"
            if hasattr(config, "model_dump_json"):
                config_json = config.model_dump_json(indent=2)
            elif hasattr(config, "json"):
                config_json = config.json(indent=2)
            else:
                config_json = json.dumps(config.__dict__, indent=2)

            self.s3_client.put_object(
                Bucket=self.aws_config.config_bucket,
                Key=key,
                Body=config_json.encode("utf-8"),
                ContentType="application/json",
            )
            self._config_cache.pop(f"institution_{config.institution_id}", None)
            logger.info("Saved config for institution %s to S3", config.institution_id)
            return True
        except Exception as e:
            logger.exception("Failed to save config: %s", e)
            return False

    def invalidate_cache(self, key: Optional[str] = None):
        """Invalidate the configuration cache."""
        if key:
            self._config_cache.pop(key, None)
        else:
            self._config_cache.clear()
        logger.info("Config cache invalidated (%s)", key or "all")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_from_s3(self, key: str) -> Optional[Dict[str, Any]]:
        """Load and parse JSON config from S3."""
        try:
            obj = self.s3_client.get_object(Bucket=self.aws_config.config_bucket, Key=key)
            data = obj["Body"].read().decode("utf-8")
            return json.loads(data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.debug("S3 config key not found: %s", key)
                return None
            logger.error("S3 error loading %s: %s", key, e)
            raise
        except Exception as e:
            logger.error("Unexpected S3 error (%s): %s", key, e)
            return None

    def _load_from_dynamodb(self, table: str, key_value: str) -> Optional[Dict[str, Any]]:
        """Load JSON-like item from DynamoDB."""
        try:
            resp = self.dynamodb_client.get_item(TableName=table, Key={"id": {"S": key_value}})
            if "Item" not in resp:
                return None
            return self._dynamodb_to_dict(resp["Item"])
        except ClientError as e:
            logger.error("DynamoDB client error: %s", e)
            return None
        except Exception as e:
            logger.error("DynamoDB unknown error: %s", e)
            return None

    def _dynamodb_to_dict(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively convert DynamoDB wire format to plain dict."""
        result: Dict[str, Any] = {}
        for k, v in item.items():
            if "S" in v:
                result[k] = v["S"]
            elif "N" in v:
                num = v["N"]
                result[k] = float(num) if "." in num else int(num)
            elif "BOOL" in v:
                result[k] = v["BOOL"]
            elif "L" in v:
                result[k] = [self._dynamodb_to_dict(x) if isinstance(x, dict) else x for x in v["L"]]
            elif "M" in v:
                result[k] = self._dynamodb_to_dict(v["M"])
        return result

    def _get_default_routing_config(self, artifact_type: ArtifactType) -> RoutingConfig:
        """Build a default routing config for a given artifact type."""
        default_weights = {
            ACEDimension.ANALYSIS: 0.4,
            ACEDimension.COMMUNICATION: 0.3,
            ACEDimension.EVALUATION: 0.3,
        }

        default_processors: Dict[ArtifactType, Dict[str, Any]] = {
            ArtifactType.MCQ: {
                "processor_type": "deterministic",
                "evaluation_method": "exact_match",
            },
            ArtifactType.TEXT: {
                "processor_type": "ai",
                "model": "gpt-3.5-turbo",
                "evaluation_criteria": ["clarity", "reasoning", "depth"],
            },
            ArtifactType.AUDIO: {
                "processor_type": "ai",
                "speech_to_text": "whisper",
                "evaluation_method": "text_analysis",
                "communication_metrics": ["fluency", "pace", "confidence"],
            },
        }

        return RoutingConfig(
            artifact_type=artifact_type,
            processor_config=default_processors.get(artifact_type, {}),
            ace_weight_mapping=default_weights,
            evaluation_criteria={"criteria": ["accuracy", "comprehension", "communication"]},
            custom_rules={},
        )

    def _create_default_config(self) -> InstitutionConfig:
        """Generate an in-memory fallback default configuration."""
        default_weights = {
            ACEDimension.ANALYSIS: 0.4,
            ACEDimension.COMMUNICATION: 0.3,
            ACEDimension.EVALUATION: 0.3,
        }
        routing_cfgs = {atype: self._get_default_routing_config(atype) for atype in ArtifactType}

        return InstitutionConfig(
            institution_id="default",
            name="Default Institution",
            ace_weights=default_weights,
            passing_threshold=70.0,
            excellence_threshold=90.0,
            routing_configs=routing_cfgs,
            branding={},
            custom_fields={},
        )
