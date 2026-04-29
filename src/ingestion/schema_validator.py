"""
Validateur de schéma strict pour les sources du pipeline AXA.

Lève des exceptions explicites sur violations et logue les rejets
dans la dead_letter_queue/.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.utils.config import DEAD_LETTER
from src.utils.logger import get_logger

log = get_logger("ingestion.schema_validator")

# ── Schémas attendus (colonnes obligatoires + types acceptés) ──────────────────

REQUIRED_COLUMNS: dict[str, dict[str, type]] = {
    "claims": {
        "claim_id":         str,
        "policy_id":        str,
        "client_id":        str,
        "type_sinistre":    str,
        "date_sinistre":    str,
        "date_declaration": str,
        "montant_declare":  float,
        "statut":           str,
    },
    "policies": {
        "policy_id":          str,
        "client_id":          str,
        "produit":            str,
        "date_souscription":  str,
        "date_echeance":      str,
        "prime_annuelle":     float,
        "canal_vente":        str,
        "statut":             str,
    },
    "clients": {
        "client_id":     str,
        "client_type":   str,
        "nom":           str,
        "region_code":   str,
        "date_creation": str,
    },
    "payments": {
        "payment_id":    str,
        "claim_id":      str,
        "policy_id":     str,
        "client_id":     str,
        "date_paiement": str,
        "montant":       float,
        "statut":        str,
    },
}

VALID_VALUES: dict[str, dict[str, set[str]]] = {
    "claims": {
        "statut": {"ouvert", "en_cours", "clos", "refuse", "en_litige"},
        "type_sinistre": {
            "accident_auto", "vol", "bris_glace", "catastrophe_naturelle",
            "degat_eau", "incendie", "hospitalisation", "optique", "dentaire",
            "pharmacie", "deces", "invalidite",
        },
    },
    "policies": {
        "produit": {"auto", "habitation", "sante", "vie", "mrh"},
        "statut":  {"actif", "resilié", "suspendu"},
    },
    "clients": {
        "client_type": {"particulier", "professionnel", "entreprise"},
    },
    "payments": {
        "statut": {"paye", "impaye", "en_cours", "rembourse"},
    },
}


class SchemaValidationError(Exception):
    """Levée quand le DataFrame ne satisfait pas le schéma attendu."""


class SchemaValidator:
    """Valide le schéma et la qualité d'un DataFrame avant ingestion.

    Args:
        entity_name: Nom de l'entité ('claims', 'policies', 'clients', 'payments').
        batch_id: Identifiant du batch en cours.
    """

    def __init__(self, entity_name: str, batch_id: str | None = None) -> None:
        if entity_name not in REQUIRED_COLUMNS:
            raise ValueError(f"Entité inconnue : {entity_name!r}. "
                             f"Valeurs acceptées : {list(REQUIRED_COLUMNS)}")
        self.entity_name = entity_name
        self.batch_id = batch_id or str(uuid.uuid4())[:8]
        self._required = REQUIRED_COLUMNS[entity_name]
        self._valid_values = VALID_VALUES.get(entity_name, {})
        self._dead_letter_path = Path(DEAD_LETTER) / entity_name
        self._dead_letter_path.mkdir(parents=True, exist_ok=True)

    # ── Public ─────────────────────────────────────────────────────────────────

    def validate_spark(self, df: DataFrame) -> DataFrame:
        """Valide un Spark DataFrame et retourne uniquement les lignes valides.

        Les lignes invalides sont écrites dans la dead_letter_queue.

        Args:
            df: DataFrame à valider.

        Returns:
            DataFrame épuré (lignes valides uniquement).

        Raises:
            SchemaValidationError: Si des colonnes obligatoires sont manquantes.
        """
        self._check_required_columns_spark(df)
        valid_df, rejected_df = self._split_valid_invalid_spark(df)
        n_rejected = rejected_df.count()
        if n_rejected > 0:
            self._write_dead_letter_spark(rejected_df)
            log.warning(
                "Records rejected",
                entity=self.entity_name,
                n_rejected=n_rejected,
                dead_letter=str(self._dead_letter_path),
            )
        log.info(
            "Schema validation complete",
            entity=self.entity_name,
            n_rejected=n_rejected,
        )
        return valid_df

    def validate_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valide un Pandas DataFrame (utilisé hors contexte Spark).

        Args:
            df: DataFrame à valider.

        Returns:
            DataFrame épuré.

        Raises:
            SchemaValidationError: Si des colonnes obligatoires sont manquantes.
        """
        self._check_required_columns_pandas(df)
        mask_valid = self._build_validity_mask_pandas(df)
        rejected = df[~mask_valid]
        valid = df[mask_valid]
        if len(rejected) > 0:
            self._write_dead_letter_pandas(rejected)
            log.warning(
                "Records rejected (pandas)",
                entity=self.entity_name,
                n_rejected=len(rejected),
            )
        log.info(
            "Schema validation complete (pandas)",
            entity=self.entity_name,
            n_valid=len(valid),
            n_rejected=len(rejected),
        )
        return valid.reset_index(drop=True)

    # ── Private ────────────────────────────────────────────────────────────────

    def _check_required_columns_spark(self, df: DataFrame) -> None:
        missing = set(self._required) - set(df.columns)
        if missing:
            raise SchemaValidationError(
                f"[{self.entity_name}] Colonnes manquantes : {sorted(missing)}"
            )

    def _check_required_columns_pandas(self, df: pd.DataFrame) -> None:
        missing = set(self._required) - set(df.columns)
        if missing:
            raise SchemaValidationError(
                f"[{self.entity_name}] Colonnes manquantes : {sorted(missing)}"
            )

    def _split_valid_invalid_spark(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Sépare lignes valides et invalides via une colonne _is_valid."""
        conditions = []

        # Nulls sur colonnes obligatoires
        for col in self._required:
            conditions.append(F.col(col).isNotNull())

        # Valeurs acceptées
        for col, valid_set in self._valid_values.items():
            if col in df.columns:
                conditions.append(F.col(col).isin(list(valid_set)))

        # Montants > 0
        for col in ["montant_declare", "montant", "prime_annuelle"]:
            if col in df.columns:
                conditions.append(F.col(col) > 0)

        validity = conditions[0]
        for c in conditions[1:]:
            validity = validity & c

        valid_df    = df.filter(validity)
        rejected_df = df.filter(~validity).withColumn(
            "_rejection_reason", F.lit("schema_validation_failed")
        ).withColumn(
            "_rejected_at", F.lit(datetime.now(timezone.utc).isoformat())
        ).withColumn(
            "_batch_id", F.lit(self.batch_id)
        )
        return valid_df, rejected_df

    def _build_validity_mask_pandas(self, df: pd.DataFrame) -> "pd.Series[bool]":
        mask = pd.Series([True] * len(df), index=df.index)
        for col in self._required:
            if col in df.columns:
                mask &= df[col].notna()
        for col, valid_set in self._valid_values.items():
            if col in df.columns:
                mask &= df[col].isin(valid_set)
        for col in ["montant_declare", "montant", "prime_annuelle"]:
            if col in df.columns:
                mask &= df[col] > 0
        return mask

    def _write_dead_letter_spark(self, rejected_df: DataFrame) -> None:
        path = str(self._dead_letter_path / f"batch_{self.batch_id}")
        rejected_df.write.mode("append").parquet(path)

    def _write_dead_letter_pandas(self, rejected: pd.DataFrame) -> None:
        path = self._dead_letter_path / f"batch_{self.batch_id}_rejected.parquet"
        rejected.to_parquet(path, index=False)


def validate_spark_schema(
    df: DataFrame,
    expected_schema: StructType,
    entity_name: str,
) -> None:
    """Vérifie que le schéma Spark correspond exactement au schéma attendu.

    Args:
        df: DataFrame à vérifier.
        expected_schema: Schéma StructType de référence.
        entity_name: Nom de l'entité pour le message d'erreur.

    Raises:
        SchemaValidationError: Si le schéma diffère.
    """
    actual_fields = {f.name: str(f.dataType) for f in df.schema.fields}
    expected_fields = {f.name: str(f.dataType) for f in expected_schema.fields}

    missing = set(expected_fields) - set(actual_fields)
    extra   = set(actual_fields) - set(expected_fields)
    mismatched = {
        k for k in actual_fields
        if k in expected_fields and actual_fields[k] != expected_fields[k]
    }

    errors: list[str] = []
    if missing:
        errors.append(f"Colonnes manquantes : {sorted(missing)}")
    if extra:
        errors.append(f"Colonnes inattendues : {sorted(extra)}")
    if mismatched:
        errors.append(f"Types incorrects : {sorted(mismatched)}")

    if errors:
        raise SchemaValidationError(
            f"[{entity_name}] Schéma invalide — " + " | ".join(errors)
        )

    log.info("Spark schema validation passed", entity=entity_name)
