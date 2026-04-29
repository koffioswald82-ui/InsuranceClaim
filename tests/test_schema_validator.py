"""Tests unitaires — SchemaValidator (Pandas uniquement, pas de Spark requis)."""
import pandas as pd
import pytest

from src.ingestion.schema_validator import SchemaValidationError, SchemaValidator


class TestSchemaValidatorPandas:

    def test_valid_claims_pass(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.ingestion.schema_validator.DEAD_LETTER", str(tmp_path)
        )
        validator = SchemaValidator("claims", batch_id="test01")
        df = pd.DataFrame({
            "claim_id":         ["C001"],
            "policy_id":        ["P001"],
            "client_id":        ["CL001"],
            "type_sinistre":    ["vol"],
            "date_sinistre":    ["2024-01-01"],
            "date_declaration": ["2024-01-05"],
            "montant_declare":  [1000.0],
            "statut":           ["ouvert"],
        })
        result = validator.validate_pandas(df)
        assert len(result) == 1

    def test_missing_required_column_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.ingestion.schema_validator.DEAD_LETTER", str(tmp_path)
        )
        validator = SchemaValidator("claims", batch_id="test02")
        df = pd.DataFrame({"claim_id": ["C001"]})  # colonnes manquantes
        with pytest.raises(SchemaValidationError):
            validator.validate_pandas(df)

    def test_invalid_statut_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.ingestion.schema_validator.DEAD_LETTER", str(tmp_path)
        )
        validator = SchemaValidator("claims", batch_id="test03")
        df = pd.DataFrame({
            "claim_id":         ["C001", "C002"],
            "policy_id":        ["P001", "P002"],
            "client_id":        ["CL001", "CL002"],
            "type_sinistre":    ["vol", "vol"],
            "date_sinistre":    ["2024-01-01", "2024-01-01"],
            "date_declaration": ["2024-01-05", "2024-01-05"],
            "montant_declare":  [1000.0, 500.0],
            "statut":           ["ouvert", "statut_invalide"],
        })
        result = validator.validate_pandas(df)
        assert len(result) == 1  # C002 rejeté

    def test_zero_amount_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.ingestion.schema_validator.DEAD_LETTER", str(tmp_path)
        )
        validator = SchemaValidator("claims", batch_id="test04")
        df = pd.DataFrame({
            "claim_id":         ["C001"],
            "policy_id":        ["P001"],
            "client_id":        ["CL001"],
            "type_sinistre":    ["vol"],
            "date_sinistre":    ["2024-01-01"],
            "date_declaration": ["2024-01-05"],
            "montant_declare":  [0.0],  # invalide : doit être > 0
            "statut":           ["ouvert"],
        })
        result = validator.validate_pandas(df)
        assert len(result) == 0

    def test_unknown_entity_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.ingestion.schema_validator.DEAD_LETTER", str(tmp_path)
        )
        with pytest.raises(ValueError, match="Entité inconnue"):
            SchemaValidator("inexistant")
