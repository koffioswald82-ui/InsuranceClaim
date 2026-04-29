"""
DAG Airflow — pipeline AXA Claims Lakehouse.

Orchestration : ingestion → Bronze → Silver → Gold → fraud → churn → qualité.
Retry logic, SLA miss callback, dépendances explicites.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ── Callbacks ──────────────────────────────────────────────────────────────────

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback SLA miss : notifie l'équipe data engineering."""
    missed = ", ".join([str(s) for s in slas])
    send_email(
        to=["data-ops@axa-france.fr"],
        subject=f"[AXA Lakehouse] SLA MISS — DAG {dag.dag_id}",
        html_content=(
            f"<h3>SLA manqué sur le DAG <b>{dag.dag_id}</b></h3>"
            f"<p>Tâches : {missed}</p>"
            f"<p>Heure : {datetime.utcnow().isoformat()}</p>"
        ),
    )


def on_failure_callback(context):
    """Callback échec de tâche : log structuré + notification."""
    task_instance = context.get("task_instance")
    print(f"[FAILURE] Task {task_instance.task_id} failed. "
          f"Execution date: {context.get('execution_date')}")


# ── Default args ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["data-ops@axa-france.fr"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
}

# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="axa_claims_lakehouse",
    description="Pipeline Bronze→Silver→Gold + fraud detection + churn scoring",
    schedule_interval="0 6 * * *",  # Tous les jours à 06h00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["insurance", "lakehouse", "claims", "fraud"],
    sla_miss_callback=sla_miss_callback,
    doc_md="""
## AXA Claims Lakehouse Pipeline

Pipeline de données principal gérant :
1. Ingestion des sinistres, polices et clients (Bronze)
2. Transformation et enrichissement (Silver)
3. Agrégation analytique et KPIs (Gold)
4. Détection de fraude (règles métier)
5. Score de churn client
6. Rapport qualité des données
    """,
) as dag:

    # ── Task functions (wrappers légers) ──────────────────────────────────────

    def _ingest_claims(**context) -> None:
        """Wrapper Airflow pour l'ingestion des sinistres."""
        from src.ingestion.ingest_claims import ingest_claims
        from src.utils.spark_session import get_spark_session

        batch_id = context["ds_nodash"]  # YYYYMMDD
        spark = get_spark_session()
        result = ingest_claims(spark=spark, batch_id=batch_id)
        context["ti"].xcom_push(key="claims_stats", value=result)

    def _ingest_policies(**context) -> None:
        """Wrapper Airflow pour l'ingestion des polices."""
        from src.ingestion.ingest_policies import ingest_policies
        from src.utils.spark_session import get_spark_session

        batch_id = context["ds_nodash"]
        spark = get_spark_session()
        result = ingest_policies(spark=spark, batch_id=batch_id)
        context["ti"].xcom_push(key="policies_stats", value=result)

    def _bronze_to_silver(**context) -> None:
        """Wrapper Airflow pour la transformation Bronze → Silver."""
        from src.transformation.bronze_to_silver import run_bronze_to_silver
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        result = run_bronze_to_silver(spark=spark, batch_id=context["ds_nodash"])
        context["ti"].xcom_push(key="silver_stats", value=result)

    def _silver_to_gold(**context) -> None:
        """Wrapper Airflow pour la transformation Silver → Gold."""
        from src.transformation.silver_to_gold import run_silver_to_gold
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        result = run_silver_to_gold(spark=spark, batch_id=context["ds_nodash"])
        context["ti"].xcom_push(key="gold_stats", value=result)

    def _fraud_detection(**context) -> None:
        """Wrapper Airflow pour la détection de fraude."""
        from src.transformation.fraud_flags import run_fraud_flags
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        result = run_fraud_flags(spark=spark, batch_id=context["ds_nodash"])
        context["ti"].xcom_push(key="fraud_stats", value=result)

    def _churn_scoring(**context) -> None:
        """Wrapper Airflow pour le scoring churn."""
        from src.business_views.churn_score import compute_churn_scores
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        compute_churn_scores(spark=spark)

    def _compute_loss_ratio(**context) -> None:
        """Wrapper Airflow pour le calcul du loss ratio."""
        from src.business_views.loss_ratio import compute_loss_ratio
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        compute_loss_ratio(spark=spark)

    def _portfolio_exposure(**context) -> None:
        """Wrapper Airflow pour l'exposition portefeuille."""
        from src.business_views.portfolio_exposure import compute_portfolio_exposure
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        compute_portfolio_exposure(spark=spark)

    def _claims_aging(**context) -> None:
        """Wrapper Airflow pour le vieillissement des sinistres."""
        from src.business_views.claims_aging import compute_claims_aging
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session()
        compute_claims_aging(spark=spark)

    def _data_quality_report(**context) -> None:
        """Génère le rapport qualité HTML."""
        from monitoring.data_quality_report import generate_report
        generate_report()

    def _pipeline_metrics(**context) -> None:
        """Collecte les métriques pipeline."""
        from monitoring.pipeline_metrics import collect_metrics
        collect_metrics(run_date=context["ds"])

    # ── Opérateurs ─────────────────────────────────────────────────────────────

    ingest_claims_task = PythonOperator(
        task_id="ingest_claims",
        python_callable=_ingest_claims,
        sla=timedelta(minutes=30),
    )

    ingest_policies_task = PythonOperator(
        task_id="ingest_policies",
        python_callable=_ingest_policies,
        sla=timedelta(minutes=30),
    )

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=_bronze_to_silver,
        sla=timedelta(hours=1),
    )

    silver_to_gold_task = PythonOperator(
        task_id="silver_to_gold",
        python_callable=_silver_to_gold,
        sla=timedelta(hours=2),
    )

    fraud_detection_task = PythonOperator(
        task_id="fraud_detection",
        python_callable=_fraud_detection,
        sla=timedelta(hours=1),
    )

    churn_scoring_task = PythonOperator(
        task_id="churn_scoring",
        python_callable=_churn_scoring,
        sla=timedelta(hours=1),
    )

    loss_ratio_task = PythonOperator(
        task_id="compute_loss_ratio",
        python_callable=_compute_loss_ratio,
    )

    portfolio_task = PythonOperator(
        task_id="portfolio_exposure",
        python_callable=_portfolio_exposure,
    )

    claims_aging_task = PythonOperator(
        task_id="claims_aging",
        python_callable=_claims_aging,
    )

    quality_report_task = PythonOperator(
        task_id="data_quality_report",
        python_callable=_data_quality_report,
        trigger_rule="all_done",  # Toujours générer le rapport
    )

    pipeline_metrics_task = PythonOperator(
        task_id="pipeline_metrics",
        python_callable=_pipeline_metrics,
        trigger_rule="all_done",
    )

    # ── Dépendances ────────────────────────────────────────────────────────────
    #
    #  ingest_claims ──┐
    #                  ├─→ bronze_to_silver ──→ silver_to_gold ──┬──→ fraud_detection ──┐
    #  ingest_policies ┘                                         ├──→ churn_scoring    ──┤──→ quality_report
    #                                                            ├──→ loss_ratio       ──┤
    #                                                            ├──→ portfolio        ──┤
    #                                                            └──→ claims_aging    ──┘
    #                                                                                    └──→ pipeline_metrics

    [ingest_claims_task, ingest_policies_task] >> bronze_to_silver_task
    bronze_to_silver_task >> silver_to_gold_task
    silver_to_gold_task >> [
        fraud_detection_task,
        churn_scoring_task,
        loss_ratio_task,
        portfolio_task,
        claims_aging_task,
    ]
    [
        fraud_detection_task,
        churn_scoring_task,
        loss_ratio_task,
        portfolio_task,
        claims_aging_task,
    ] >> quality_report_task >> pipeline_metrics_task
