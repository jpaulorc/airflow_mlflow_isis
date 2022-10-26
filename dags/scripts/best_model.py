from mlflow import MlflowClient
from mlflow.entities import ViewType

best_run = MlflowClient().search_runs(
    experiment_ids="1",
    filter_string="",
    run_view_type=ViewType.ACTIVE_ONLY,
    max_results=1,
    order_by=["metrics.accuracy DESC"]
)[0]

print(best_run)