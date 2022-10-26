from urllib.parse import urlparse

import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier

iris = pd.read_parquet('data/pandas_arquivo_correto.parquet')

train, test = train_test_split(iris, test_size=0.3)

train_X = train[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
train_y = train.classEncoder
test_X = test[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
test_y = test.classEncoder


def eval_metrics(actual, pred):
    rmse = np.sqrt(metrics.mean_squared_error(actual, pred))
    mae = metrics.mean_absolute_error(actual, pred)
    r2 = metrics.r2_score(actual, pred)
    return rmse, mae, r2


with mlflow.start_run(experiment_id=1):
    random = 42
    model = KNeighborsClassifier(n_neighbors=3)
    model.fit(train_X, train_y)
    prediction = model.predict(test_X)

    (rmse, mae, r2) = eval_metrics(test_y, prediction)

    mlflow.log_param("random-state", random)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)

    tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

    if tracking_url_type_store != "file":
        mlflow.sklearn.log_model(model, "model", registered_model_name="KNNIrisModel")
    else:
        mlflow.sklearn.log_model(model, "model")
