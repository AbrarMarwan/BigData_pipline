import sys
import os
import pytest
from fastapi.testclient import TestClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.api import app

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client

def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert "collections_estimated_count" in data
    assert "scheduler" in data

def test_indexes_creation(client):
    response = client.post("/indexes")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert len(data["indexes"]) >= 3

def test_queries_list(client):
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 5
    assert "city_and_status" in data["queries"]
    assert "customer_history" in data["queries"]
    assert "date_range" in data["queries"]

def test_run_single_query(client):
    response = client.get("/queries/city_and_status?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data["query_name"] == "city_and_status"
    assert "results" in data
    assert isinstance(data["results"], list)

def test_run_query_with_explain(client):
    response = client.get("/queries/city_and_status?limit=5&explain=true")
    assert response.status_code == 200
    plan = response.json()
    assert "executionStats" in plan or "queryPlanner" in plan

def test_explain_comparison(client):
    response = client.get("/queries/explain/compare")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["queries_benchmarked"] == 3
    for comp in data["comparisons"]:
        assert "before_index" in comp
        assert "after_index" in comp
        assert "metrics_comparison" in comp

def test_aggregations_list(client):
    response = client.get("/aggregations")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 5
    assert "sales_by_city" in data["reports"]
    assert "top_products" in data["reports"]
    assert "top_customers" in data["reports"]
    assert "sales_by_period" in data["reports"]
    assert "orders_by_status" in data["reports"]

@pytest.mark.parametrize("report_name", [
    "sales_by_city",
    "top_products",
    "top_customers",
    "sales_by_period",
    "orders_by_status"
])
def test_each_aggregation_report(client, report_name):
    response = client.get(f"/aggregations/{report_name}")
    assert response.status_code == 200
    data = response.json()
    assert data["report_name"] == report_name
    assert "data" in data

def test_refresh_materialized_views(client):
    # Test incremental refresh
    response = client.post("/refresh-mv?incremental=true")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert len(data["views"]) == 2

def test_get_views_data(client):
    res1 = client.get("/views/daily_sales_summary?limit=5")
    assert res1.status_code == 200
    assert res1.json()["view_name"] == "daily_sales_summary"

    res2 = client.get("/views/top_products_summary?limit=5")
    assert res2.status_code == 200
    assert res2.json()["view_name"] == "top_products_summary"

def test_jobs_list(client):
    response = client.get("/jobs")
    assert response.status_code == 200
    data = response.json()
    assert "scheduler_status" in data
    assert len(data["scheduler_status"]["jobs"]) >= 2
    assert "recent_executions_log" in data

def test_manual_job_trigger(client):
    response = client.post("/jobs/refresh_materialized_views/run")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["job_name"] == "refresh_materialized_views"
