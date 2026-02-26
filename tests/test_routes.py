from fastapi.testclient import TestClient

from src.main import app


client = TestClient(app)


def test_healthz_status_code_is_success():
    response = client.get("/_healthz")
    assert response.status_code in {200, 201}


def test_probe_status_code_is_success():
    response = client.get("/_probe")
    assert response.status_code in {200, 201}


def test_root_rejects_request_when_required_headers_are_missing():
    root_path = str(app.url_path_for("root"))
    response = client.get(root_path)

    assert response.status_code == 400
    payload = response.json()
    assert payload["result"]["code"] == "PT-1401"
    assert "missing required header" in payload["result"]["description"].lower()
    assert isinstance(payload["errors"], list)
    assert payload["errors"]


def test_root_allows_request_when_required_headers_are_present():
    root_path = str(app.url_path_for("root"))
    response = client.get(
        root_path,
        headers={
            "Authorization": "Bearer token",
            "X-Transaction-Id": "123e4567-e89b-12d3-a456-426614174000",
            "X-Correlation-Id": "123e4567-e89b-12d3-a456-426614174001",
            "Accept-Language": "en-US",
            "Accept": "application/json",
        },
    )

    assert response.status_code in {200, 201}
