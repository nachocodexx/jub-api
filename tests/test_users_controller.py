"""
Integration tests for /api/v2/users/ endpoints.

Covers:
  POST  /users/signup              create a new user
  POST  /users/auth                login
  GET   /users/me                  get authenticated user profile
  GET   /users/{user_id}/settings  get user settings
  PUT   /users/{user_id}/settings  update user settings
"""

import pytest
from uuid import uuid4
from httpx import AsyncClient
import commonx.dto.xolo as XoloDTO
import jubapi.dto.v2 as DTO


# ==========================================
# Helpers
# ==========================================

def unique_user(prefix: str = "testuser") -> dict:
    uid = uuid4().hex[:8]
    username = f"{prefix}{uid}"
    return {
        "username":      username,
        "password":      "password123",
        "email":         f"{username}@test.com",
        "expiration":    "1h",
        "first_name":    "Test",
        "last_name":     "User",
        "scope":         "jub",
        "profile_photo": "",
    }


async def signup_and_login(client: AsyncClient) -> tuple[dict, dict]:
    """Returns (user_profile_dict, auth_headers)."""
    user_data = unique_user()
    signup_resp = await client.post("/api/v2/users/signup", json=user_data)
    assert signup_resp.status_code == 200

    login_resp = await client.post("/api/v2/users/auth", json={
        "username":   user_data["username"],
        "password":   user_data["password"],
        "scope":      user_data["scope"],
        "expiration": "1h",
    })
    assert login_resp.status_code == 200
    body = login_resp.json()
    headers = {
        "Authorization":      f"Bearer {body['access_token']}",
        "Temporal-Secret-Key": body.get("temporal_secret_key") or "",
    }
    return body["user_profile"], headers


# ==========================================
# 1. Signup
# ==========================================

@pytest.mark.asyncio
async def test_signup_success(unauth_client: AsyncClient):
    resp = await unauth_client.post("/api/v2/users/signup", json=unique_user())
    assert resp.status_code == 200
    body = resp.json()
    assert "username" in body


@pytest.mark.asyncio
async def test_signup_missing_required_field_returns_422(unauth_client: AsyncClient):
    incomplete = {"username": "nopassword", "scope": "jub"}
    resp = await unauth_client.post("/api/v2/users/signup", json=incomplete)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_signup_duplicate_username_returns_error(unauth_client: AsyncClient):
    data = unique_user()
    first = await unauth_client.post("/api/v2/users/signup", json=data)
    assert first.status_code == 200
    second = await unauth_client.post("/api/v2/users/signup", json=data)
    assert second.status_code != 200


# ==========================================
# 2. Login
# ==========================================

@pytest.mark.asyncio
async def test_login_success(unauth_client: AsyncClient):
    data = unique_user()
    await unauth_client.post("/api/v2/users/signup", json=data)
    resp = await unauth_client.post("/api/v2/users/auth", json={
        "username": data["username"], "password": data["password"],
        "scope": data["scope"], "expiration": "1h",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert "user_profile" in body
    assert body["user_profile"]["username"] == data["username"]


@pytest.mark.asyncio
async def test_login_wrong_password_returns_error(unauth_client: AsyncClient):
    data = unique_user()
    await unauth_client.post("/api/v2/users/signup", json=data)
    resp = await unauth_client.post("/api/v2/users/auth", json={
        "username": data["username"], "password": "WRONG_PASSWORD",
        "scope": data["scope"], "expiration": "1h",
    })
    assert resp.status_code != 200


@pytest.mark.asyncio
async def test_login_nonexistent_user_returns_error(unauth_client: AsyncClient):
    resp = await unauth_client.post("/api/v2/users/auth", json={
        "username": "ghost_user_xyz", "password": "whatever",
        "scope": "jub", "expiration": "1h",
    })
    assert resp.status_code != 200


# ==========================================
# 3. GET /me
# ==========================================

@pytest.mark.asyncio
async def test_get_me_success(unauth_client: AsyncClient):
    user_profile, headers = await signup_and_login(unauth_client)
    resp = await unauth_client.get("/api/v2/users/me", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["username"] == user_profile["username"]
    assert body["user_id"] == user_profile["user_id"]


@pytest.mark.asyncio
async def test_get_me_without_token_returns_401(unauth_client: AsyncClient):
    resp = await unauth_client.get("/api/v2/users/me")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_get_me_invalid_token_returns_401(unauth_client: AsyncClient):
    resp = await unauth_client.get(
        "/api/v2/users/me",
        headers={"Authorization": "Bearer invalid.token.here"},
    )
    assert resp.status_code == 401


# ==========================================
# 4. GET settings
# ==========================================

@pytest.mark.asyncio
async def test_get_settings_own_user(unauth_client: AsyncClient):
    user_profile, headers = await signup_and_login(unauth_client)
    resp = await unauth_client.get(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "appearance" in body
    assert "exploration" in body
    assert "export" in body


@pytest.mark.asyncio
async def test_get_settings_other_user_returns_403(unauth_client: AsyncClient):
    user_a, headers_a = await signup_and_login(unauth_client)
    user_b, _         = await signup_and_login(unauth_client)

    resp = await unauth_client.get(
        f"/api/v2/users/{user_b['user_id']}/settings",
        headers=headers_a,
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_get_settings_unauthenticated_returns_401(unauth_client: AsyncClient):
    user_profile, _ = await signup_and_login(unauth_client)
    resp = await unauth_client.get(f"/api/v2/users/{user_profile['user_id']}/settings")
    assert resp.status_code == 401


# ==========================================
# 5. PUT settings
# ==========================================

NEW_SETTINGS = {
    "appearance": {"theme": "dark", "font_size": 18, "reduce_animations": True},
    "exploration": {"enable_tutorial": False, "default_view": "list", "items_per_page": 50},
    "export": {"default_format": "json", "include_metadata": True},
}


@pytest.mark.asyncio
async def test_update_settings_own_user(unauth_client: AsyncClient):
    user_profile, headers = await signup_and_login(unauth_client)
    resp = await unauth_client.put(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        headers=headers,
        json=NEW_SETTINGS,
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_update_settings_persisted(unauth_client: AsyncClient):
    user_profile, headers = await signup_and_login(unauth_client)
    await unauth_client.put(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        headers=headers,
        json=NEW_SETTINGS,
    )
    # Verify the change was persisted
    get_resp = await unauth_client.get(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        headers=headers,
    )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["appearance"]["theme"] == "dark"
    assert body["appearance"]["font_size"] == 18
    assert body["exploration"]["default_view"] == "list"
    assert body["export"]["default_format"] == "json"


@pytest.mark.asyncio
async def test_update_settings_other_user_returns_403(unauth_client: AsyncClient):
    user_a, headers_a = await signup_and_login(unauth_client)
    user_b, _         = await signup_and_login(unauth_client)

    resp = await unauth_client.put(
        f"/api/v2/users/{user_b['user_id']}/settings",
        headers=headers_a,
        json=NEW_SETTINGS,
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_update_settings_unauthenticated_returns_401(unauth_client: AsyncClient):
    user_profile, _ = await signup_and_login(unauth_client)
    resp = await unauth_client.put(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        json=NEW_SETTINGS,
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_update_settings_invalid_body_returns_422(unauth_client: AsyncClient):
    user_profile, headers = await signup_and_login(unauth_client)
    resp = await unauth_client.put(
        f"/api/v2/users/{user_profile['user_id']}/settings",
        headers=headers,
        json={"appearance": {"font_size": "not_a_number"}},
    )
    assert resp.status_code == 422
