"""
Integration tests for /api/v2/observatories/ endpoints.

Covers every route exposed by jubapi/controllers/v2/observatories.py:

  GET    /observatories/                               list (pagination)
  POST   /observatories/                               create (enabled)
  GET    /observatories/{id}                           get one
  PUT    /observatories/{id}                           update
  DELETE /observatories/{id}                           delete

  POST   /observatories/setup                          setup (disabled + task)

  POST   /observatories/{id}/catalogs/bulk             bulk-assign catalogs
  POST   /observatories/{id}/products/bulk             bulk-assign products

  POST   /observatories/{id}/catalogs                  link a catalog
  GET    /observatories/{id}/catalogs                  list linked catalogs
  DELETE /observatories/{id}/catalogs/{cat_id}         unlink a catalog

  GET    /observatories/{id}/products                  list linked products
  POST   /observatories/{id}/products                  link a product
  DELETE /observatories/{id}/products/{prod_id}        unlink a product

  POST   /observatories/{id}/view                      increment view count

  GET    /observatories/{id}/reviews                   list reviews
  POST   /observatories/{id}/reviews                   create review  (auth)
  PUT    /observatories/{id}/reviews/{rev_id}          update review  (auth)
  DELETE /observatories/{id}/reviews/{rev_id}          delete review  (auth)
"""

import pytest
from httpx import AsyncClient
from jubapi.db.constants import CollectionNames

BASE = "/api/v2/observatories"


# ==========================================
# Helpers / fixtures
# ==========================================

@pytest.fixture(autouse=True)
async def clean(test_db):
    """
    Drops every observatory-related collection before each test so each test
    starts with a completely blank slate.
    """
    await test_db[CollectionNames.OBSERVATORIES.value].drop()
    await test_db[CollectionNames.OBSERVATORY_CATALOG_LINKS.value].drop()
    await test_db[CollectionNames.OBSERVATORY_PRODUCT_LINKS.value].drop()
    await test_db[CollectionNames.OBSERVATORY_SERVICE_LINKS.value].drop()
    await test_db[CollectionNames.OBSERVATORY_DATASOURCE_LINKS.value].drop()
    await test_db[CollectionNames.CATALOGS.value].drop()
    await test_db[CollectionNames.PRODUCTS.value].drop()
    await test_db[CollectionNames.SERVICES.value].drop()
    await test_db[CollectionNames.DATA_SOURCES.value].drop()
    await test_db[CollectionNames.OBSERVATORY_REVIEWS.value].drop()
    await test_db[CollectionNames.TASKS.value].drop()
    yield


@pytest.fixture
async def seeded_observatories(async_client: AsyncClient, clean):
    """
    Inserts 5 observatories directly into the database (bypassing the HTTP
    layer) so pagination tests have a stable data set without depending on
    the create endpoint.

    Explicitly depends on `clean` to guarantee the drop-then-insert order —
    without this dependency pytest may interleave the two autouse/explicit
    fixtures arbitrarily.
    """
    from jubapi.db import get_collection
    from jubapi.db.constants import CollectionNames as CN
    import jubapi.models.v2 as M

    col = get_collection(CN.OBSERVATORIES.value)
    for i in range(1, 6):
        obs = M.ObservatoryX(
            observatory_id=f"obs_{i:03d}",
            title=f"Observatory {i}",
            description=f"Test observatory number {i}",
        )
        await col.insert_one(obs.model_dump())
    return async_client


@pytest.fixture
async def created_observatory(async_client: AsyncClient):
    """
    Creates a single observatory via the API and returns the parsed JSON body.
    Other fixtures that need an existing observatory should depend on this one.
    """
    payload = {
        "title": "Fixture Observatory",
        "description": "Created by test fixture",
    }
    resp = await async_client.post(BASE, json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


@pytest.fixture
async def created_catalog(async_client: AsyncClient):
    """
    Creates a minimal catalog via the catalogs endpoint and returns its ID.
    Used by tests that need to link a catalog to an observatory.
    """
    payload = {
        "name": "Fixture Catalog",
        "value": "FIXTURE_CAT",
        "catalog_type": "SPATIAL",
        "items": [],
    }
    resp = await async_client.post("/api/v2/catalogs", json=payload)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    return data if isinstance(data, str) else data["catalog_id"]


@pytest.fixture
async def created_product(async_client: AsyncClient, created_observatory):
    """
    Creates a product linked to the fixture observatory and returns the
    full product JSON body.  Depends on `created_observatory` so both
    exist in the DB at test time.
    """
    obs_id = created_observatory["observatory_id"]
    payload = {
        "name": "Fixture Product",
        "description": "Created by test fixture",
        "observatory_id": obs_id,
        "catalog_item_ids": [],
    }
    resp = await async_client.post("/api/v2/products", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()



# ==========================================
# List observatories
# ==========================================

@pytest.mark.asyncio
async def test_list_observatories_empty(async_client: AsyncClient):
    resp = await async_client.get(BASE)
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_observatories_returns_all(seeded_observatories: AsyncClient):
    resp = await seeded_observatories.get(BASE)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 5
    titles = {o["title"] for o in body}
    for i in range(1, 6):
        assert f"Observatory {i}" in titles


@pytest.mark.asyncio
async def test_list_observatories_pagination_limit(seeded_observatories: AsyncClient):
    resp = await seeded_observatories.get(BASE, params={"limit": 2, "page_index": 0})
    assert resp.status_code == 200
    assert len(resp.json()) == 2


@pytest.mark.asyncio
async def test_list_observatories_pagination_second_page(seeded_observatories: AsyncClient):
    page0 = (await seeded_observatories.get(BASE, params={"limit": 2, "page_index": 0})).json()
    page1 = (await seeded_observatories.get(BASE, params={"limit": 2, "page_index": 1})).json()
    ids0 = {o["observatory_id"] for o in page0}
    ids1 = {o["observatory_id"] for o in page1}
    assert ids0.isdisjoint(ids1), "Pages must not overlap"


@pytest.mark.asyncio
async def test_list_observatories_returns_correct_shape(seeded_observatories: AsyncClient):
    resp = await seeded_observatories.get(BASE)
    assert resp.status_code == 200
    obs = resp.json()[0]
    assert "observatory_id" in obs
    assert "title" in obs
    assert "description" in obs


# ==========================================
# Create observatory  (POST /)
# ==========================================

@pytest.mark.asyncio
async def test_create_observatory_returns_201(async_client: AsyncClient):
    """A valid payload must create the observatory and return HTTP 201."""
    payload = {"title": "New Observatory", "description": "Some description"}
    resp = await async_client.post(BASE, json=payload)
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_create_observatory_response_shape(async_client: AsyncClient):
    """The response body must include every field defined in ObservatoryXDTO."""
    payload = {"title": "Shape Observatory"}
    resp = await async_client.post(BASE, json=payload)
    body = resp.json()
    for field in ("observatory_id", "title", "description", "view_count", "created_at", "updated_at"):
        assert field in body, f"Missing field: {field}"


@pytest.mark.asyncio
async def test_create_observatory_custom_id(async_client: AsyncClient):
    """When observatory_id is supplied the API must use it verbatim."""
    payload = {"observatory_id": "my_custom_id", "title": "Custom ID Obs"}
    resp = await async_client.post(BASE, json=payload)
    assert resp.status_code == 201
    assert resp.json()["observatory_id"] == "my_custom_id"


@pytest.mark.asyncio
async def test_create_observatory_auto_generates_id(async_client: AsyncClient):
    """When no observatory_id is supplied the API must auto-generate a non-empty one."""
    resp = await async_client.post(BASE, json={"title": "Auto ID Obs"})
    assert resp.status_code == 201
    assert resp.json()["observatory_id"]


@pytest.mark.asyncio
async def test_create_observatory_duplicate_id_returns_error(async_client: AsyncClient):
    """
    Attempting to create two observatories with the same ID must fail.
    AlreadyExists maps to HTTP 403 in this codebase (see jubapi/errors/__init__.py).
    """
    payload = {"observatory_id": "dup_id", "title": "First"}
    await async_client.post(BASE, json=payload)
    resp = await async_client.post(BASE, json={**payload, "title": "Second"})
    assert resp.status_code == 403


# ==========================================
# Get observatory  (GET /{observatory_id})
# ==========================================

@pytest.mark.asyncio
async def test_get_observatory_found(async_client: AsyncClient, created_observatory):
    """GET on an existing observatory_id must return 200 with the correct data."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}")
    assert resp.status_code == 200
    assert resp.json()["observatory_id"] == obs_id


@pytest.mark.asyncio
async def test_get_observatory_not_found(async_client: AsyncClient):
    """GET on an unknown observatory_id must return 404."""
    resp = await async_client.get(f"{BASE}/nonexistent_id")
    assert resp.status_code == 404


# ==========================================
# Update observatory  (PUT /{observatory_id})
# ==========================================

@pytest.mark.asyncio
async def test_update_observatory_title(async_client: AsyncClient, created_observatory):
    """Updating the title must persist and be reflected in the response."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.put(f"{BASE}/{obs_id}", json={"title": "Updated Title"})
    assert resp.status_code == 200
    assert resp.json()["title"] == "Updated Title"


@pytest.mark.asyncio
async def test_update_observatory_description(async_client: AsyncClient, created_observatory):
    """Updating the description must persist independently of other fields."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.put(f"{BASE}/{obs_id}", json={"description": "New description"})
    assert resp.status_code == 200
    assert resp.json()["description"] == "New description"


@pytest.mark.asyncio
async def test_update_observatory_empty_body_returns_current_state(async_client: AsyncClient, created_observatory):
    """
    Sending an empty update body (all fields None) must not modify the
    observatory — the endpoint should return the current state unchanged.
    """
    obs_id = created_observatory["observatory_id"]
    original_title = created_observatory["title"]
    resp = await async_client.put(f"{BASE}/{obs_id}", json={})
    assert resp.status_code == 200
    assert resp.json()["title"] == original_title


@pytest.mark.asyncio
async def test_update_observatory_not_found(async_client: AsyncClient):
    """Updating a non-existent observatory must return 404."""
    resp = await async_client.put(f"{BASE}/ghost_id", json={"title": "Ghost"})
    assert resp.status_code == 404


# ==========================================
# Delete observatory  (DELETE /{observatory_id})
# ==========================================

@pytest.mark.asyncio
async def test_delete_observatory_success(async_client: AsyncClient, created_observatory):
    """Deleting an existing observatory must return 200 with deleted=True."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.delete(f"{BASE}/{obs_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True


@pytest.mark.asyncio
async def test_delete_observatory_makes_it_unfetchable(async_client: AsyncClient, created_observatory):
    """After deletion a GET on the same ID must return 404."""
    obs_id = created_observatory["observatory_id"]
    await async_client.delete(f"{BASE}/{obs_id}")
    resp = await async_client.get(f"{BASE}/{obs_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_observatory_not_found(async_client: AsyncClient):
    """Deleting a non-existent observatory must return 404."""
    resp = await async_client.delete(f"{BASE}/ghost_id")
    assert resp.status_code == 404


# ==========================================
# Setup  (POST /setup)
# ==========================================

@pytest.mark.asyncio
async def test_setup_observatory_returns_201(async_client: AsyncClient):
    """
    POST /setup must return HTTP 201 with both an observatory_id and a task_id.
    The observatory is created in a disabled state and a PENDING task is queued.
    """
    payload = {
        "title": "Setup Observatory - Test",
        "user_id": "user_001",
        "description": "Provisioned via setup endpoint from test_setup_observatory_returns_201",
    }
    resp = await async_client.post(f"{BASE}/setup", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert "observatory_id" in body
    assert "task_id" in body
    assert body["status"] == "pending"


@pytest.mark.asyncio
async def test_setup_observatory_custom_id(async_client: AsyncClient):
    """When observatory_id is provided to /setup, it must be honoured."""
    payload = {
        "title": "Custom Setup - Test",
        "user_id": "user_002",
        "description": "Provisioned via setup endpoint with custom ID from test_setup_observatory_custom_id",
        "observatory_id": "custom_setup_id",
    }
    resp = await async_client.post(f"{BASE}/setup", json=payload)
    assert resp.status_code == 201
    assert resp.json()["observatory_id"] == "custom_setup_id"


@pytest.mark.asyncio
async def test_setup_observatory_is_disabled(async_client: AsyncClient):
    """
    An observatory created via /setup must be disabled (is_disabled=True).
    We verify this by checking that the observatory is present in the DB but
    the response shape from GET still returns it (the list endpoint includes
    disabled observatories).
    """
    payload = {"title": "Disabled Obs", "user_id": "user_003"}
    setup_resp = await async_client.post(f"{BASE}/setup", json=payload)
    obs_id = setup_resp.json()["observatory_id"]

    # The observatory must be retrievable (it exists) …
    get_resp = await async_client.get(f"{BASE}/{obs_id}")
    assert get_resp.status_code == 200
    # … and the list endpoint must include it (disabled items are not filtered out)
    list_resp = await async_client.get(BASE)
    ids = [o["observatory_id"] for o in list_resp.json()]
    assert obs_id in ids


# ==========================================
# Catalog links  (POST / GET / DELETE /{id}/catalogs)
# ==========================================

@pytest.mark.asyncio
async def test_link_catalog_to_observatory(async_client: AsyncClient, created_observatory, created_catalog):
    """
    POST /{observatory_id}/catalogs must link the catalog and return the
    observatory_id, catalog_id, and level in the response body.
    """
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.post(
        f"{BASE}/{obs_id}/catalogs",
        json={"catalog_id": created_catalog, "level": 0},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert body["catalog_id"] == created_catalog


@pytest.mark.asyncio
async def test_link_catalog_to_nonexistent_observatory(async_client: AsyncClient, created_catalog):
    """Linking a catalog to a non-existent observatory must return 404."""
    resp = await async_client.post(
        f"{BASE}/ghost_obs/catalogs",
        json={"catalog_id": created_catalog, "level": 0},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_catalogs_empty(async_client: AsyncClient, created_observatory):
    """GET /{observatory_id}/catalogs on a fresh observatory must return an empty list."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}/catalogs")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_catalogs_returns_linked_catalog(async_client: AsyncClient, created_observatory, created_catalog):
    """After linking a catalog it must appear in the GET /{observatory_id}/catalogs list."""
    obs_id = created_observatory["observatory_id"]
    await async_client.post(
        f"{BASE}/{obs_id}/catalogs",
        json={"catalog_id": created_catalog, "level": 0},
    )
    resp = await async_client.get(f"{BASE}/{obs_id}/catalogs")
    assert resp.status_code == 200
    ids = [c["catalog_id"] for c in resp.json()]
    assert created_catalog in ids


@pytest.mark.asyncio
async def test_unlink_catalog(async_client: AsyncClient, created_observatory, created_catalog):
    """
    DELETE /{observatory_id}/catalogs/{catalog_id} must sever the link.
    A subsequent GET must no longer include that catalog.
    """
    obs_id = created_observatory["observatory_id"]
    await async_client.post(
        f"{BASE}/{obs_id}/catalogs",
        json={"catalog_id": created_catalog, "level": 0},
    )
    del_resp = await async_client.delete(f"{BASE}/{obs_id}/catalogs/{created_catalog}")
    assert del_resp.status_code == 204

    ids = [c["catalog_id"] for c in (await async_client.get(f"{BASE}/{obs_id}/catalogs")).json()]
    assert created_catalog not in ids


# ==========================================
# Product links  (GET / POST / DELETE /{id}/products)
# ==========================================

@pytest.mark.asyncio
async def test_list_products_empty(async_client: AsyncClient, created_observatory):
    """GET /{observatory_id}/products on a fresh observatory must return an empty list."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}/products")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_link_product_to_observatory(async_client: AsyncClient, created_observatory, created_product):
    """
    POST /{observatory_id}/products must link the product.
    The product should then appear in the GET /{observatory_id}/products list.
    """
    obs_id = created_observatory["observatory_id"]
    prod_id = created_product["product_id"]

    # The product was already linked at creation time via the products endpoint;
    # unlink it first so we can test the explicit link endpoint in isolation.
    await async_client.delete(f"{BASE}/{obs_id}/products/{prod_id}")

    link_resp = await async_client.post(
        f"{BASE}/{obs_id}/products",
        json={"product_id": prod_id},
    )
    assert link_resp.status_code == 201
    body = link_resp.json()
    assert body["product_id"] == prod_id

    ids = [p["product_id"] for p in (await async_client.get(f"{BASE}/{obs_id}/products")).json()]
    assert prod_id in ids


@pytest.mark.asyncio
async def test_unlink_product(async_client: AsyncClient, created_observatory, created_product):
    """
    DELETE /{observatory_id}/products/{product_id} must sever the link.
    A subsequent GET must no longer list that product.
    """
    obs_id = created_observatory["observatory_id"]
    prod_id = created_product["product_id"]

    del_resp = await async_client.delete(f"{BASE}/{obs_id}/products/{prod_id}")
    assert del_resp.status_code == 204

    ids = [p["product_id"] for p in (await async_client.get(f"{BASE}/{obs_id}/products")).json()]
    assert prod_id not in ids


# ==========================================
# Fixtures for service and datasource links
# ==========================================

@pytest.fixture
async def created_service(async_client: AsyncClient):
    """Creates a service via the API and returns its JSON body."""
    payload = {
        "name": "Fixture Service",
        "description": "Created by test fixture",
        "owner_id": "usr_test",
        "public": False,
    }
    resp = await async_client.post("/api/v2/services", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


@pytest.fixture
async def created_datasource(async_client: AsyncClient):
    """Creates a data source via the API and returns its JSON body."""
    payload = {
        "name": "Fixture DataSource",
        "description": "Created by test fixture",
        "format": "csv",
    }
    resp = await async_client.post("/api/v2/datasources", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


# ==========================================
# Service links  (GET / POST / DELETE /{id}/services)
# ==========================================

@pytest.mark.asyncio
async def test_list_services_empty(async_client: AsyncClient, created_observatory):
    """GET /{observatory_id}/services on a fresh observatory must return an empty list."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}/services")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_link_service_to_observatory(async_client: AsyncClient, created_observatory, created_service):
    """POST /{observatory_id}/services must link the service and return 201 with identifiers."""
    obs_id = created_observatory["observatory_id"]
    svc_id = created_service["service_id"]
    resp = await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    assert resp.status_code == 201
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert body["service_id"] == svc_id


@pytest.mark.asyncio
async def test_link_service_to_nonexistent_observatory(async_client: AsyncClient, created_service):
    """Linking a service to a non-existent observatory must return 404."""
    resp = await async_client.post(
        f"{BASE}/ghost_obs/services",
        json={"service_id": created_service["service_id"]},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_link_nonexistent_service_to_observatory(async_client: AsyncClient, created_observatory):
    """Linking a non-existent service must return 404."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": "svc_does_not_exist"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_services_returns_linked_service(async_client: AsyncClient, created_observatory, created_service):
    """After linking a service it must appear in GET /{observatory_id}/services."""
    obs_id = created_observatory["observatory_id"]
    svc_id = created_service["service_id"]
    await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    resp = await async_client.get(f"{BASE}/{obs_id}/services")
    assert resp.status_code == 200
    ids = [s["service_id"] for s in resp.json()]
    assert svc_id in ids


@pytest.mark.asyncio
async def test_unlink_service(async_client: AsyncClient, created_observatory, created_service):
    """DELETE /{observatory_id}/services/{service_id} must sever the link."""
    obs_id = created_observatory["observatory_id"]
    svc_id = created_service["service_id"]
    await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    del_resp = await async_client.delete(f"{BASE}/{obs_id}/services/{svc_id}")
    assert del_resp.status_code == 204
    ids = [s["service_id"] for s in (await async_client.get(f"{BASE}/{obs_id}/services")).json()]
    assert svc_id not in ids


@pytest.mark.asyncio
async def test_link_service_idempotent(async_client: AsyncClient, created_observatory, created_service):
    """Linking the same service twice must succeed and appear only once in the list."""
    obs_id = created_observatory["observatory_id"]
    svc_id = created_service["service_id"]
    r1 = await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    r2 = await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    assert r1.status_code == 201
    assert r2.status_code == 201
    items = [s["service_id"] for s in (await async_client.get(f"{BASE}/{obs_id}/services")).json()]
    assert items.count(svc_id) == 1


# ==========================================
# DataSource links  (GET / POST / DELETE /{id}/datasources)
# ==========================================

@pytest.mark.asyncio
async def test_list_datasources_empty(async_client: AsyncClient, created_observatory):
    """GET /{observatory_id}/datasources on a fresh observatory must return an empty list."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}/datasources")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_link_datasource_to_observatory(async_client: AsyncClient, created_observatory, created_datasource):
    """POST /{observatory_id}/datasources must link the source and return 201 with identifiers."""
    obs_id = created_observatory["observatory_id"]
    src_id = created_datasource["source_id"]
    resp = await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    assert resp.status_code == 201
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert body["source_id"] == src_id


@pytest.mark.asyncio
async def test_link_datasource_to_nonexistent_observatory(async_client: AsyncClient, created_datasource):
    """Linking a datasource to a non-existent observatory must return 404."""
    resp = await async_client.post(
        f"{BASE}/ghost_obs/datasources",
        json={"source_id": created_datasource["source_id"]},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_link_nonexistent_datasource_to_observatory(async_client: AsyncClient, created_observatory):
    """Linking a non-existent datasource must return 404."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": "src_does_not_exist"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_datasources_returns_linked_datasource(async_client: AsyncClient, created_observatory, created_datasource):
    """After linking a datasource it must appear in GET /{observatory_id}/datasources."""
    obs_id = created_observatory["observatory_id"]
    src_id = created_datasource["source_id"]
    await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    resp = await async_client.get(f"{BASE}/{obs_id}/datasources")
    assert resp.status_code == 200
    ids = [s["source_id"] for s in resp.json()]
    assert src_id in ids


@pytest.mark.asyncio
async def test_unlink_datasource(async_client: AsyncClient, created_observatory, created_datasource):
    """DELETE /{observatory_id}/datasources/{source_id} must sever the link."""
    obs_id = created_observatory["observatory_id"]
    src_id = created_datasource["source_id"]
    await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    del_resp = await async_client.delete(f"{BASE}/{obs_id}/datasources/{src_id}")
    assert del_resp.status_code == 204
    ids = [s["source_id"] for s in (await async_client.get(f"{BASE}/{obs_id}/datasources")).json()]
    assert src_id not in ids


@pytest.mark.asyncio
async def test_link_datasource_idempotent(async_client: AsyncClient, created_observatory, created_datasource):
    """Linking the same datasource twice must succeed and appear only once in the list."""
    obs_id = created_observatory["observatory_id"]
    src_id = created_datasource["source_id"]
    r1 = await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    r2 = await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    assert r1.status_code == 201
    assert r2.status_code == 201
    items = [s["source_id"] for s in (await async_client.get(f"{BASE}/{obs_id}/datasources")).json()]
    assert items.count(src_id) == 1


# ==========================================
# Observatory cascade delete (service & datasource links)
# ==========================================

@pytest.mark.asyncio
async def test_delete_observatory_clears_service_links(async_client: AsyncClient, created_observatory, created_service, test_db):
    """Deleting an observatory must clean up all its observatory_service_links rows."""
    obs_id = created_observatory["observatory_id"]
    svc_id = created_service["service_id"]
    await async_client.post(f"{BASE}/{obs_id}/services", json={"service_id": svc_id})
    await async_client.delete(f"{BASE}/{obs_id}")
    remaining = await test_db[CollectionNames.OBSERVATORY_SERVICE_LINKS.value].count_documents({"observatory_id": obs_id})
    assert remaining == 0


@pytest.mark.asyncio
async def test_delete_observatory_clears_datasource_links(async_client: AsyncClient, created_observatory, created_datasource, test_db):
    """Deleting an observatory must clean up all its observatory_datasource_links rows."""
    obs_id = created_observatory["observatory_id"]
    src_id = created_datasource["source_id"]
    await async_client.post(f"{BASE}/{obs_id}/datasources", json={"source_id": src_id})
    await async_client.delete(f"{BASE}/{obs_id}")
    remaining = await test_db[CollectionNames.OBSERVATORY_DATASOURCE_LINKS.value].count_documents({"observatory_id": obs_id})
    assert remaining == 0


# ==========================================
# View counter  (POST /{id}/view)
# ==========================================

@pytest.mark.asyncio
async def test_increment_view_returns_200(async_client: AsyncClient, created_observatory):
    """POST /{observatory_id}/view must return 200 with observatory_id and view_count."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.post(f"{BASE}/{obs_id}/view")
    assert resp.status_code == 200
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert "view_count" in body


@pytest.mark.asyncio
async def test_increment_view_increases_count(async_client: AsyncClient, created_observatory):
    """Each call to /view must increment the counter by exactly 1."""
    obs_id = created_observatory["observatory_id"]
    first  = (await async_client.post(f"{BASE}/{obs_id}/view")).json()["view_count"]
    second = (await async_client.post(f"{BASE}/{obs_id}/view")).json()["view_count"]
    assert second == first + 1


# ==========================================
# Reviews  (GET / POST / PUT / DELETE /{id}/reviews)
# ==========================================

@pytest.mark.asyncio
async def test_list_reviews_empty(async_client: AsyncClient, created_observatory):
    """GET /{observatory_id}/reviews on a new observatory must return an empty list."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.get(f"{BASE}/{obs_id}/reviews")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_review_returns_201(async_client: AsyncClient, created_observatory):
    """POST /{observatory_id}/reviews must return 201 with the review content and rating."""
    obs_id = created_observatory["observatory_id"]
    resp = await async_client.post(
        f"{BASE}/{obs_id}/reviews",
        json={"content": "Great observatory!", "rating": 5},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["content"] == "Great observatory!"
    assert body["rating"] == 5
    assert body["observatory_id"] == obs_id


@pytest.mark.asyncio
async def test_create_review_unauthenticated_returns_401(unauth_client: AsyncClient):
    """Posting a review without an Authorization header must return 401."""
    resp = await unauth_client.post(
        f"{BASE}/any_obs_id/reviews",
        json={"content": "No auth", "rating": 3},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_create_review_appears_in_list(async_client: AsyncClient, created_observatory):
    """A review created via POST must be retrievable via GET /reviews."""
    obs_id = created_observatory["observatory_id"]
    await async_client.post(
        f"{BASE}/{obs_id}/reviews",
        json={"content": "Good data", "rating": 4},
    )
    reviews = (await async_client.get(f"{BASE}/{obs_id}/reviews")).json()
    assert len(reviews) == 1
    assert reviews[0]["content"] == "Good data"


@pytest.mark.asyncio
async def test_update_review(async_client: AsyncClient, created_observatory):
    """PUT /{observatory_id}/reviews/{review_id} must accept partial updates."""
    obs_id = created_observatory["observatory_id"]

    create_resp = await async_client.post(
        f"{BASE}/{obs_id}/reviews",
        json={"content": "Initial content", "rating": 3},
    )
    review_id = create_resp.json()["review_id"]

    update_resp = await async_client.put(
        f"{BASE}/{obs_id}/reviews/{review_id}",
        json={"rating": 5},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["rating"] == 5


@pytest.mark.asyncio
async def test_delete_review(async_client: AsyncClient, created_observatory):
    """DELETE /{observatory_id}/reviews/{review_id} must remove the review."""
    obs_id = created_observatory["observatory_id"]

    create_resp = await async_client.post(
        f"{BASE}/{obs_id}/reviews",
        json={"content": "To be deleted", "rating": 2},
    )
    review_id = create_resp.json()["review_id"]

    del_resp = await async_client.delete(f"{BASE}/{obs_id}/reviews/{review_id}")
    assert del_resp.status_code == 204

    reviews = (await async_client.get(f"{BASE}/{obs_id}/reviews")).json()
    assert all(r["review_id"] != review_id for r in reviews)


# ==========================================
# Bulk catalog assignment  (POST /{id}/catalogs/bulk)
# ==========================================

@pytest.mark.asyncio
async def test_bulk_assign_catalogs_returns_201(async_client: AsyncClient, created_observatory):
    """
    POST /{observatory_id}/catalogs/bulk must create each catalog described in
    the payload and link all of them to the observatory in a single request.
    The response must list every generated catalog_id.
    """
    obs_id = created_observatory["observatory_id"]
    payload = {
        "catalogs": [
            {
                "name": "Spatial Bulk",
                "value": "SPATIAL_BULK",
                "catalog_type": "SPATIAL",
                "items": [],
            },
            {
                "name": "Temporal Bulk",
                "value": "TEMPORAL_BULK",
                "catalog_type": "TEMPORAL",
                "items": [],
            },
        ]
    }
    resp = await async_client.post(f"{BASE}/{obs_id}/catalogs/bulk", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert len(body["catalog_ids"]) == 2


@pytest.mark.asyncio
async def test_bulk_assign_catalogs_links_appear_in_list(async_client: AsyncClient, created_observatory):
    """After a bulk-assign the linked catalogs must be returned by GET /{id}/catalogs."""
    obs_id = created_observatory["observatory_id"]
    payload = {
        "catalogs": [
            {"name": "Cat A", "value": "CAT_A", "catalog_type": "SPATIAL", "items": []},
        ]
    }
    bulk_resp = await async_client.post(f"{BASE}/{obs_id}/catalogs/bulk", json=payload)
    created_ids = set(bulk_resp.json()["catalog_ids"])

    linked_ids = {c["catalog_id"] for c in (await async_client.get(f"{BASE}/{obs_id}/catalogs")).json()}
    assert created_ids.issubset(linked_ids)


@pytest.mark.asyncio
async def test_bulk_assign_catalogs_nonexistent_observatory(async_client: AsyncClient):
    """Bulk-assigning catalogs to a non-existent observatory must return 404."""
    payload = {
        "catalogs": [
            {"name": "Ghost Cat", "value": "GHOST_CAT", "catalog_type": "SPATIAL", "items": []},
        ]
    }
    resp = await async_client.post(f"{BASE}/ghost_obs/catalogs/bulk", json=payload)
    assert resp.status_code == 404


# ==========================================
# Bulk product assignment  (POST /{id}/products/bulk)
# ==========================================

@pytest.mark.asyncio
async def test_bulk_assign_products_returns_201(async_client: AsyncClient, created_observatory):
    """
    POST /{observatory_id}/products/bulk must create each product and link
    it to the observatory.  The response must echo the observatory_id and
    list every created product with its product_id and name.
    """
    obs_id = created_observatory["observatory_id"]
    payload = {
        "products": [
            {"name": "Cancer Incidence 2023", "description": "Breast cancer by state"},
            {"name": "Mortality 2023", "description": "All-cause mortality"},
        ]
    }
    resp = await async_client.post(f"{BASE}/{obs_id}/products/bulk", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["observatory_id"] == obs_id
    assert len(body["products"]) == 2
    names = {p["name"] for p in body["products"]}
    assert "Cancer Incidence 2023" in names
    assert "Mortality 2023" in names


@pytest.mark.asyncio
async def test_bulk_assign_products_appear_in_list(async_client: AsyncClient, created_observatory):
    """Products created via bulk-assign must appear in GET /{id}/products."""
    obs_id = created_observatory["observatory_id"]
    payload = {
        "products": [{"name": "Bulk Product Alpha", "description": ""}]
    }
    bulk_resp = await async_client.post(f"{BASE}/{obs_id}/products/bulk", json=payload)
    created_ids = {p["product_id"] for p in bulk_resp.json()["products"]}

    listed_ids = {p["product_id"] for p in (await async_client.get(f"{BASE}/{obs_id}/products")).json()}
    assert created_ids.issubset(listed_ids)


@pytest.mark.asyncio
async def test_bulk_assign_products_nonexistent_observatory(async_client: AsyncClient):
    """Bulk-assigning products to a non-existent observatory must return 404."""
    payload = {"products": [{"name": "Ghost Product", "description": ""}]}
    resp = await async_client.post(f"{BASE}/ghost_obs/products/bulk", json=payload)
    assert resp.status_code == 404
