from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
import pytest
from dotenv import load_dotenv
import os
from jubapi.server import app
from uuid import uuid4
import commonx.dto.xolo as XoloDTO
import jubapi.dto.v2 as DTO
import jubapi.middlewares as MX
from jubapi.db.constants import CollectionNames
from typing import Tuple, Dict
from motor.motor_asyncio import AsyncIOMotorClient as MongoClient

# import commonx.dto.e as DTO

JUB_ENV_FILE_PATH = os.environ.get("JUB_ENV_FILE_PATH", ".env.test")
os.environ.setdefault("JUB_ENV_FILE_PATH", JUB_ENV_FILE_PATH)
env_exists        = os.path.exists(JUB_ENV_FILE_PATH)

print(f"Loading environment variables from: {JUB_ENV_FILE_PATH} - Exists: {env_exists}")
if env_exists:
    load_dotenv(JUB_ENV_FILE_PATH, override=True)


_FAKE_USER = DTO.UserProfileDTO(
    user_id    = "test_user_id",
    username   = "testuser",
    fullname   = "Test User",
    first_name = "Test",
    last_name  = "User",
    email      = "testuser@test.com",
    is_disabled= False,
    created_at = "2024-01-01T00:00:00",
    updated_at = "2024-01-01T00:00:00",
    settings   = DTO.UserPreferencesDTO.default(),
)


@pytest.fixture
async def async_client():
    """Creates the async test client with get_current_user overridden to a fake user."""
    app.dependency_overrides[MX.get_current_user] = lambda: _FAKE_USER
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.pop(MX.get_current_user, None)


@pytest.fixture
async def unauth_client():
    """Creates a test client without auth override — use for tests that assert 401."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

# @pytest.fixture()
async def connect_to_database():
    from jubapi.db import connect_to_mongo
    print("Connecting to the database...")
    await connect_to_mongo()
    # await asyncio.sleep(0.1)  # simulate async connection

@pytest.fixture( autouse=True)
async def before_all():
    from jubapi.db import close_mongo_connection
    await connect_to_database()
    print("Database connected before tests")
    yield 
    print("Disconnecting from database...")
    await close_mongo_connection()


@pytest.fixture(scope="function")
async def test_db():
    """
    Sets up a clean MongoDB test database before each test.
    All collections are dropped (awaited) so tests start with a blank slate.
    """
    client = MongoClient("mongodb://localhost:27027/")
    db = client.jub_test

    collections_to_drop = [
        CollectionNames.DATA_SOURCES.value,
        CollectionNames.DATA_RECORDS.value,
        CollectionNames.USER_PROFILES.value,
        CollectionNames.OBSERVATORIES.value,
        CollectionNames.PRODUCTS.value,
        CollectionNames.CATALOGS.value,
        CollectionNames.CATALOG_ITEMS.value,
        CollectionNames.CATALOG_ITEM_ALIASES.value,
        CollectionNames.OBSERVATORY_PRODUCT_LINKS.value,
        CollectionNames.PRODUCT_CATALOGS_ITEM_LINKS.value,
        CollectionNames.CATALOG_ITEM_RELATIONSHIPS.value,
        CollectionNames.CATALOG_CATALOG_ITEM_LINKS.value,
        CollectionNames.CATALOG_ITEM_CATALOG_ALIAS_LINKS.value,
        CollectionNames.OBSERVATORY_CATALOG_LINKS.value,
        CollectionNames.OBSERVATORY_SERVICE_LINKS.value,
        CollectionNames.OBSERVATORY_DATASOURCE_LINKS.value,
        CollectionNames.BUILDING_BLOCKS.value,
        CollectionNames.PATTERNS.value,
        CollectionNames.STAGES.value,
        CollectionNames.WORKFLOWS.value,
        CollectionNames.SERVICES.value,
    ]
    for col in collections_to_drop:
        await db.drop_collection(col)

    yield db



@pytest.fixture()
async def get_current_user() -> Tuple[DTO.UserProfileDTO, Dict[str, str]]:
    """Returns a fake user and empty headers. Auth is handled via dependency_overrides in async_client."""
    return _FAKE_USER, {}