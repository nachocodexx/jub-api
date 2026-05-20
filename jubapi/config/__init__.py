from dotenv import load_dotenv
import os

JUB_ENV_FILE_PATH = os.environ.get("JUB_ENV_FILE_PATH",".env")
jub_env_exists = os.path.exists(JUB_ENV_FILE_PATH)
print(f"Loading environment variables from: {JUB_ENV_FILE_PATH} - Exists: {jub_env_exists}")
if jub_env_exists:
    load_dotenv(JUB_ENV_FILE_PATH,override=True)

JUB_STORAGE_PATH          = os.environ.get("JUB_STORAGE_PATH","/jub")
JUB_MONGODB_URI           = os.environ.get("JUB_MONGODB_URI","mongodb://localhost:27017/jub")
JUB_MONGODB_DATABASE_NAME = os.environ.get("JUB_MONGODB_DATABASE_NAME","jub")
JUB_OPENAPI_TITLE         = os.environ.get("JUB_OPENAPI_TITLE","OCA - API")
JUB_OPENAPI_VERSION       = os.environ.get("JUB_OPENAPI_VERSION","0.0.1")
JUB_OPENAPI_SUMMARY       = os.environ.get("JUB_OPENAPI_SUMMARY","This API enable the manipulation of observatories and catalogs")
JUB_OPENAPI_DESCRIPTION   = os.environ.get("JUB_OPENAPI_DESCRIPTION","")
JUB_OPENAPI_LOGO          = os.environ.get("JUB_OPENAPI_LOGO","https://i.ibb.co/9vSnz09/android-chrome-192x192.png")
JUB_ROOT_PATH             = os.environ.get("JUB_ROOT_PATH","")
JUB_LOG_DEBUG             = bool(int(os.environ.get("JUB_LOG_DEBUG","1")))
JUB_LOG_NAME              = os.environ.get("JUB_LOG_NAME","jubapi")
JUB_LOG_PATH              = os.environ.get("JUB_LOG_PATH","/log")
JUB_CORS_ORIGINS          = os.environ.get("JUB_CORS_ORIGINS","http://localhost:3100,https://jub.tamps.cinvestav.mx").split(",")
JUB_CORS_METHODS          = os.environ.get("JUB_CORS_METHODS","*").split(",")
JUB_CORS_HEADERS          = os.environ.get("JUB_CORS_HEADERS","*").split(",")
JUB_CORS_CREDENTIALS      = os.environ.get("JUB_CORS_CREDENTIALS","True").lower() in ("true", "1")
JUB_XOLO_API_URL = os.environ.get("JUB_XOLO_API_URL","http://localhost:10000/api/v4")
JUB_XOLO_SECRET   = os.environ.get("JUB_XOLO_SECRET","secret")

JUB_ORPHAN_CHECK_ENABLED          = bool(int(os.environ.get("JUB_ORPHAN_CHECK_ENABLED", "0")))
JUB_ORPHAN_CHECK_INTERVAL_SECONDS = int(os.environ.get("JUB_ORPHAN_CHECK_INTERVAL_SECONDS", "3600"))
JUB_ORPHAN_CHECK_DELETE           = bool(int(os.environ.get("JUB_ORPHAN_CHECK_DELETE", "0")))

JUB_STORAGE_CACHE_MAX_BYTES = int(os.environ.get("JUB_STORAGE_CACHE_MAX_BYTES", str(4 * 1024 * 1024 * 1024)))  # 4 GB
JUB_STORAGE_CACHE_TTL       = int(os.environ.get("JUB_STORAGE_CACHE_TTL", "300"))                              # 5 min

JUB_SEARCH_PRODUCT_CACHE_TTL     = int(os.environ.get("JUB_SEARCH_PRODUCT_CACHE_TTL", "60"))   # 1 min
JUB_SEARCH_OBSERVATORY_CACHE_TTL = int(os.environ.get("JUB_SEARCH_OBSERVATORY_CACHE_TTL", "120"))  # 2 min