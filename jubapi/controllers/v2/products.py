import os
import time
import mimetypes
from typing import List, Optional
from fastapi import Depends, Query, Request, status, UploadFile, File, Form, BackgroundTasks, HTTPException
from fastapi.responses import Response, StreamingResponse
from fastapi.routing import APIRouter
from nanoid import generate as nanoid

import jubapi.services.v2 as S
import jubapi.middlewares as MX
import jubapi.models.v2 as M
import jubapi.dto.v2 as DTO
import jubapi.enums.v2 as ENUMS
from jubapi.storage import StorageBackend
from jubapi.log.log import Log

router = APIRouter(prefix="/products", tags=["products_v2"])

log = Log(name=__name__, path=os.environ.get("JUB_LOG_PATH", "/log"))


# ==========================================
# CRUD
# ==========================================

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DTO.ProductSimpleDTO,
    summary="Create a single product",
    description=(
        "Creates one product and links it to an observatory with optional catalog-item tags. "
        "Use **POST /observatories/{id}/products/bulk** to create multiple products at once."
    ),
)
async def create_product(
    payload: DTO.ProductCreateDTO,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    product_id = payload.product_id or nanoid(size=12)
    model = M.ProductX(
        product_id  = product_id,
        name        = payload.name,
        description = payload.description or "",
    )
    result = await svc.insert_product(
        product          = model,
        observatory_id   = payload.observatory_id,
        catalog_item_ids = payload.catalog_item_ids or [],
    )
    if result.is_err:
        log.error({"action": "controller.product.create", "error": str(result.unwrap_err().detail), "input": {"name": payload.name, "observatory_id": payload.observatory_id}})
        raise result.unwrap_err().to_http_exception()
    fetched = await svc.get_product_by_id(result.unwrap())
    if fetched.is_err:
        log.error({"action": "controller.product.create", "error": str(fetched.unwrap_err().detail), "input": {"product_id": result.unwrap()}})
        raise fetched.unwrap_err().to_http_exception()
    log.info({"action": "controller.product.create", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"product_id": result.unwrap()}})
    return DTO.ProductSimpleDTO.from_model(fetched.unwrap())


@router.get("", response_model=List[DTO.ProductSimpleDTO])
async def list_products(
    limit: int = Query(100, ge=1, le=500),
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.list_products(limit=limit)
    if result.is_err:
        log.error({"action": "controller.product.list", "error": str(result.unwrap_err().detail)})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.product.list", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(data)}})
    return data


@router.get(
    "/filter",
    response_model=List[DTO.ProductSimpleDTO],
    summary="Filter products by metadata",
    description=(
        "Returns products whose metadata matches **all** supplied key-value query parameters. "
        "Any string key is accepted — values are matched exactly. "
        "Example: `GET /products/filter?extension=csv&format=parquet`"
    ),
)
async def filter_products(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results to return."),
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Filter products by arbitrary metadata key-value pairs.

    All query parameters (except `limit`) are treated as metadata filters.
    Multiple parameters are combined with AND — only products matching every
    supplied pair are returned.

    **Examples**

    - `GET /products/filter?extension=csv` — products with metadata.extension = "csv"
    - `GET /products/filter?extension=csv&format=parquet` — AND match on two keys
    """
    filters = {k: v for k, v in request.query_params.items() if k != "limit"}
    result = await svc.filter_products_by_metadata(filters, limit=limit)
    if result.is_err:
        log.error({"action": "controller.product.filter", "error": str(result.unwrap_err().detail), "input": filters})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.product.filter", "result": {"count": len(data)}, "input": filters})
    return data


@router.get("/{product_id}", response_model=DTO.ProductSimpleDTO)
async def get_product(
    product_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.get_product_by_id(product_id)
    if result.is_err:
        log.error({"action": "controller.product.get", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.product.get", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"product_id": product_id}})
    return DTO.ProductSimpleDTO.from_model(result.unwrap())


@router.put("/{product_id}", response_model=DTO.ProductSimpleDTO)
async def update_product(
    product_id: str,
    payload: DTO.ProductUpdateDTO,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not update_data:
        result = await svc.get_product_by_id(product_id)
        if result.is_err:
            log.error({"action": "controller.product.update", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id}})
            raise result.unwrap_err().to_http_exception()
        return DTO.ProductSimpleDTO.from_model(result.unwrap())
    result = await svc.update_product(product_id, update_data)
    if result.is_err:
        log.error({"action": "controller.product.update", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.product.update", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"product_id": product_id}})
    return result.unwrap()


@router.delete("/{product_id}", response_model=DTO.ProductDeleteResponseDTO)
async def delete_product(
    product_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.delete_product(product_id)
    if result.is_err:
        log.error({"action": "controller.product.delete", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.product.delete", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"product_id": product_id}})
    return DTO.ProductDeleteResponseDTO(deleted=result.unwrap())


# ==========================================
# Catalog-item tag management
# ==========================================

@router.get("/{product_id}/tags")
async def get_tags(
    product_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    # Ensure product exists
    check = await svc.get_product_by_id(product_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.get_product_tags(product_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"product_id": product_id, "catalog_item_ids": result.unwrap()}


@router.get("/{product_id}/tags/details", response_model=List[DTO.CatalogItemXResponseDTO])
async def get_tag_details(
    product_id: str,
    prod_svc: S.ProductService = Depends(MX.get_product_service),
    cat_svc:  S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await prod_svc.get_product_by_id(product_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    tags_result = await prod_svc.get_product_tags(product_id)
    if tags_result.is_err:
        raise tags_result.unwrap_err().to_http_exception()

    items = []
    for item_id in tags_result.unwrap():
        item_result = await cat_svc.get_catalog_item(item_id)
        if item_result.is_ok:
            items.append(DTO.CatalogItemXResponseDTO.from_model(item_result.unwrap()))
    return items


@router.post("/{product_id}/tags", status_code=status.HTTP_201_CREATED)
async def add_tags(
    product_id: str,
    payload: DTO.TagProductDTO,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    result = await svc.tag_product(product_id, payload.catalog_item_ids)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"product_id": product_id, "added": result.unwrap()}


@router.post(
    "/{product_id}/tags/catalog/{catalog_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=DTO.BulkTagFromCatalogResponseDTO,
    summary="Wildcard-assign all catalog items to a product",
    description="Links a product to every item currently in the given catalog in one shot.",
)
async def bulk_tag_from_catalog(
    product_id: str,
    catalog_id: str,
    prod_svc: S.ProductService = Depends(MX.get_product_service),
    cat_svc:  S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    catalog_check = await cat_svc.get_catalog_details(catalog_id)
    if catalog_check.is_err:
        raise catalog_check.unwrap_err().to_http_exception()
    result = await prod_svc.tag_product_from_catalog(product_id, catalog_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return DTO.BulkTagFromCatalogResponseDTO(
        product_id=product_id,
        catalog_id=catalog_id,
        linked_items=result.unwrap(),
    )


@router.delete("/{product_id}/tags/{catalog_item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_tag(
    product_id: str,
    catalog_item_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_product_by_id(product_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.untag_product(product_id, catalog_item_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()


# ==========================================
# Related products
# ==========================================

@router.get(
    "/{product_id}/related",
    response_model=List[DTO.ProductSimpleDTO],
    summary="List related products",
    description="Returns all products that have been explicitly related to this product. The relationship is symmetric — relating A to B means both A→B and B→A are visible.",
)
async def get_related_products(
    product_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Returns all products related to the given product.

    Relationships are symmetric: if product A is related to product B,
    querying either A or B returns the other.
    """
    check = await svc.get_product_by_id(product_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()
    result = await svc.get_related_products(product_id)
    if result.is_err:
        log.error({"action": "controller.product.related.list", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id}})
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.post(
    "/{product_id}/related",
    status_code=status.HTTP_201_CREATED,
    summary="Add a related product",
    description="Creates a symmetric relationship between two products. Relating a product to itself or creating a duplicate link is a no-op.",
)
async def add_related_product(
    product_id: str,
    payload: DTO.RelateProductDTO,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Relates two products to each other.

    The relationship is symmetric — no distinction between source and target.
    Sending the same pair twice is safe (idempotent upsert).
    """
    result = await svc.relate_products(product_id, payload.related_product_id)
    if result.is_err:
        log.error({"action": "controller.product.related.add", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id, "related_product_id": payload.related_product_id}})
        raise result.unwrap_err().to_http_exception()
    return {"product_id": product_id, "related_product_id": payload.related_product_id}


@router.delete(
    "/{product_id}/related/{related_product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a related product",
    description="Removes the relationship between two products. If the relationship does not exist this is a no-op.",
)
async def remove_related_product(
    product_id: str,
    related_product_id: str,
    svc: S.ProductService = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """Removes the symmetric relationship between two products."""
    result = await svc.unrelate_products(product_id, related_product_id)
    if result.is_err:
        log.error({"action": "controller.product.related.remove", "error": str(result.unwrap_err().detail), "input": {"product_id": product_id, "related_product_id": related_product_id}})
        raise result.unwrap_err().to_http_exception()


# ==========================================
# FILE UPLOAD  (queued background ingestion)
# ==========================================

async def _process_upload(
    product_id: str,
    job_id:     str,
    filename:   str,
    data:       bytes,
    storage:    StorageBackend,
    task_svc:   S.TasksService,
    prod_svc:   S.ProductService,
) -> None:
    try:
        key = storage.key_for(product_id, job_id, filename)
        await storage.put(key, data)
        _, ext = os.path.splitext(filename)
        ext = ext.lstrip(".").lower()
        if ext:
            await prod_svc.update_product(product_id, {"metadata.extension": ext})
        await task_svc.complete_task(job_id, success=True)
    except Exception as exc:
        log.error(f"Upload processing failed for product {product_id}: {exc}")
        await task_svc.complete_task(job_id, success=False, error_msg=str(exc))


@router.post("/{product_id}/upload", status_code=status.HTTP_202_ACCEPTED,
             response_model=DTO.ProductUploadResponseDTO)
async def upload_product_file(
    product_id:      str,
    background_tasks: BackgroundTasks,
    file:            UploadFile         = File(..., description="Data file to ingest for this product."),
    current_user:     DTO.UserProfileDTO = Depends(MX.get_current_user),
    prod_svc:        S.ProductService   = Depends(MX.get_product_service),
    task_svc:        S.TasksService     = Depends(MX.get_tasks_service),
    storage:         StorageBackend     = Depends(MX.get_storage_backend),
):
    """
    Accepts a file for a product and queues it for background ingestion.

    Returns immediately with a `job_id` (a task ID).  The background worker
    persists the file via the configured `StorageBackend`, then marks the job
    SUCCESS or FAILED.  Poll `GET /tasks/{job_id}` to track progress.

    The external indexing system should call `POST /tasks/{job_id}/complete`
    once it has finished indexing the stored file.
    """
    t0 = time.monotonic()
    check = await prod_svc.get_product_by_id(product_id)
    if check.is_err:
        log.error({"action": "controller.product.upload", "error": str(check.unwrap_err().detail), "input": {"product_id": product_id}})
        raise check.unwrap_err().to_http_exception()
    product = check.unwrap()

    obs_link = await prod_svc.get_product_observatory(product_id)
    observatory_id = obs_link.unwrap() if obs_link.is_ok else "unknown"

    task_result = await task_svc.create_task(DTO.CreateTaskDTO(
        user_id        = current_user.user_id,
        observatory_id = observatory_id,
        title          = f"Index: {product.name} — {file.filename}",
        description    = f"File ingestion queued for product {product_id}.",
        operation      = ENUMS.TaskOperationEnum.INDEX,
    ))
    if task_result.is_err:
        log.error({"action": "controller.product.upload", "error": str(task_result.unwrap_err().detail), "input": {"product_id": product_id}})
        raise task_result.unwrap_err().to_http_exception()

    job_id = task_result.unwrap()
    data = await file.read()
    background_tasks.add_task(
        _process_upload, product_id, job_id, file.filename, data, storage, task_svc, prod_svc
    )
    log.info({"action": "controller.product.upload", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"product_id": product_id, "filename": file.filename}, "result": {"job_id": job_id}})
    return DTO.ProductUploadResponseDTO(job_id=job_id, product_id=product_id)


@router.get("/{product_id}/download")
async def download_product_file(
    product_id: str,
    request:    Request,
    job_id:     Optional[str]      = Query(None, description="Specific upload job to download. Defaults to the latest."),
    prod_svc:   S.ProductService   = Depends(MX.get_product_service),
    storage:    StorageBackend     = Depends(MX.get_storage_backend),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Downloads a previously uploaded file for a product.
    If *job_id* is omitted the most recently uploaded file is returned.
    """
    t0 = time.monotonic()
    check = await prod_svc.get_product_by_id(product_id)
    if check.is_err:
        log.error({"action": "controller.product.download", "error": str(check.unwrap_err().detail), "input": {"product_id": product_id}})
        raise check.unwrap_err().to_http_exception()

    prefix = storage.key_for(product_id, job_id) if job_id else storage.prefix_for(product_id)
    keys   = await storage.list(prefix)

    if not keys:
        log.error({"action": "controller.product.download", "error": "No files found", "input": {"product_id": product_id, "job_id": job_id}})
        raise HTTPException(status_code=404, detail="No files found for this product.")

    key      = keys[-1]
    etag     = f'"{key}"'

    if request.headers.get("If-None-Match") == etag:
        return Response(status_code=304)

    filename        = key.split("/")[-1]
    data, from_cache = await storage.get(key)
    media_type      = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    log.info({"action": "controller.product.download", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"product_id": product_id}, "result": {"filename": filename, "size_bytes": len(data), "source": "cache" if from_cache else "disk"}})

    chunk_size = 256 * 1024  # 256 KB

    def iter_chunks():
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    return StreamingResponse(
        iter_chunks(),
        media_type = media_type,
        headers    = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length":      str(len(data)),
            "ETag":                etag,
            "Cache-Control":       "private, max-age=300",
        },
    )
