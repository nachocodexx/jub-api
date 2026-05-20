import os
import time
from typing import List
from fastapi.routing import APIRouter
from fastapi import Depends, status
import jubapi.services.v2 as S
import jubapi.middlewares as MX
from jubapi.log.log import Log
import jubapi.dto.v2 as DTO

router = APIRouter(prefix="/catalogs", tags=["catalogs"])

log = Log(
    name = __name__,
    path = os.environ.get("JUB_LOG_PATH", "/log")
)


@router.post("")
async def create_catalog(
    payload: DTO.CatalogCreateDTO, 
    srv: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await srv.create_catalog_bulk(payload)
    if result.is_err:
        e = result.unwrap_err()
        log.error({"action": "controller.catalog.create", "error": str(e.detail), "input": {"name": payload.name}})
        raise e.to_http_exception()
    log.info({"action": "controller.catalog.create", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"catalog_id": result.unwrap()}})
    return DTO.CatalogCreatedResponseDTO(catalog_id=result.unwrap())


@router.post("/bulk")
async def create_catalog_bulk(
    payload: List[DTO.CatalogCreateDTO], 
    srv: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    results = [await srv.create_catalog_bulk(p) for p in payload]
    if any(r.is_err for r in results):
        e = next(r.unwrap_err() for r in results if r.is_err)
        log.error({"action": "controller.catalog.create_bulk", "error": str(e.detail), "input": {"count": len(payload)}})
        raise e.to_http_exception()
    ids = [r.unwrap() for r in results]
    log.info({"action": "controller.catalog.create_bulk", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(ids)}})
    return DTO.CatalogCreatedBulkResponseDTO(catalog_ids=ids)


@router.post("/bulk/{observatory_id}/link")
async def create_catalog_bulk_and_link(
    observatory_id: str,
    payload: List[DTO.CatalogCreateDTO],
    srv: S.CatalogService = Depends(MX.get_catalog_service),
    observatory_srv: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    results = [await srv.create_catalog_bulk(p) for p in payload]
    if any(r.is_err for r in results):
        e = next(r.unwrap_err() for r in results if r.is_err)
        log.error({"action": "controller.catalog.create_bulk_link", "error": str(e.detail), "input": {"observatory_id": observatory_id, "count": len(payload)}})
        raise e.to_http_exception()

    response = DTO.CatalogCreatedBulkResponseDTO(catalog_ids=[r.unwrap() for r in results])
    for catalog_id in response.catalog_ids:
        link_result = await observatory_srv.graph_link_manager.link_observatory_to_catalog(observatory_id, catalog_id)
        if link_result.is_err:
            e = link_result.unwrap_err()
            log.error({"action": "controller.catalog.create_bulk_link", "error": str(e.detail), "input": {"observatory_id": observatory_id, "catalog_id": catalog_id}})
            raise e.to_http_exception()
    log.info({"action": "controller.catalog.create_bulk_link", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"observatory_id": observatory_id, "count": len(response.catalog_ids)}})
    return response


@router.get("", response_model=List[DTO.CatalogSummaryDTO])
async def list_catalogs(
    srv: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    """Returns a lightweight list of all available catalogs."""
    t0 = time.monotonic()
    result = await srv.list_catalogs()
    if result.is_err:
        log.error({"action": "controller.catalog.list", "error": str(result.unwrap_err().detail)})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.catalog.list", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(data)}})
    return data


@router.get("/{catalog_id}", response_model=DTO.CatalogResponseDTO)
async def get_catalog(catalog_id: str, srv: S.CatalogService = Depends(MX.get_catalog_service), _: DTO.UserProfileDTO = Depends(MX.get_current_user)):
    """Fetches a specific catalog with all its items, aliases, and hierarchy populated."""
    t0 = time.monotonic()
    result = await srv.get_catalog_details(catalog_id)
    if result.is_err:
        log.error({"action": "controller.catalog.get", "error": str(result.unwrap_err().detail), "input": {"catalog_id": catalog_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog.get", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_id": catalog_id}})
    return result.unwrap()


@router.put("/{catalog_id}", response_model=DTO.CatalogSummaryDTO)
async def update_catalog(catalog_id: str, payload: DTO.CatalogUpdateDTO, srv: S.CatalogService = Depends(MX.get_catalog_service), _: DTO.UserProfileDTO = Depends(MX.get_current_user)):
    t0 = time.monotonic()
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    result = await srv.update_catalog(catalog_id, data)
    if result.is_err:
        log.error({"action": "controller.catalog.update", "error": str(result.unwrap_err().detail), "input": {"catalog_id": catalog_id}})
        raise result.unwrap_err().to_http_exception()
    cat = result.unwrap()
    log.info({"action": "controller.catalog.update", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_id": catalog_id}})
    return DTO.CatalogSummaryDTO(catalog_id=cat.catalog_id, name=cat.name, value=cat.value, catalog_type=cat.catalog_type)


@router.get("/{catalog_id}/items", response_model=List[DTO.CatalogItemXResponseDTO])
async def get_catalog_items(catalog_id: str, srv: S.CatalogService = Depends(MX.get_catalog_service), _: DTO.UserProfileDTO = Depends(MX.get_current_user)):
    t0 = time.monotonic()
    result = await srv.get_catalog_items(catalog_id)
    if result.is_err:
        log.error({"action": "controller.catalog.get_items", "error": str(result.unwrap_err().detail), "input": {"catalog_id": catalog_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog.get_items", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_id": catalog_id}})
    return result.unwrap()

@router.delete("/{catalog_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_catalog(catalog_id: str, srv: S.CatalogService = Depends(MX.get_catalog_service), _: DTO.UserProfileDTO = Depends(MX.get_current_user)):
    t0 = time.monotonic()
    result = await srv.delete_catalog(catalog_id)
    if result.is_err:
        log.error({"action": "controller.catalog.delete", "error": str(result.unwrap_err().detail), "input": {"catalog_id": catalog_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog.delete", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_id": catalog_id}})
