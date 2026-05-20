import os
import time
from typing import List
from fastapi import Depends, Query, status
from fastapi.routing import APIRouter
from nanoid import generate as nanoid
import jubapi.services.v2 as S
import jubapi.middlewares as MX
import jubapi.models.v2 as M
import jubapi.dto.v2 as DTO
from jubapi.log.log import Log
import jubapi.enums.v2 as ENUMS

router = APIRouter(prefix="/catalog-items", tags=["catalog_items_v2"])
log = Log(name=__name__, path=os.environ.get("JUB_LOG_PATH", "/log"))


# ==========================================
# CRUD
# ==========================================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=DTO.CatalogItemXResponseDTO)
async def create_catalog_item(
    payload: DTO.CatalogItemStandaloneCreateDTO,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    item_id = payload.catalog_item_id or f"itm_{nanoid(size=8)}"
    model = M.CatalogItemX(
        catalog_item_id = item_id,
        name            = payload.name,
        value           = payload.value,
        code            = payload.code,
        value_type      = payload.value_type,
        temporal_value  = payload.temporal_value,
        description     = payload.description or "",
        catalog_type    = payload.catalog_type or ENUMS.CatalogType.INTEREST,
        metadata        = payload.metadata or {}
    )
    result = await svc.add_item_to_catalog(
        catalog_id = payload.catalog_id,
        item       = model,
        parent_id  = payload.parent_item_id,
    )
    if result.is_err:
        e = result.unwrap_err()
        log.error({"action": "controller.catalog_item.create", "error": str(e.detail), "input": {"catalog_id": payload.catalog_id, "value": payload.value}})
        raise e.to_http_exception()
    fetched = await svc.get_catalog_item(result.unwrap())
    if fetched.is_err:
        e = fetched.unwrap_err()
        log.error({"action": "controller.catalog_item.create", "error": str(e.detail), "input": {"catalog_item_id": result.unwrap()}})
        raise e.to_http_exception()
    log.info({"action": "controller.catalog_item.create", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"catalog_item_id": result.unwrap()}})
    return DTO.CatalogItemXResponseDTO.from_model(fetched.unwrap())


@router.get("", response_model=List[DTO.CatalogItemXResponseDTO])
async def list_catalog_items(
    limit: int = Query(100, ge=1, le=500),
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.list_catalog_items(limit=limit)
    if result.is_err:
        log.error({"action": "controller.catalog_item.list", "error": str(result.unwrap_err().detail)})
        raise result.unwrap_err().to_http_exception()
    data = [DTO.CatalogItemXResponseDTO.from_model(i) for i in result.unwrap()]
    log.info({"action": "controller.catalog_item.list", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(data)}})
    return data


@router.get("/{catalog_item_id}", response_model=DTO.CatalogItemXResponseDTO)
async def get_catalog_item(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.get_catalog_item(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.get", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.get", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}})
    return DTO.CatalogItemXResponseDTO.from_model(result.unwrap())


@router.put("/{catalog_item_id}", response_model=DTO.CatalogItemXResponseDTO)
async def update_catalog_item(
    catalog_item_id: str,
    payload: DTO.CatalogItemUpdateDTO,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not update_data:
        result = await svc.get_catalog_item(catalog_item_id)
        if result.is_err:
            log.error({"action": "controller.catalog_item.update", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
            raise result.unwrap_err().to_http_exception()
        return DTO.CatalogItemXResponseDTO.from_model(result.unwrap())
    result = await svc.update_catalog_item(catalog_item_id, update_data)
    if result.is_err:
        log.error({"action": "controller.catalog_item.update", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.update", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}})
    return DTO.CatalogItemXResponseDTO.from_model(result.unwrap())


@router.delete("/{catalog_item_id}", response_model=DTO.CatalogItemDeleteResponseDTO)
async def delete_catalog_item(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.delete_catalog_item(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.delete", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.delete", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}})
    return DTO.CatalogItemDeleteResponseDTO(deleted=result.unwrap())


# ==========================================
# Alias management
# ==========================================

@router.get("/{catalog_item_id}/aliases", response_model=List[DTO.CatalogItemAliasXResponseDTO])
async def list_aliases(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.list_aliases", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.get_aliases_for_item(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.list_aliases", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    data = [DTO.CatalogItemAliasXResponseDTO.from_model(a) for a in result.unwrap()]
    log.info({"action": "controller.catalog_item.list_aliases", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}, "result": {"count": len(data)}})
    return data


@router.post("/{catalog_item_id}/aliases", status_code=status.HTTP_201_CREATED, response_model=DTO.CatalogItemAliasXResponseDTO)
async def add_alias(
    catalog_item_id: str,
    payload: DTO.CatalogItemAliasCreateDTO,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.add_alias", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    item = check.unwrap()
    alias_id = payload.alias_id or f"alias_{nanoid(size=8)}"
    alias_model = M.CatalogItemAlias(
        catalog_item_alias_id = alias_id,
        value                 = payload.value,
        value_type            = payload.value_type,
        catalog_type          = item.catalog_type,
        description           = payload.description or "",
    )
    result = await svc.add_alias_to_catalog_item(catalog_item_id, alias_model)
    if result.is_err:
        log.error({"action": "controller.catalog_item.add_alias", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "alias_id": alias_id}})
        raise result.unwrap_err().to_http_exception()
    fetched_result = await svc.catalog_item_alias_repository.get_by_id(result.unwrap())
    if fetched_result.is_err:
        log.error({"action": "controller.catalog_item.add_alias", "error": str(fetched_result.unwrap_err().detail), "input": {"alias_id": alias_id}})
        raise fetched_result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.add_alias", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"alias_id": alias_id}})
    return DTO.CatalogItemAliasXResponseDTO.from_model(fetched_result.unwrap())


@router.delete("/{catalog_item_id}/aliases/{alias_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_alias(
    catalog_item_id: str,
    alias_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.remove_alias", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.delete_alias(catalog_item_id, alias_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.remove_alias", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "alias_id": alias_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.remove_alias", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id, "alias_id": alias_id}})


# ==========================================
# Hierarchy (parent ↔ child relationships)
# ==========================================

@router.get("/{catalog_item_id}/children", response_model=List[DTO.CatalogItemXResponseDTO])
async def list_children(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.list_children", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.get_items_by_parent(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.list_children", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    data = [DTO.CatalogItemXResponseDTO.from_model(i) for i in result.unwrap()]
    log.info({"action": "controller.catalog_item.list_children", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}, "result": {"count": len(data)}})
    return data


@router.post("/{catalog_item_id}/children", status_code=status.HTTP_201_CREATED)
async def link_child(
    catalog_item_id: str,
    payload: DTO.LinkItemRelationshipDTO,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.link_child", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    child_check = await svc.get_catalog_item(payload.child_item_id)
    if child_check.is_err:
        log.error({"action": "controller.catalog_item.link_child", "error": str(child_check.unwrap_err().detail), "input": {"child_item_id": payload.child_item_id}})
        raise child_check.unwrap_err().to_http_exception()
    result = await svc.add_item_relationship(catalog_item_id, payload.child_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.link_child", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "child_item_id": payload.child_item_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.link_child", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id, "child_item_id": payload.child_item_id}})
    return {"parent_item_id": catalog_item_id, "child_item_id": payload.child_item_id}


@router.delete("/{catalog_item_id}/children/{child_item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_child(
    catalog_item_id: str,
    child_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.unlink_child", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.remove_item_relationship(catalog_item_id, child_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.unlink_child", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "child_item_id": child_item_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.unlink_child", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id, "child_item_id": child_item_id}})


# ==========================================
# Catalog links
# ==========================================

@router.get("/{catalog_item_id}/catalogs", response_model=List[DTO.CatalogXDTO])
async def list_catalogs_for_item(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.list_catalogs", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.get_catalogs_for_item(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.list_catalogs", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    data = [DTO.CatalogXDTO.from_model(c) for c in result.unwrap()]
    log.info({"action": "controller.catalog_item.list_catalogs", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}, "result": {"count": len(data)}})
    return data


@router.post("/{catalog_item_id}/catalogs", status_code=status.HTTP_201_CREATED)
async def link_to_catalog(
    catalog_item_id: str,
    payload: DTO.LinkItemToCatalogDTO,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.link_catalog", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.link_item_to_catalog(payload.catalog_id, catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.link_catalog", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "catalog_id": payload.catalog_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.link_catalog", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id, "catalog_id": payload.catalog_id}})
    return {"catalog_item_id": catalog_item_id, "catalog_id": payload.catalog_id}


@router.delete("/{catalog_item_id}/catalogs/{catalog_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_from_catalog(
    catalog_item_id: str,
    catalog_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.unlink_catalog", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.unlink_item_from_catalog(catalog_id, catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.unlink_catalog", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id, "catalog_id": catalog_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.catalog_item.unlink_catalog", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id, "catalog_id": catalog_id}})

# ==========================================
# Product links
# ==========================================

@router.get("/{catalog_item_id}/products")
async def list_products_for_item(
    catalog_item_id: str,
    svc: S.CatalogService = Depends(MX.get_catalog_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    check = await svc.get_catalog_item(catalog_item_id)
    if check.is_err:
        log.error({"action": "controller.catalog_item.list_products", "error": str(check.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise check.unwrap_err().to_http_exception()
    result = await svc.get_product_ids_for_item(catalog_item_id)
    if result.is_err:
        log.error({"action": "controller.catalog_item.list_products", "error": str(result.unwrap_err().detail), "input": {"catalog_item_id": catalog_item_id}})
        raise result.unwrap_err().to_http_exception()
    ids = result.unwrap()
    log.info({"action": "controller.catalog_item.list_products", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"catalog_item_id": catalog_item_id}, "result": {"count": len(ids)}})
    return {"catalog_item_id": catalog_item_id, "product_ids": ids}
