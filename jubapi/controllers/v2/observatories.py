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
import jubapi.enums.v2 as ENUMS
from jubapi.log.log import Log

router = APIRouter(prefix="/observatories", tags=["observatories_v2"])

log = Log(name=__name__, path=os.environ.get("JUB_LOG_PATH", "/log"))


# ==========================================
# SETUP (disabled observatory + pending task)
# ==========================================

@router.post("/setup", status_code=status.HTTP_201_CREATED, response_model=DTO.ObservatorySetupResponseDTO)
async def setup_observatory(
    payload: DTO.ObservatorySetupDTO,
    obs_svc:  S.ObservatoriesService = Depends(MX.get_observatories_service),
    task_svc: S.TasksService          = Depends(MX.get_tasks_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Creates a disabled observatory and a PENDING setup task.
    The calling system should provision catalogs and products, then POST
    /tasks/{task_id}/complete to flip the observatory to enabled.
    """
    obs_id = payload.observatory_id or nanoid(size=12)
    model  = M.ObservatoryX(
        observatory_id = obs_id,
        title          = payload.title,
        description    = payload.description or "",
        image_url      = payload.image_url,
        metadata       = payload.metadata or {},
        is_disabled    = True,
    )
    obs_result = await obs_svc.create_observatory(model)
    if obs_result.is_err:
        raise obs_result.unwrap_err().to_http_exception()

    task_result = await task_svc.create_task(DTO.CreateTaskDTO(
        task_id        = obs_id,
        user_id        = payload.user_id,
        observatory_id = obs_id,
        title          = f"Setup: {payload.title}",
        description    = "Provisioning catalogs, products, and data sources.",
        operation      = ENUMS.TaskOperationEnum.SETUP,
    ))
    if task_result.is_err:
        raise task_result.unwrap_err().to_http_exception()

    return DTO.ObservatorySetupResponseDTO(
        observatory_id = obs_id,
        task_id        = task_result.unwrap(),
    )


# ==========================================
# BULK CATALOG ASSIGNMENT
# ==========================================

@router.post("/{observatory_id}/catalogs/bulk", status_code=status.HTTP_201_CREATED,
             response_model=DTO.BulkCatalogsResponseDTO)
async def bulk_assign_catalogs(
    observatory_id: str,
    payload:  DTO.BulkCatalogsDTO,
    obs_svc:  S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
    cat_svc:  S.CatalogService        = Depends(MX.get_catalog_service),
):
    """
    Creates N fully-nested catalogs (items, aliases, hierarchy) in one request
    and links each one to the observatory.

    Catalog structure mirrors `CatalogCreateDTO`:
    ```json
    {
      "catalogs": [
        {
          "name": "Spatial",
          "value": "SPATIAL",
          "catalog_type": "spatial",
          "items": [
            {
              "name": "México",
              "value": "MX",
              "code": 9,
              "value_type": "string",
              "aliases": [{"value": "MEX", "value_type": "string"}],
              "children": [
                {"name": "Tamaulipas", "value": "TAM", "code": 28, "value_type": "string",
                 "aliases": [], "children": []}
              ]
            }
          ]
        }
      ]
    }
    ```
    """
    check = await obs_svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    catalog_ids: List[str] = []
    for level, catalog_dto in enumerate(payload.catalogs):
        result = await cat_svc.create_catalog_bulk(catalog_dto)
        if result.is_err:
            raise result.unwrap_err().to_http_exception()
        cid = result.unwrap()
        catalog_ids.append(cid)

        link_result = await obs_svc.add_catalog(observatory_id, cid, level)
        if link_result.is_err:
            raise link_result.unwrap_err().to_http_exception()

    return DTO.BulkCatalogsResponseDTO(observatory_id=observatory_id, catalog_ids=catalog_ids)


# ==========================================
# BULK PRODUCT ASSIGNMENT
# ==========================================

@router.post("/{observatory_id}/products/bulk", status_code=status.HTTP_201_CREATED,
             response_model=DTO.BulkProductsResponseDTO)
async def bulk_assign_products(
    observatory_id: str,
    payload:  DTO.BulkProductsDTO,
    obs_svc:  S.ObservatoriesService = Depends(MX.get_observatories_service),
    prod_svc: S.ProductService        = Depends(MX.get_product_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    """
    Creates N products and links each one to this observatory and its catalog-item tags.
    Returns the generated product_ids so the caller can subsequently upload files.

    ```json
    {
      "products": [
        {
          "name": "Cancer incidence 2024",
          "description": "Breast cancer rates by state",
          "catalog_item_ids": ["item_abc", "item_def"]
        }
      ]
    }
    ```
    """
    check = await obs_svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    created: List[DTO.BulkProductCreatedDTO] = []
    for item in payload.products:
        pid   = item.product_id or nanoid(size=12)
        model = M.ProductX(
            product_id  = pid,
            name        = item.name,
            description = item.description or "",
        )
        result = await prod_svc.insert_product(
            product          = model,
            observatory_id   = observatory_id,
            catalog_item_ids = item.catalog_item_ids,
        )
        if result.is_err:
            raise result.unwrap_err().to_http_exception()
        created.append(DTO.BulkProductCreatedDTO(product_id=pid, name=item.name))

    return DTO.BulkProductsResponseDTO(observatory_id=observatory_id, products=created)


# ==========================================
# CRUD
# ==========================================

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DTO.ObservatoryXDTO,
    summary="Create observatory (enabled)",
    description=(
        "Creates an immediately active observatory. "
        "Use **POST /setup** instead when you need the full provisioning workflow "
        "(disabled observatory + pending task)."
    ),
)
async def create_observatory(
    payload: DTO.ObservatoryCreateDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    obs_id = payload.observatory_id or nanoid(size=12)
    model = M.ObservatoryX(
        observatory_id = obs_id,
        title          = payload.title,
        description    = payload.description or "",
        image_url      = payload.image_url,
        metadata       = payload.metadata or {},
    )
    result = await svc.create_observatory(model)
    if result.is_err:
        log.error({"action": "controller.observatory.create", "error": str(result.unwrap_err().detail), "input": {"title": payload.title}})
        raise result.unwrap_err().to_http_exception()
    get_result = await svc.get_observatory(result.unwrap())
    if get_result.is_err:
        log.error({"action": "controller.observatory.create", "error": str(get_result.unwrap_err().detail), "input": {"observatory_id": obs_id}})
        raise get_result.unwrap_err().to_http_exception()
    log.info({"action": "controller.observatory.create", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"observatory_id": obs_id}})
    return get_result.unwrap()


@router.get(
    "",
    response_model=List[DTO.ObservatoryXDTO],
    summary="List observatories",
    description="Returns all observatories. Disabled observatories (still being provisioned) are included; filter by `is_disabled` on the client if needed.",
)
async def list_observatories(
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
    page_index: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
):
    t0 = time.monotonic()
    result = await svc.get_observatories(limit=limit, page_index=page_index)
    if result.is_err:
        log.error({"action": "controller.observatory.list", "error": str(result.unwrap_err().detail)})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.observatory.list", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(data)}})
    return data


@router.post("/details", response_model=List[DTO.ObservatoryStatsDTO])
async def get_observatory_stats_batch(
    payload: DTO.ObservatoryStatsBatchRequestDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    data = await svc.get_observatory_stats_batch(payload.ids)
    log.info({"action": "controller.observatory.stats_batch", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"ids_count": len(payload.ids)}, "result": {"count": len(data)}})
    return data


@router.get("/{observatory_id}", response_model=DTO.ObservatoryDetailDTO)
async def get_observatory(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.get_observatory_detail(observatory_id)
    if result.is_err:
        log.error({"action": "controller.observatory.get", "error": str(result.unwrap_err().detail), "input": {"observatory_id": observatory_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.observatory.get", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"observatory_id": observatory_id}})
    return result.unwrap()


@router.put("/{observatory_id}", response_model=DTO.ObservatoryXDTO)
async def update_observatory(
    observatory_id: str,
    payload: DTO.ObservatoryUpdateDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not update_data:
        result = await svc.get_observatory(observatory_id)
        if result.is_err:
            log.error({"action": "controller.observatory.update", "error": str(result.unwrap_err().detail), "input": {"observatory_id": observatory_id}})
            raise result.unwrap_err().to_http_exception()
        return result.unwrap()
    result = await svc.update_observatory(observatory_id, update_data)
    if result.is_err:
        log.error({"action": "controller.observatory.update", "error": str(result.unwrap_err().detail), "input": {"observatory_id": observatory_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.observatory.update", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"observatory_id": observatory_id}})
    return result.unwrap()


@router.delete("/{observatory_id}", response_model=DTO.ObservatoryDeleteResponseDTO)
async def delete_observatory(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.delete_observatory(observatory_id)
    if result.is_err:
        log.error({"action": "controller.observatory.delete", "error": str(result.unwrap_err().detail), "input": {"observatory_id": observatory_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.observatory.delete", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"observatory_id": observatory_id}})
    return DTO.ObservatoryDeleteResponseDTO(deleted=result.unwrap())


# ==========================================
# Catalog links
# ==========================================

@router.post("/{observatory_id}/catalogs", status_code=status.HTTP_201_CREATED)
async def link_catalog(
    observatory_id: str,
    payload: DTO.LinkCatalogDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    # Ensure observatory exists first
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.add_catalog(observatory_id, payload.catalog_id, payload.level)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"observatory_id": observatory_id, "catalog_id": payload.catalog_id, "level": payload.level}


@router.get("/{observatory_id}/catalogs", response_model=List[DTO.CatalogXDTO])
async def list_catalogs(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.get_catalogs_by_observatory_id(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return [DTO.CatalogXDTO.from_model(c) for c in result.unwrap()]


@router.delete("/{observatory_id}/catalogs/{catalog_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_catalog(
    observatory_id: str,
    catalog_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.remove_catalog(observatory_id, catalog_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()


# ==========================================
# Product links
# ==========================================

@router.get("/{observatory_id}/products", response_model=List[DTO.ProductSimpleDTO])
async def list_products(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.get_all_products_in_observatory(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.post("/{observatory_id}/products", status_code=status.HTTP_201_CREATED)
async def link_product(
    observatory_id: str,
    payload: DTO.LinkProductDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.link_product(observatory_id, payload.product_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"observatory_id": observatory_id, "product_id": payload.product_id}


@router.delete("/{observatory_id}/products/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_product(
    observatory_id: str,
    product_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.unlink_product(observatory_id, product_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()


# ==========================================
# Service links
# ==========================================

@router.post("/{observatory_id}/services", status_code=status.HTTP_201_CREATED)
async def link_service(
    observatory_id: str,
    payload: DTO.LinkServiceDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.link_service(observatory_id, payload.service_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"observatory_id": observatory_id, "service_id": payload.service_id}


@router.get("/{observatory_id}/services", response_model=List[DTO.ServiceSimpleDTO])
async def list_services(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.list_services(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.delete("/{observatory_id}/services/{service_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_service(
    observatory_id: str,
    service_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.unlink_service(observatory_id, service_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()


# ==========================================
# DataSource links
# ==========================================

@router.post("/{observatory_id}/datasources", status_code=status.HTTP_201_CREATED)
async def link_datasource(
    observatory_id: str,
    payload: DTO.LinkDataSourceDTO,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.link_datasource(observatory_id, payload.source_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"observatory_id": observatory_id, "source_id": payload.source_id}


@router.get("/{observatory_id}/datasources", response_model=List[DTO.DataSourceDTO])
async def list_datasources(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.list_datasources(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.delete("/{observatory_id}/datasources/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_datasource(
    observatory_id: str,
    source_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    check = await svc.get_observatory(observatory_id)
    if check.is_err:
        raise check.unwrap_err().to_http_exception()

    result = await svc.unlink_datasource(observatory_id, source_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()


# ==========================================
# VIEWS & REVIEWS
# ==========================================

@router.post("/{observatory_id}/view", status_code=status.HTTP_200_OK)
async def increment_view(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    result = await svc.increment_views(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return {"observatory_id": observatory_id, "view_count": result.unwrap()}


@router.get("/{observatory_id}/reviews", response_model=List[DTO.ReviewDTO])
async def list_reviews(
    observatory_id: str,
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    result = await svc.get_reviews(observatory_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.post("/{observatory_id}/reviews", status_code=status.HTTP_201_CREATED, response_model=DTO.ReviewDTO)
async def create_review(
    observatory_id: str,
    payload: DTO.CreateReviewDTO,
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
):
    result = await svc.create_review(observatory_id, current_user.user_id, payload)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.put("/{observatory_id}/reviews/{review_id}", response_model=DTO.ReviewDTO)
async def update_review(
    observatory_id: str,
    review_id: str,
    payload: DTO.UpdateReviewDTO,
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
):
    result = await svc.update_review(review_id, current_user.user_id, payload)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return result.unwrap()


@router.delete("/{observatory_id}/reviews/{review_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_review(
    observatory_id: str,
    review_id: str,
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
    svc: S.ObservatoriesService = Depends(MX.get_observatories_service),
):
    result = await svc.delete_review(review_id, current_user.user_id)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
