import os
import time
from typing import Optional
from fastapi import Depends, Query
from fastapi.routing import APIRouter
import jubapi.middlewares as M
import jubapi.services.v2 as S
import jubapi.dto.v2 as DTO
import jubapi.middlewares as MX
from jubapi.log.log import Log

log = Log(
    name = __name__,
    path = os.environ.get("JUB_LOG_PATH", "/log")
)

router = APIRouter(prefix="/search", tags=["search"])


@router.post(
    "",
    summary="Search products by DSL",
    description=(
        "Runs a JUB DSL query against the product catalog and returns hydrated ProductXDTOs. "
        "Each DTO includes resolved spatial, temporal, and interest variable metadata. "
        "Example: `jub.v1.VS(MX).VT(>= 2020).VI(SEX_FEMALE AND C_MAMA)`"
    ),
)
async def search(
    query: DTO.SearchQueryDTO,
    search: S.SearchService = Depends(M.get_search_service), 
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),

):
    t0 = time.monotonic()
    result = await search.search(query=query.query, user_id=current_user.user_id, observatory_id=query.observatory_id, limit=query.limit, skip=query.skip, no_cache=query.no_cache)
    if result.is_err:
        log.error({"action": "controller.search.products", "error": str(result.unwrap_err()), "input": {"query": query.query, "observatory_id": query.observatory_id}})
        raise result.unwrap_err().to_http_exception()
    response = result.unwrap()
    log.info({"action": "controller.search.products", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"query": query.query}, "result": {"count": len(response)}})
    return response


@router.post(
    "/records",
    summary="Fetch raw data records by DSL",
    description=(
        "Translates the DSL into a MongoDB `$match` filter and returns the matching raw DataRecord documents. "
        "Values in VS() and VI() are resolved via catalog lookup (catalog_item_id, value, code, or alias). "
        "Use `source_id` in the query body to scope results to a single data source."
    ),
)
async def search_records(
    query: DTO.SearchQueryDTO,
    search: S.SearchService = Depends(M.get_search_service),
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await search.search_data_records(
        query_str      = query.query,
        observatory_id = query.observatory_id,
        limit          = query.limit,
        skip           = query.skip,
        strict         = query.strict,
    )
    if result.is_err:
        log.error({"action": "controller.search.records", "error": str(result.unwrap_err()), "input": {"query": query.query, "observatory_id": query.observatory_id}})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.search.records", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"query": query.query}, "result": {"count": len(data)}})
    return data


@router.post(
    "/plot",
    summary="Generate ECharts plot data",
    description=(
        "Runs a full DSL aggregation and returns an ECharts-ready JSON object.\n\n"
        "Pipeline: `$match` (VS/VT/VI resolved to catalog_item_ids) → `$group` (BY catalog membership) → `$sort`.\n\n"
        "Axis labels replace raw catalog_item_ids with the catalog item `name` field.\n\n"
        "DSL example: `jub.v1.VS(MX).VI(C_MAMA OR C_OVARIO).VO(AVG(TASA_100K)).BY(CIE10_CANCER)`\n\n"
        "VO operators: `COUNT`, `AVG(field)`, `SUM(field)` — `field` must be a key in `numerical_interest_ids`."
    ),
)
async def generate_plot(
    query: DTO.PlotQueryDTO,
    search: S.SearchService = Depends(M.get_search_service),
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await search.generate_plot(
        query_str  = query.query,
        source_id  = query.source_id,
        chart_type = query.chart_type,
        strict     = query.strict,
    )
    if result.is_err:
        log.error({"action": "controller.search.plot", "error": str(result.unwrap_err()), "input": {"query": query.query, "source_id": query.source_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.search.plot", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"query": query.query}})
    return result.unwrap()


@router.post("/observatories")
async def search_observatories(
    query: DTO.SearchQueryDTO,
    search: S.SearchService = Depends(M.get_search_service), 
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),

):
    t0 = time.monotonic()
    result = await search.search_observatories(
        query    = query.query,
        user_id  = current_user.user_id,
        strict   = query.strict,
        skip     = query.skip or 0,
        limit    = query.limit or 100,
        no_cache = query.no_cache,
    )
    if result.is_err:
        log.error({"action": "controller.search.observatories", "error": str(result.unwrap_err()), "input": {"query": query.query}})
        raise result.unwrap_err().to_http_exception()
    response = result.unwrap()
    log.info({"action": "controller.search.observatories", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"query": query.query}, "result": {"count": len(response)}})
    return response


@router.get(
    "/observatories/suggestions",
    response_model=DTO.ObservatorySearchSuggestionsResponseDTO,
    summary="Get top observatory search suggestions",
    description="Returns the most-used DSL queries that returned results from the observatory search, ranked by hit count.",
)
async def get_observatory_search_suggestions(
    limit: int = Query(5, ge=1, le=20),
    search: S.SearchService = Depends(M.get_search_service),
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    result = await search.get_observatory_search_suggestions(limit=limit)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return DTO.ObservatorySearchSuggestionsResponseDTO(suggestions=result.unwrap())


@router.get(
    "/products/suggestions",
    response_model=DTO.SearchSuggestionsResponseDTO,
    summary="Get top product search suggestions for an observatory",
    description="Returns the most-used DSL queries that returned results for the given observatory, ranked by hit count.",
)
async def get_search_suggestions(
    observatory_id: Optional[str] = Query(None, description="Observatory to fetch suggestions for. Omit for global suggestions."),
    limit: int = Query(5, ge=1, le=20),
    search: S.SearchService = Depends(M.get_search_service),
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    key = observatory_id if observatory_id else "__global__"
    result = await search.get_search_suggestions(observatory_id=key, limit=limit)
    if result.is_err:
        raise result.unwrap_err().to_http_exception()
    return DTO.SearchSuggestionsResponseDTO(observatory_id=key, suggestions=result.unwrap())


@router.post("/services", summary="Search services by DSL", description=(
    "Queries services using the SVC DSL. Examples:\n\n"
    "- `jub.v1.SVC(*)` — all services\n"
    "- `jub.v1.SVC(name=cancer)` — name contains 'cancer' (case-insensitive)\n"
    "- `jub.v1.SVC(public=true)` — public only\n"
    "- `jub.v1.SVC(owner=usr_abc)` — by owner\n"
    "- `jub.v1.SVC(name=cancer,public=true)` — combine filters with comma"
))
async def search_services(
    query: DTO.ServiceQueryDTO,
    svc:   S.ServiceXService = Depends(M.get_service_x_service),
    current_user: DTO.UserProfileDTO = Depends(MX.get_current_user),
):
    t0 = time.monotonic()
    result = await svc.query_services_hydrated(query.query, skip=query.skip, limit=query.limit)
    if result.is_err:
        log.error({"action": "controller.search.services", "error": str(result.unwrap_err()), "input": {"query": query.query}})
        raise result.unwrap_err().to_http_exception()
    data = result.unwrap()
    log.info({"action": "controller.search.services", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"query": query.query}, "result": {"count": len(data)}})
    return data
