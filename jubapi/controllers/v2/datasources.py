import os
import time
from typing import List
from fastapi import Depends, status
from fastapi.routing import APIRouter

import jubapi.services.v2 as S
import jubapi.middlewares as MX
import jubapi.dto.v2 as DTO
import jubapi.models.v2 as M
from jubapi.log.log import Log

log = Log(name=__name__, path=os.environ.get("JUB_LOG_PATH", "/log"))

router = APIRouter(prefix="/datasources", tags=["datasources_v2"])


@router.post("", response_model=DTO.DataSourceDTO, status_code=status.HTTP_201_CREATED)
async def register_data_source(
    payload: DTO.DataSourceCreateDTO,
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _:DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.register_data_source(
        source_id   = payload.source_id,
        name        = payload.name,
        description = payload.description or "",
        bucket_id   = payload.bucket_id or "",
    )
    if result.is_err:
        log.error({"action": "controller.datasource.create", "error": str(result.unwrap_err().detail), "input": {"name": payload.name}})
        raise result.unwrap_err().to_http_exception()
    source = result.unwrap()
    log.info({"action": "controller.datasource.create", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"source_id": source.source_id}})
    return DTO.DataSourceDTO.from_model(source)


@router.get("", response_model=List[DTO.DataSourceDTO])
async def list_data_sources(
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _:DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action

):
    t0 = time.monotonic()
    result = await svc.source_repo.find({}, limit=200)
    if result.is_err:
        log.error({"action": "controller.datasource.list", "error": str(result.unwrap_err().detail)})
        raise result.unwrap_err().to_http_exception()
    data = [DTO.DataSourceDTO.from_model(s) for s in result.unwrap()]
    log.info({"action": "controller.datasource.list", "duration_ms": int((time.monotonic()-t0)*1000), "result": {"count": len(data)}})
    return data


@router.get("/{source_id}", response_model=DTO.DataSourceDTO)
async def get_data_source(
    source_id: str,
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.source_repo.get_by_id(source_id)
    if result.is_err:
        log.error({"action": "controller.datasource.get", "error": str(result.unwrap_err().detail), "input": {"source_id": source_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.datasource.get", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"source_id": source_id}})
    return DTO.DataSourceDTO.from_model(result.unwrap())


@router.put("/{source_id}", response_model=DTO.DataSourceDTO)
async def update_data_source(
    source_id: str,
    payload: DTO.DataSourceUpdateDTO,
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    result = await svc.update_data_source(source_id, data)
    if result.is_err:
        log.error({"action": "controller.datasource.update", "error": str(result.unwrap_err().detail), "input": {"source_id": source_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.datasource.update", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"source_id": source_id}})
    return DTO.DataSourceDTO.from_model(result.unwrap())


@router.delete("/{source_id}", response_model=DTO.DataSourceDeleteResponseDTO)
async def delete_data_source(
    source_id: str,
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    count_result = await svc.record_repo.count({"source_id": source_id})
    records_removed = count_result.unwrap() if count_result.is_ok else 0

    result = await svc.delete_data_source(source_id)
    if result.is_err:
        log.error({"action": "controller.datasource.delete", "error": str(result.unwrap_err().detail), "input": {"source_id": source_id}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.datasource.delete", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"source_id": source_id}, "result": {"records_removed": records_removed}})
    return DTO.DataSourceDeleteResponseDTO(deleted=result.unwrap(), records_removed=records_removed)


@router.post("/{source_id}/records", status_code=status.HTTP_201_CREATED)
async def ingest_records(
    source_id: str,
    records: List[DTO.DataRecordCreateDTO],
    svc: S.DataIngestionService = Depends(MX.get_data_ingestion_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    models = [
        M.DataRecord(
            record_id              = r.record_id,
            source_id              = source_id,
            spatial_id             = r.spatial_id,
            temporal_id            = r.temporal_id,
            interest_ids           = r.interest_ids,
            numerical_interest_ids = r.numerical_interest_ids,
            raw_payload            = r.raw_payload,
        )
        for r in records
    ]
    result = await svc.ingest_parsed_records(source_id, models)
    if result.is_err:
        log.error({"action": "controller.datasource.ingest_records", "error": str(result.unwrap_err().detail), "input": {"source_id": source_id, "count": len(records)}})
        raise result.unwrap_err().to_http_exception()
    log.info({"action": "controller.datasource.ingest_records", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"source_id": source_id}, "result": {"inserted": result.unwrap()}})
    return {"inserted": result.unwrap()}


@router.post("/{source_id}/query")
async def query_records(
    source_id: str,
    payload: DTO.DataSourceQueryDTO,
    svc: S.DataQueryService = Depends(MX.get_data_query_service),
    _: DTO.UserProfileDTO = Depends(MX.get_current_user)   # Ensure user is authenticated for this action
):
    t0 = time.monotonic()
    result = await svc.query(source_id, payload.query)
    if result.is_err:
        log.error({"action": "controller.datasource.query", "error": str(result.unwrap_err().detail), "input": {"source_id": source_id, "query": payload.query}})
        raise result.unwrap_err().to_http_exception()
    records = result.unwrap()
    skip  = payload.skip or 0
    limit = payload.limit or 100
    data  = records[skip: skip + limit]
    log.info({"action": "controller.datasource.query", "duration_ms": int((time.monotonic()-t0)*1000), "input": {"source_id": source_id, "query": payload.query}, "result": {"count": len(data)}})
    return data
