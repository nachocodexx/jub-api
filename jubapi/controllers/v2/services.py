import os
import time as T 
from typing import List
from fastapi import Depends, status
from fastapi.routing import APIRouter

import jubapi.services.v2 as S
import jubapi.middlewares as MX
import jubapi.dto.v2 as DTO
from jubapi.log.log import Log

log = Log(name=__name__, path=os.environ.get("JUB_LOG_PATH", "/log"))

# ── BuildingBlocks ─────────────────────────────────────────────────────────────

bb_router = APIRouter(prefix="/building-blocks")


@bb_router.post("", response_model=DTO.BuildingBlockDTO, status_code=status.HTTP_201_CREATED)
async def create_building_block(
    payload: DTO.BuildingBlockCreateDTO,
    svc: S.BuildingBlockService = Depends(MX.get_building_block_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create(payload)
    if result.is_err:
        log.error({"action":"building_blocks.create","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"building_blocks.create","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.BuildingBlockDTO.from_model(result.unwrap())


@bb_router.get("", response_model=List[DTO.BuildingBlockDTO])
async def list_building_blocks(
    skip: int = 0,
    limit: int = 100,
    svc: S.BuildingBlockService = Depends(MX.get_building_block_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.list(skip=skip, limit=limit)
    if result.is_err:
        log.error({"action":"building_blocks.list","input":{"skip": skip, "limit": limit},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    bbs = [DTO.BuildingBlockDTO.from_model(m) for m in result.unwrap()]
    log.info({"action":"building_blocks.list","input":{"skip": skip, "limit": limit},"result":{"count": len(bbs)},"duration_ms": (T.monotonic() - t0) * 1000})
    return bbs 


@bb_router.get("/{building_block_id}", response_model=DTO.BuildingBlockDTO)
async def get_building_block(
    building_block_id: str,
    svc: S.BuildingBlockService = Depends(MX.get_building_block_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.get(building_block_id)
    if result.is_err:
        log.error({"action":"building_blocks.get","input":{"building_block_id": building_block_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"building_blocks.get","input":{"building_block_id": building_block_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.BuildingBlockDTO.from_model(result.unwrap())


@bb_router.patch("/{building_block_id}", response_model=DTO.BuildingBlockDTO)
async def update_building_block(
    building_block_id: str,
    payload: DTO.BuildingBlockUpdateDTO,
    svc: S.BuildingBlockService = Depends(MX.get_building_block_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.update(building_block_id, payload)
    if result.is_err:
        log.error({"action":"building_blocks.update","input":{"building_block_id": building_block_id,"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"building_blocks.update","input":{"building_block_id": building_block_id,"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.BuildingBlockDTO.from_model(result.unwrap())


@bb_router.delete("/{building_block_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_building_block(
    building_block_id: str,
    svc: S.BuildingBlockService = Depends(MX.get_building_block_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.delete(building_block_id)
    if result.is_err:
        log.error({"action":"building_blocks.delete","input":{"building_block_id": building_block_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"building_blocks.delete","input":{"building_block_id": building_block_id},"duration_ms": (T.monotonic() - t0) * 1000})


# ── Patterns ───────────────────────────────────────────────────────────────────

pattern_router = APIRouter(prefix="/patterns" )


@pattern_router.post("", response_model=DTO.PatternDTO, status_code=status.HTTP_201_CREATED)
async def create_pattern(
    payload: DTO.PatternCreateDTO,
    svc: S.PatternService = Depends(MX.get_pattern_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create(payload)
    if result.is_err:
        log.error({"action":"patterns.create","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"patterns.create","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.PatternDTO.from_model(result.unwrap())


@pattern_router.get("", response_model=List[DTO.PatternDTO])
async def list_patterns(
    skip: int = 0,
    limit: int = 100,
    svc: S.PatternService = Depends(MX.get_pattern_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.list(skip=skip, limit=limit)
    if result.is_err:
        log.error({"action":"patterns.list","input":{"skip": skip, "limit": limit},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"patterns.list","input":{"skip": skip, "limit": limit},"result":{"count": len(result.unwrap())},"duration_ms": (T.monotonic() - t0) * 1000})
    return [DTO.PatternDTO.from_model(m) for m in result.unwrap()]


@pattern_router.get("/{pattern_id}", response_model=DTO.PatternDTO)
async def get_pattern(
    pattern_id: str,
    svc: S.PatternService = Depends(MX.get_pattern_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.get(pattern_id)
    if result.is_err:
        log.error({"action":"patterns.get","input":{"pattern_id": pattern_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"patterns.get","input":{"pattern_id": pattern_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.PatternDTO.from_model(result.unwrap())


@pattern_router.patch("/{pattern_id}", response_model=DTO.PatternDTO)
async def update_pattern(
    pattern_id: str,
    payload: DTO.PatternUpdateDTO,
    svc: S.PatternService = Depends(MX.get_pattern_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.update(pattern_id, payload)
    if result.is_err:
        log.error({"action":"patterns.update","input":{"pattern_id": pattern_id,"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"patterns.update","input":{"pattern_id": pattern_id,"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.PatternDTO.from_model(result.unwrap())


@pattern_router.delete("/{pattern_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pattern(
    pattern_id: str,
    svc: S.PatternService = Depends(MX.get_pattern_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.delete(pattern_id)
    if result.is_err:
        log.error({"action":"patterns.delete","input":{"pattern_id": pattern_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"patterns.delete","input":{"pattern_id": pattern_id},"duration_ms": (T.monotonic() - t0) * 1000})


# ── Stages ─────────────────────────────────────────────────────────────────────

stage_router = APIRouter(prefix="/stages" )


@stage_router.post("", response_model=DTO.StageDTO, status_code=status.HTTP_201_CREATED)
async def create_stage(
    payload: DTO.StageCreateDTO,
    svc: S.StageService = Depends(MX.get_stage_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create(payload)
    if result.is_err:
        log.error({"action":"stages.create","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"stages.create","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.StageDTO.from_model(result.unwrap())


@stage_router.get("", response_model=List[DTO.StageDTO])
async def list_stages(
    skip: int = 0,
    limit: int = 100,
    svc: S.StageService = Depends(MX.get_stage_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.list(skip=skip, limit=limit)
    if result.is_err:
        log.error({"action":"stages.list","input":{"skip": skip, "limit": limit},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"stages.list","input":{"skip": skip, "limit": limit},"result":{"count": len(result.unwrap())},"duration_ms": (T.monotonic() - t0) * 1000})
    return [DTO.StageDTO.from_model(m) for m in result.unwrap()]


@stage_router.get("/{stage_id}", response_model=DTO.StageDTO)
async def get_stage(
    stage_id: str,
    svc: S.StageService = Depends(MX.get_stage_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.get(stage_id)
    if result.is_err:
        log.error({"action":"stages.get","input":{"stage_id": stage_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"stages.get","input":{"stage_id": stage_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.StageDTO.from_model(result.unwrap())


@stage_router.patch("/{stage_id}", response_model=DTO.StageDTO)
async def update_stage(
    stage_id: str,
    payload: DTO.StageUpdateDTO,
    svc: S.StageService = Depends(MX.get_stage_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.update(stage_id, payload)
    if result.is_err:
        log.error({"action":"stages.update","input":{"stage_id": stage_id,"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"stages.update","input":{"stage_id": stage_id,"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.StageDTO.from_model(result.unwrap())


@stage_router.delete("/{stage_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_stage(
    stage_id: str,
    svc: S.StageService = Depends(MX.get_stage_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.delete(stage_id)
    if result.is_err:
        log.error({"action":"stages.delete","input":{"stage_id": stage_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"stages.delete","input":{"stage_id": stage_id},"duration_ms": (T.monotonic() - t0) * 1000})


# ── Workflows ──────────────────────────────────────────────────────────────────

workflow_router = APIRouter(prefix="/workflows")


@workflow_router.post("", response_model=DTO.WorkflowDTO, status_code=status.HTTP_201_CREATED)
async def create_workflow(
    payload: DTO.WorkflowCreateDTO,
    svc: S.WorkflowService = Depends(MX.get_workflow_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create(payload)
    if result.is_err:
        log.error({"action":"workflows.create","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"workflows.create","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.WorkflowDTO.from_model(result.unwrap())


@workflow_router.get("", response_model=List[DTO.WorkflowDTO])
async def list_workflows(
    skip: int = 0,
    limit: int = 100,
    svc: S.WorkflowService = Depends(MX.get_workflow_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.list(skip=skip, limit=limit)
    if result.is_err:
        log.error({"action":"workflows.list","input":{"skip": skip, "limit": limit},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"workflows.list","input":{"skip": skip, "limit": limit},"result":{"count": len(result.unwrap())},"duration_ms": (T.monotonic() - t0) * 1000})
    return [DTO.WorkflowDTO.from_model(m) for m in result.unwrap()]


@workflow_router.get("/{workflow_id}", response_model=DTO.WorkflowDTO)
async def get_workflow(
    workflow_id: str,
    svc: S.WorkflowService = Depends(MX.get_workflow_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.get(workflow_id)
    if result.is_err:
        log.error({"action":"workflows.get","input":{"workflow_id": workflow_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"workflows.get","input":{"workflow_id": workflow_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.WorkflowDTO.from_model(result.unwrap())


@workflow_router.patch("/{workflow_id}", response_model=DTO.WorkflowDTO)
async def update_workflow(
    workflow_id: str,
    payload: DTO.WorkflowUpdateDTO,
    svc: S.WorkflowService = Depends(MX.get_workflow_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.update(workflow_id, payload)
    if result.is_err:
        log.error({"action":"workflows.update","input":{"workflow_id": workflow_id,"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"workflows.update","input":{"workflow_id": workflow_id,"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.WorkflowDTO.from_model(result.unwrap())


@workflow_router.delete("/{workflow_id}")
async def delete_workflow(
    workflow_id: str,
    cascade: bool = False,
    svc: S.WorkflowService = Depends(MX.get_workflow_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.delete(workflow_id, cascade=cascade)
    if result.is_err:
        log.error({"action":"workflows.delete","input":{"workflow_id": workflow_id,"cascade": cascade},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"workflows.delete","input":{"workflow_id": workflow_id,"cascade": cascade},"duration_ms": (T.monotonic() - t0) * 1000})
    return result.unwrap()


# ── Services ───────────────────────────────────────────────────────────────────

service_router = APIRouter(prefix="/services" )


@service_router.post("", response_model=DTO.ServiceDTO, status_code=status.HTTP_201_CREATED)
async def create_service(
    payload: DTO.ServiceCreateDTO,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create(payload)
    if result.is_err:
        log.error({"action":"services.create","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.create","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.ServiceDTO.from_model(result.unwrap())


@service_router.post("/index", response_model=DTO.ServiceIndexResponseDTO, status_code=status.HTTP_201_CREATED)
async def index_service(
    payload: DTO.ServiceIndexDTO,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.create_full(payload)
    if result.is_err:
        log.error({"action":"services.index","input":{"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.index","input":{"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return result.unwrap()


@service_router.get("", response_model=List[DTO.ServiceDTO])
async def list_services(
    skip: int = 0,
    limit: int = 100,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.list(skip=skip, limit=limit)
    if result.is_err:
        log.error({"action":"services.list","input":{"skip": skip,"limit": limit},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.list","input":{"skip": skip,"limit": limit},"duration_ms": (T.monotonic() - t0) * 1000})
    return [DTO.ServiceDTO.from_model(m) for m in result.unwrap()]


@service_router.get("/{service_id}", response_model=DTO.ServiceDTO)
async def get_service(
    service_id: str,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.get(service_id)
    if result.is_err:
        log.error({"action":"services.get","input":{"service_id": service_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.get","input":{"service_id": service_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.ServiceDTO.from_model(result.unwrap())


@service_router.patch("/{service_id}", response_model=DTO.ServiceDTO)
async def update_service(
    service_id: str,
    payload: DTO.ServiceUpdateDTO,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.update(service_id, payload)
    if result.is_err:
        log.error({"action":"services.update","input":{"service_id": service_id,"payload": payload.model_dump()},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.update","input":{"service_id": service_id,"payload": payload.model_dump()},"duration_ms": (T.monotonic() - t0) * 1000})
    return DTO.ServiceDTO.from_model(result.unwrap())


@service_router.delete("/{service_id}", response_model=DTO.ServiceDeleteResponseDTO)
async def delete_service(
    service_id: str,
    svc: S.ServiceXService = Depends(MX.get_service_x_service),
    _ = Depends(MX.get_current_user)
):
    t0 = T.monotonic()
    result = await svc.delete(service_id)
    if result.is_err:
        log.error({"action":"services.delete","input":{"service_id": service_id},"error":str(result.unwrap_err()),"duration_ms": (T.monotonic() - t0) * 1000})
        raise result.unwrap_err().to_http_exception()
    log.info({"action":"services.delete","input":{"service_id": service_id},"duration_ms": (T.monotonic() - t0) * 1000})
    return result.unwrap()
