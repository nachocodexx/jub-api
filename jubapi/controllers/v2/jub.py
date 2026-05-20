import os
from fastapi.routing import APIRouter
from fastapi import Depends,Query,status, UploadFile, File, HTTPException
import yaml
import jubapi.dto as DTO
import jubapi.models.v2 as M
import jubapi.services.v2 as S
import jubapi.middlewares as MX
from jubapi.log.log import Log

router = APIRouter(prefix="", tags=["Jub"])

log = Log(
    name = __name__,
    path = os.environ.get("JUB_LOG_PATH", "/log")
)

@router.post("/code", status_code=status.HTTP_201_CREATED)
async def seed_database_from_yaml(
    file: UploadFile = File(...),
    cat_srv: S.CatalogService = Depends(MX.get_catalog_service),
    obs_srv: S.ObservatoriesService = Depends(MX.get_observatories_service),
    prod_srv: S.ProductService = Depends(MX.get_product_service)
):
    """
    Ingests a YAML file to fully seed Catalogs, Items, Aliases, Observatories, and Products.
    """
    pass
#     # 1. Read and parse the YAML file
    try:
        content = await file.read()
        yaml_data = yaml.safe_load(content)
        print(yaml_data)
    except yaml.YAMLError as e:
        log.error(f"Failed to parse YAML: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid YAML format: {str(e)}")

#     # 2. Validate against the Pydantic schema
    try:
        jub_file_data = DTO.V2.JubFile.model_validate(yaml_data)
        print(jub_file_data)
    except Exception as e:
        log.error(f"YAML validation failed: {e}")
        raise HTTPException(status_code=422, detail=f"YAML schema validation error: {str(e)}")

    try:
#         # ==========================================
#         # PHASE 1: Process Catalogs, Items, and Aliases
#         # ==========================================
        for cat_dto in jub_file_data.catalogs:
            # Create Root Catalog
            await cat_srv.create_catalog(M.CatalogX(
                catalog_id   = cat_dto.catalog_id,
                value        = cat_dto.value,
                catalog_type = M.CatalogType(cat_dto.catalog_type),   # Assuming Enum usage
                name         = cat_dto.name,
                description  = cat_dto.description
            ))

            # Create Items and Aliases
            for item_dto in cat_dto.items:
                item_model = M.CatalogItemX(
                    catalog_item_id=item_dto.catalog_item_id,
                    name=item_dto.name,
                    value=item_dto.value,
                    code=item_dto.code,
                    value_type=M.CatalogItemValueType(item_dto.value_type),
                    description=item_dto.description,
                    temporal_value=item_dto.temporal_value
                )
                await cat_srv.add_item_to_catalog(cat_dto.catalog_id, item_model, parent_id=item_dto.parent_id)

                # Insert Aliases (Adjust method name to match your CatalogService)
                for alias_dto in item_dto.aliases:
                    await cat_srv.add_alias_to_catalog_item(
                        catalog_item_id=item_dto.catalog_item_id, 
                        value=M.CatalogItemAlias(
                            catalog_item_alias_id = alias_dto.alias_id,
                            value                 = alias_dto.value,
                            description           = alias_dto.description,
                            value_type            = M.CatalogItemValueType(item_dto.value_type),
                            metadata              = {}
                        )
                        # =alias_dto.value 
                    )
        for obs_dto in jub_file_data.observatories:
            await obs_srv.create_observatory(M.ObservatoryX(
                observatory_id=obs_dto.observatory_id,
                title=obs_dto.title,
                description=obs_dto.description
            ))
            
            # Link catalogs with priority based on array order
            for priority, cat_id in enumerate(obs_dto.linked_catalogs):
                await obs_srv.add_catalog(obs_dto.observatory_id, cat_id, priority)

        for prod_dto in jub_file_data.products:
            await prod_srv.insert_product(
                M.ProductX(
                    product_id=prod_dto.product_id,
                    name=prod_dto.name,
                    description=prod_dto.description
                ),
                prod_dto.obs_id,
                prod_dto.tags
            )
    except Exception as e:
        log.error(f"Error processing catalogs and items: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process catalogs/items: {str(e)}")