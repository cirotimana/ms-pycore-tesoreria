from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from contextlib import asynccontextmanager

from app.events import *
from app.config import Config
from app.auth.dependencies import get_current_active_user

from scalar_fastapi import get_scalar_api_reference, Layout


# CANAL DIGITAL

from app.digital.collectors.kashio.routes import router as getkashio_router
from app.digital.collectors.kashio.liquidations.routes import router as getkashioliq_router
from app.digital.collectors.monnet.routes import router as getmonnet_router
from app.digital.collectors.kushki.routes import router as getkushki_router
from app.digital.collectors.yape.routes import router as getyape_router
from app.digital.collectors.niubiz.routes import router as getniubiz_router
from app.digital.collectors.nuvei.routes import router as getnuvei_router
from app.digital.collectors.pagoefectivo.routes import router as getpagoefectivo_router
from app.digital.collectors.pagoefectivo.liquidations.routes import router as getpagoefectivoliq_router
from app.digital.collectors.safetypay.routes import router as getsafetypay_router
from app.digital.collectors.tupay.routes import router as gettupay_router
from app.digital.collectors.tupay.liquidations.routes import router as gettupayliq_router
from app.common.routes_download import router as download_router

##
from app.digital.concentratorIP.routes import router as concentratorip_router
from app.digital.DNIcorrelatives.routes import router as dnicorrelatives_router


##CONTROL
from app.utils.routes import router as utils_router

# AUTH
from app.auth.routes import router as auth_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    if(Config.DEBUG == False):
        processing_data_with_cron_updated_kashio()
        processing_data_with_cron_updated_monnet()
        processing_data_with_cron_updated_kushki()
        processing_data_with_cron_updated_niubiz()
        processing_data_with_cron_updated_yape()
        processing_data_with_cron_updated_nuvei()
        processing_data_with_cron_updated_pagoefectivo()
        processing_data_with_cron_updated_safetypay()
        processing_data_with_cron_updated_tupay()
        processing_data_with_cron_getkashio()
        processing_data_with_cron_getmonnet()
        processing_data_with_cron_getkushki()
        processing_data_with_cron_getniubiz()
        processing_data_with_cron_getyape()
        processing_data_with_cron_getnuvei()
        processing_data_with_cron_getpagoefectivo()
        processing_data_with_cron_getsafetypay()
        processing_data_with_cron_gettupayy()
        cron_liquidation_kashio()
        cron_liquidation_pagoefectivo()
        cron_liquidation_tupay()
    yield

# Crear una instancia de FastAPI, Swagger y OpenAPI
app = FastAPI(
    title="Backend Project API",
    description="API AT.Pycore",
    lifespan=lifespan
)

# esquema para el boton de autorizacion de swagger
def custom_openapi_schema():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT"
        }
    }

    # aplicar seguridad por defecto a todos los endpoints excepto login
    for path_item, path_data in openapi_schema["paths"].items():
        # el endpoint de login y debug-headers no necesitan autenticacion
        if "/auth/login" in path_item or "/auth/debug-headers" in path_item:
            continue

        for method_name, method in path_data.items():
            if isinstance(method, dict):
                # forzar la configuracion de seguridad para todos los endpoints protegidos
                method["security"] = [{"BearerAuth": []}]

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi_schema

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conectar routers
## AUTH (sin dependencias de autenticacion para permitir login publico)
app.include_router(auth_router, prefix="/auth", tags=["AUTH"])


##DIGITAL (protegido con jwt)
app.include_router(getkashio_router, prefix="/digital", tags=["CONCILIACION-KASHIO"], dependencies=[Depends(get_current_active_user)])
app.include_router(getkashioliq_router, prefix="/digital", tags=["LIQUIDACION-KASHIO"], dependencies=[Depends(get_current_active_user)] )
app.include_router(getmonnet_router, prefix="/digital", tags=["CONCILIACION-MONNET"], dependencies=[Depends(get_current_active_user)])
app.include_router(getkushki_router, prefix="/digital", tags=["CONCILIACION-KUSHKI"], dependencies=[Depends(get_current_active_user)])
app.include_router(getniubiz_router, prefix="/digital", tags=["CONCILIACION-NIUBIZ"], dependencies=[Depends(get_current_active_user)])
app.include_router(getyape_router, prefix="/digital", tags=["CONCILIACION-YAPE"], dependencies=[Depends(get_current_active_user)])
app.include_router(getnuvei_router, prefix="/digital", tags=["CONCILIACION-NUVEI"], dependencies=[Depends(get_current_active_user)])
app.include_router(getpagoefectivo_router, prefix="/digital", tags=["CONCILIACION-PAGOEFFECTIVO"], dependencies=[Depends(get_current_active_user)])
app.include_router(getpagoefectivoliq_router, prefix="/digital", tags=["LIQUIDACION-PAGOEFFECTIVO"], dependencies=[Depends(get_current_active_user)])
app.include_router(getsafetypay_router, prefix="/digital", tags=["CONCILIACION-SAFETYPAY"], dependencies=[Depends(get_current_active_user)])
app.include_router(gettupay_router, prefix="/digital", tags=["CONCILIACION-TUPAY"], dependencies=[Depends(get_current_active_user)])
app.include_router(gettupayliq_router, prefix="/digital", tags=["LIQUIDACION-TUPAY"], dependencies=[Depends(get_current_active_user)])
app.include_router(download_router, prefix="/digital", tags=["GENERAR-LINK"], dependencies=[Depends(get_current_active_user)])


app.include_router(concentratorip_router, prefix="/digital", tags=["CONCENTRATOR-IP"], dependencies=[Depends(get_current_active_user)])
app.include_router(dnicorrelatives_router, prefix="/digital", tags=["DNI-CORRELATIVES"], dependencies=[Depends(get_current_active_user)])

##CONTROL (protegido con jwt)
app.include_router(utils_router, prefix="/utils", tags=["UTILS"], dependencies=[Depends(get_current_active_user)])

# Mensaje de bienvenida
@app.get("/")
def read_root():
    return {"message": "Bienvenido al servicio de Pycore-Backend API - Tesoreria"}


@app.get("/docs-scalar",  include_in_schema=False)
async def scalar_docs():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title = "Backend Project API - Scalar Documentation",
        layout=Layout.MODERN, 
        dark_mode=True,
        show_sidebar=True,
        default_open_all_tags=True,
        hide_download_button=False,
        hide_models=False,
        
    )