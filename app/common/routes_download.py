from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.common.s3_utils import generate_s3_download_link

router = APIRouter()

@router.get("/download/{s3_key:path}")
async def download_file(s3_key: str):
    presigned_url = generate_s3_download_link(s3_key, expiration_hours=12)
    if not presigned_url:
        return JSONResponse(content={"error": "No se pudo generar link"}, status_code=500)
    return JSONResponse(content={"url": presigned_url})