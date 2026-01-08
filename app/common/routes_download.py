from fastapi import APIRouter, HTTPException, Query, status
from app.common.s3_utils import generate_s3_download_link
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/generate-download-link")
def generate_download_link(s3_key: str = Query(..., description="S3 Key del archivo en el bucket")):
    """
    Genera un enlace temporal (presigned URL) para descargar un archivo desde S3.
    """
    try:
        link = generate_s3_download_link(s3_key)
        if not link:
            logger.error(f"No se pudo generar link para: {s3_key}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No se pudo generar el enlace para el archivo especificado: {s3_key}"
            )
        
        return {
            "status": "success",
            "message": "Enlace generado con éxito",
            "data": {
                "s3_key": s3_key,
                "download_link": link
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error inesperado al generar link: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar la solicitud: {str(e)}"
        )
