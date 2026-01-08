import boto3
from botocore.exceptions import ClientError
import os
from typing import List
from app.config import Config

def get_s3_client_with_role():
    try:
        sts = boto3.client(
            "sts",
            aws_access_key_id=Config.BASE_ACCESS_KEY,
            aws_secret_access_key=Config.BASE_SECRET_KEY
        )
        assumed = sts.assume_role(
            RoleArn=Config.ROLE_ARN,
            RoleSessionName="user-session",
            DurationSeconds=43200 ##---> solicitar cambio a 43200 (12 horas) o 86400 (24 horas)
        )
        creds = assumed['Credentials']
        return boto3.client(
            "s3",
            region_name=Config.S3_REGION,
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )
    except ClientError as e:
        print("[ALERTA] error al asumir el rol:", e)
        return None
    

def upload_file_to_s3(content: bytes, s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3_client.put_object(Body=content, Bucket=Config.S3_BUCKET, Key=s3_key)
        print(f"[✔] Subido a S3: s3://{Config.S3_BUCKET}/{s3_key}")
    except ClientError as e:
        print(f"[ALERTA] error subiendo {s3_key} a S3: {e}")


def read_file_from_s3(s3_key: str) -> bytes:
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        response = s3.get_object(Bucket=Config.S3_BUCKET, Key=str(s3_key))
        return response['Body'].read()
    except ClientError as e:
        print(f"[ALERTA] error al leer archivo S3: {e}")
        return b""


def delete_file_from_s3(s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        s3.delete_object(Bucket=Config.S3_BUCKET, Key=str(s3_key))
        print(f"[✔] eliminado de S3: {s3_key}")
    except ClientError as e:
        print(f"[ALERTA] error al eliminar archivo de S3: {e}")


def list_files_in_s3(prefix: str) -> List[str]:
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=Config.S3_BUCKET, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
        return files
    except ClientError as e:
        print(f"[ALERTA] error al listar archivos: {e}")
        return []


def get_latest_file_from_s3(prefix: str) -> str:
    try:
        files = list_files_in_s3(prefix)
        if not files:
            return None
        # Ordena por fecha (asumiendo que los nombres contienen fechas)
        files.sort(reverse=True)
        return files[0]
    except Exception as e:
        print(f"[ALERTA] error al obtener el archivo mas reciente de S3: {e}")
        return None
    
    
def get_attachment_from_s3(s3_key):
    # obtiene el contenido binario y el nombre del archivo
    content = read_file_from_s3(s3_key)
    filename = os.path.basename(s3_key)
    return (filename, content)


def download_file_from_s3_to_local(s3_key: str, local_dir: str = "debug_output") -> str:
    try:
        # Crear carpeta local si no existe
        os.makedirs(local_dir, exist_ok=True)

        # Obtener contenido
        content = read_file_from_s3(s3_key)
        if not content:
            print(f"[ALERTA] No se pudo descargar {s3_key} desde S3.")
            return None

        # Nombre local
        local_path = os.path.join(local_dir, os.path.basename(s3_key))

        # Guardar localmente
        with open(local_path, "wb") as f:
            f.write(content)

        print(f"[✔] Archivo guardado en local: {os.path.abspath(local_path)}")
        return local_path

    except Exception as e:
        print(f"[ALERTA] Error al descargar y guardar archivo de S3: {e}")
        return None
    
def get_s3_file_size(s3_key : str):
    s3_client = get_s3_client_with_role()
    s3 = s3_client
    response = s3.head_object(Bucket=Config.S3_BUCKET, Key=str(s3_key))
    size_bytes = response['ContentLength']
    size_mb = size_bytes / (1024 * 1024)
    print(f"[INFO] Tamaño de {s3_key}: {size_bytes} bytes ({size_mb:.2f} MB)")
    return size_mb


def generate_s3_download_link(s3_key: str, expiration_hours: int = 12) -> str:
    try:
        s3_client = get_s3_client_with_role()
        if not s3_client:
            print(f"[ALERTA] No se pudo obtener cliente S3 para generar enlace de {s3_key}")
            return None
            
        expiration_seconds = min(expiration_hours * 3600, 43200)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': Config.S3_BUCKET, 'Key': s3_key},
            ExpiresIn=expiration_seconds
        )
        
        print(f"[✔] Enlace de descarga generado para {s3_key} (valido por {expiration_hours} horas)")
        return presigned_url
        
    except ClientError as e:
        print(f"[ALERTA] Error al generar enlace de descarga para {s3_key}: {e}")
        return None
    except Exception as e:
        print(f"[ALERTA] Error inesperado al generar enlace: {e}")
        return None


def delete_files_in_paths_keeping_folders(paths: List[str]):
    s3_client = get_s3_client_with_role()
    if not s3_client:
        print("[ALERTA] No se pudo obtener cliente S3 para limpieza.")
        return

    for path in paths:
        # Asegurar que el prefijo termine en '/' para ser tratado como directorio
        prefix = path if path.endswith('/') else f"{path}/"
        print(f"[INFO] Analizando ruta para limpieza: {prefix}")
        
        try:
            # Usar Delimiter='/' para no listar recursivamente dentro de subcarpetas
            paginator = s3_client.get_paginator('list_objects_v2')
            
            total_deleted = 0
            
            for page in paginator.paginate(Bucket=Config.S3_BUCKET, Prefix=prefix, Delimiter='/'):
                # 'Contents' tiene los archivos en ESTE nivel
                if 'Contents' in page:
                    objects_to_delete = []
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # ignorar si es el mismo prefijo (marcador de carpeta)
                        if key == prefix:
                            continue
                            
                        # ignorar .keep
                        if key.endswith('.keep') or key.endswith('/'):
                            continue
                            
                        objects_to_delete.append({'Key': key})
                    
                    if objects_to_delete:
                        # Eliminar en lotes (batch de 1000 es limita de AWS, aqui hacemos simple)
                        # Como paginator devuelve max 1000 por pagina, podemos borrar directo
                        s3_client.delete_objects(
                            Bucket=Config.S3_BUCKET,
                            Delete={'Objects': objects_to_delete}
                        )
                        count = len(objects_to_delete)
                        total_deleted += count
                        print(f"[✔] Eliminados {count} archivos en lote de {prefix}")
                        
            print(f"[RESUMEN] Total eliminados en {prefix}: {total_deleted}")
            
        except ClientError as e:
            print(f"[ALERTA] Error al limpiar ruta {prefix}: {e}")



paths = [
    "digital/collectors/kashio/output", 
    "digital/collectors/kashio/input",
    "digital/collectors/kashio/calimaco/output",
    "digital/collectors/kashio/calimaco/input",
    "digital/collectors/kushki/output", 
    "digital/collectors/kushki/input",
    "digital/collectors/kushki/calimaco/output",
    "digital/collectors/kushki/calimaco/input",
    "digital/collectors/monnet/output", 
    "digital/collectors/monnet/input",
    "digital/collectors/monnet/calimaco/output",
    "digital/collectors/monnet/calimaco/input",
    "digital/collectors/niubiz/output", 
    "digital/collectors/niubiz/input",
    "digital/collectors/niubiz/calimaco/output",
    "digital/collectors/niubiz/calimaco/input",
    "digital/collectors/nuvei/output", 
    "digital/collectors/nuvei/input",
    "digital/collectors/nuvei/calimaco/output",
    "digital/collectors/nuvei/calimaco/input",
    "digital/collectors/pagoefectivo/output", 
    "digital/collectors/pagoefectivo/input",
    "digital/collectors/pagoefectivo/calimaco/output",
    "digital/collectors/pagoefectivo/calimaco/input",
    "digital/collectors/safetypay/output", 
    "digital/collectors/safetypay/input",
    "digital/collectors/safetypay/calimaco/output",
    "digital/collectors/safetypay/calimaco/input",
    "digital/collectors/tupay/output", 
    "digital/collectors/tupay/input",
    "digital/collectors/tupay/calimaco/output",
    "digital/collectors/tupay/calimaco/input",
    "digital/collectors/yape/output", 
    "digital/collectors/yape/input",
    "digital/collectors/yape/calimaco/output",
    "digital/collectors/yape/calimaco/input"
]

if __name__ == "__main__":
    delete_files_in_paths_keeping_folders(paths)
