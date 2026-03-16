import os
from dotenv import load_dotenv

def env_bool(key, default=False):
    try:
        value = os.getenv(key)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes')
    except Exception:
        return default

class Config:
    load_dotenv()

    APP_ENV = os.getenv("APP_ENV", "development")
    DEBUG = APP_ENV == "development"

    # jwt
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "default_secret_key")
    JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_EXPIRATION_TIME = int(os.getenv("JWT_EXPIRATION_TIME", 86400))  # 1 dia = 86400 segundos 

    # cors
    CORS_ORIGINS = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]

    # cctv
    DB_USER_CCTV = os.getenv("DB_USER_CCTV", "")
    DB_PASSWORD_CCTV = os.getenv("DB_PASSWORD_CCTV", "")
    DB_HOST_CCTV = os.getenv("DB_HOST_CCTV")
    DB_PORT_CCTV = os.getenv("DB_PORT_CCTV")
    DB_NAME_CCTV = os.getenv("DB_NAME_CCTV")

    # ts
    DB_USER_TS = os.getenv("DB_USER_TS", "")
    DB_PASSWORD_TS = os.getenv("DB_PASSWORD_TS", "")
    DB_HOST_TS = os.getenv("DB_HOST_TS")
    DB_PORT_TS = os.getenv("DB_PORT_TS")
    DB_NAME_TS = os.getenv("DB_NAME_TS")
    
    # alertas
    DB_USER_DTS = os.getenv("DB_USER_DTS", "")
    DB_PASSWORD_DTS = os.getenv("DB_PASSWORD_DTS", "")
    DB_HOST_DTS = os.getenv("DB_HOST_DTS")
    DB_PORT_DTS = os.getenv("DB_PORT_DTS")
    DB_NAME_DTS = os.getenv("DB_NAME_DTS")

    # alertas aws rds
    DB_USER_DTS_AWS = os.getenv("DB_USER_DTS_AWS", "")
    DB_PASSWORD_DTS_AWS = os.getenv("DB_PASS_DTS_AWS", "") # el usuario uso db_pass_dts_aws
    DB_HOST_DTS_AWS = os.getenv("DB_HOST_DTS_AWS")
    DB_PORT_DTS_AWS = os.getenv("DB_PORT_DTS_AWS")
    DB_NAME_DTS_AWS = os.getenv("DB_NAME_DTS_AWS")

    # imap cctv
    IMAP_HOST_CCTV = os.getenv("IMAP_HOST_CCTV")
    IMAP_PORT_CCTV = os.getenv("IMAP_PORT_CCTV")
    IMAP_USER_CCTV = os.getenv("IMAP_USER_CCTV")
    IMAP_PASSWORD_CCTV = os.getenv("IMAP_PASSWORD_CCTV")
    INPUT_FOLDER = os.getenv("CARPETA_ENTRADA")
    PROCESSED_FOLDER = os.getenv("CARPETA_PROCESADOS")

    # imap ts
    IMAP_HOST_TS = os.getenv("IMAP_HOST_TS")
    IMAP_PORT_TS = os.getenv("IMAP_PORT_TS")
    IMAP_USER_TS = os.getenv("IMAP_USER_TS")
    IMAP_PASSWORD_TS = os.getenv("IMAP_PASSWORD_TS")
    INPUT_FOLDER_TS = os.getenv("CARPETA_ENTRADA_TS")
    PROCESSED_FOLDER_TS = os.getenv("CARPETA_PROCESADOS_TS")

    # api key
    API_KEY = os.getenv("API_KEY")
    
    # smtp google
    SENDER = os.getenv("REMITENTE")
    SMTP_SERVER = os.getenv("SMTP_SERVER")
    SMTP_PORT = os.getenv("SMTP_PORT")
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    
    # credenciales azure
    
    AZURE_SERVER = os.getenv("AZURE_SERVER")
    AZURE_DATABASE = os.getenv("AZURE_DATABASE")
    AZURE_USERNAME = os.getenv("AZURE_USERNAME")
    AZURE_PASSWORD = os.getenv("AZURE_PASSWORD")

    # credenciales databricks
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_PATH = os.getenv("DATABRICKS_PATH")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    # correo para notificaciones
    EMAIL_PREVENTION_ONLINE = os.getenv("CORREO_PREVENCION_ONLINE", "")
    EMAIL_PREVENTION_TELESERVICES = os.getenv("CORREO_PREVENCION_TELESERVICIOS", "")
    EMAIL_PREVENTION_RETAIL = os.getenv("CORREO_PREVENCION_RETAIL", "")
    
    EMAIL_DNI_CORRELATIVES = os.getenv("CORREO_DNICORRELATIVOS", "")
    EMAIL_IP_CONCENTRATION = os.getenv("CORREO_CONCENTRACIONIP", "")
    EMAIL_DNI_CORRELATIVES_RETAIL = os.getenv("CORREO_DNICORRELATIVOSRETAIL", "")
    EMAIL_SIMILAR_EMAIL_RETAIL = os.getenv("CORREO_SIMILITUEMAILRETAIL", "")
    
    # credenciales kashio
    USER_NAME_KASHIO = os.getenv("USER_NAME_KASHIO", "")
    PASSWORD_KASHIO = os.getenv("PASSWORD_KASHIO", "")
    EMAIL_KASHIO = os.getenv("CORREO_KASHIO", "")
    EMAIL_KASHIO_LIQ = os.getenv("CORREO_KASHIO_LIQ", "")
    
    # credenciales monnet
    USER_NAME_MONNET = os.getenv("USER_NAME_MONNET", "")
    PASSWORD_MONNET = os.getenv("PASSWORD_MONNET", "")
    EMAIL_MONNET = os.getenv("CORREO_MONNET", "")
    
    # credenciales kushki
    USER_NAME_KUSHKI = os.getenv("USER_NAME_KUSHKI", "")
    PASSWORD_KUSHKI = os.getenv("PASSWORD_KUSHKI", "")
    EMAIL_KUSHKI = os.getenv("CORREO_KUSHKI", "")

    # credenciales niubiz
    USER_NAME_NIUBIZ = os.getenv("USER_NAME_NIUBIZ", "")
    PASSWORD_NIUBIZ = os.getenv("PASSWORD_NIUBIZ", "")
    EMAIL_NIUBIZ = os.getenv("CORREO_NIUBIZ", "")
    
    # credenciales niubiz_2
    USER_NAME_NIUBIZ_2 = os.getenv("USER_NAME_NIUBIZ_2", "")
    PASSWORD_NIUBIZ_2 = os.getenv("PASSWORD_NIUBIZ_2", "")
    EMAIL_NIUBIZ_2 = os.getenv("CORREO_NIUBIZ_2", "")
    
    # credenciales niubiz teleservices
    USER_NAME_NIUBIZ_TS = os.getenv("USER_NAME_NIUBIZ_TS", "")
    PASSWORD_NIUBIZ_TS = os.getenv("PASSWORD_NIUBIZ_TS", "")
    EMAIL_NIUBIZ_TS = os.getenv("CORREO_NIUBIZ_TS", "")
    TAX_ID_NIUBIZ_TS = os.getenv("RUC_NIUBIZ_TS", "")
    COMMERCE_CODE_NIUBIZ_TS = os.getenv("COMMERCE_CODE_NIUBIZ_TS", "")
    
    # credenciales yape
    USER_NAME_YAPE = os.getenv("USER_NAME_YAPE", "")
    PASSWORD_YAPE = os.getenv("PASSWORD_YAPE", "")
    EMAIL_YAPE = os.getenv("CORREO_YAPE", "")
    
    # credenciales nuvei
    USER_NAME_NUVEI = os.getenv("USER_NAME_NUVEI", "")
    PASSWORD_NUVEI = os.getenv("PASSWORD_NUVEI", "")
    EMAIL_NUVEI = os.getenv("CORREO_NUVEI", "")
    
    # credenciales pagoefectivo
    USER_NAME_PAGOEFECTIVO = os.getenv("USER_NAME_PAGOEFECTIVO", "")
    PASSWORD_PAGOEFECTIVO = os.getenv("PASSWORD_PAGOEFECTIVO", "")
    EMAIL_PAGOEFECTIVO = os.getenv("CORREO_PAGOEFECTIVO", "")
    EMAIL_PAGOEFECTIVO_LIQ = os.getenv("CORREO_PAGOEFECTIVO_LIQ", "")
    
    # credenciales calimaco
    USER_NAME_CALIMACO = os.getenv("USER_NAME_CALIMACO", "")
    PASSWORD_CALIMACO = os.getenv("PASSWORD_CALIMACO", "")
    
    # credenciales prometeo
    USER_NAME_PROMETEO = os.getenv("USER_NAME_PROMETEO", "")
    PASSWORD_PROMETEO = os.getenv("PASSWORD_PROMETEO", "")
    EMAIL_PROMETEO = os.getenv("CORREO_PROMETEO", "")


    # credenciales safetypay
    USER_NAME_SAFETYPAY = os.getenv("USER_NAME_SAFETYPAY", "")
    PASSWORD_SAFETYPAY = os.getenv("PASSWORD_SAFETYPAY", "")
    EMAIL_SAFETYPAY = os.getenv("CORREO_SAFETYPAY", "")


    # credenciales tupay
    USER_NAME_TUPAY = os.getenv("USER_NAME_TUPAY", "")
    PASSWORD_TUPAY = os.getenv("PASSWORD_TUPAY", "")
    EMAIL_TUPAY = os.getenv("CORREO_TUPAY", "")
    EMAIL_TUPAY_LIQ = os.getenv("CORREO_TUPAY_LIQ", "")

    # credenciales imap (office 365 o gmail)
    IMAP_SERVER = os.getenv("IMAP_SERVER", "")
    IMAP_PORT = os.getenv("IMAP_PORT", "")
    EMAIL_USER = os.getenv("EMAIL_USER", "")
    EMAIL_PASS = os.getenv("EMAIL_PASS", "")
    
    # s3
    AWS_REGION = os.getenv("AWS_REGION", "")
    AWS_ROLE_ARN = os.getenv("AWS_ROLE_ARN", "")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "")
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
    
    # === s3 configuracion === #
    S3_BUCKET = os.getenv("S3_BUCKET", "")
    S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "")
    S3_REGION = os.getenv("S3_REGION", "")

    # credenciales permanentes
    BASE_ACCESS_KEY = os.getenv("BASE_ACCESS_KEY", "")
    BASE_SECRET_KEY = os.getenv("BASE_SECRET_KEY", "")
    ROLE_ARN = os.getenv("ROLE_ARN", "")

    # azure ml endpoint
    AZURE_ML_ENDPOINT = os.getenv("AZURE_ML_ENDPOINT", None)
    AZURE_ML_API_KEY = os.getenv("AZURE_ML_API_KEY", None)

    COMPANY_DIGITAL = os.getenv("COMPANY_DIGITAL", "ATP")
    COMPANY_TLS = os.getenv("COMPANY_TLS", "ATPTS")

    # credenciales imap flujo de caja
    CASH_FLOW_EMAIL_USER = os.getenv("FLUJO_CAJA_EMAIL_USER", None)
    CASH_FLOW_EMAIL_SENDER = os.getenv("FLUJO_CAJA_EMAIL_USER_SENDER", None)
    CASH_FLOW_EMAIL_COPY = os.getenv("FLUJO_CAJA_EMAIL_COPIA", None)
    
    # microsoft graph api
    GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", None)
    GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", None)
    GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", None)

    # variables de ejecucion
    RETAIL_ALERTS_EXECUTION = env_bool("RETAIL_ALERTS_EXECUTION", False)
    RETAIL_CCTV_EXECUTION = env_bool("RETAIL_CCTV_EXECUTION", False)
    RETAIL_FLUJOCAJA_EXECUTION = env_bool("RETAIL_FLUJOCAJA_EXECUTION", False)
    TLS_ALERTS_EXECUTION = env_bool("TLS_ALERTS_EXECUTION", False)
    DIGITAL_ALERTS_EXECUTION = env_bool("DIGITAL_ALERTS_EXECUTION", False)
    DIGITAL_COLLECTORS_EXECUTION = env_bool("DIGITAL_COLLECTORS_EXECUTION", False)
