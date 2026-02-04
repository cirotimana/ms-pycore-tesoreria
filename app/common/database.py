from sqlalchemy.engine import URL
from sqlmodel import SQLModel, create_engine, Session
from app.config import Config
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func
from datetime import datetime
import pytz
from decimal import Decimal
import numpy as np
import pandas as pd


# ============================================================================
# MOTORES Y SESIONES DE BASE DE DATOS
# ============================================================================

url_ts = URL.create(
    drivername="postgresql+psycopg",
    username=Config.DB_USER_TS,
    password=Config.DB_PASSWORD_TS,
    host=Config.DB_HOST_TS,
    port=Config.DB_PORT_TS,
    database=Config.DB_NAME_TS,
)

engine_ts = create_engine(url_ts, echo=True, pool_pre_ping=True)


url_cctv = URL.create(
    drivername="postgresql+psycopg",
    username=Config.DB_USER_CCTV,
    password=Config.DB_PASSWORD_CCTV,
    host=Config.DB_HOST_CCTV,
    port=Config.DB_PORT_CCTV,
    database=Config.DB_NAME_CCTV,
)

engine_cctv = create_engine(url_cctv, echo=True, pool_pre_ping=True)


url_dts = URL.create(
    drivername="postgresql+psycopg",
    username=Config.DB_USER_DTS,
    password=Config.DB_PASSWORD_DTS,
    host=Config.DB_HOST_DTS,
    port=Config.DB_PORT_DTS,
    database=Config.DB_NAME_DTS,
)

engine_dts = create_engine(url_dts, echo=False, pool_pre_ping=True)


url_dts_aws = URL.create(
    drivername="postgresql+psycopg",
    username=Config.DB_USER_DTS_AWS,
    password=Config.DB_PASSWORD_DTS_AWS,
    host=Config.DB_HOST_DTS_AWS,
    port=Config.DB_PORT_DTS_AWS,
    database=Config.DB_NAME_DTS_AWS,
)

engine_dts_aws = create_engine(url_dts_aws, echo=False, pool_pre_ping=True)


url_azure = URL.create(
    drivername="mssql+pyodbc",
    username=Config.AZURE_USERNAME,
    password=Config.AZURE_PASSWORD,
    host=Config.AZURE_SERVER,
    port=1433,
    database=Config.AZURE_DATABASE,
    query={
        "driver": "ODBC Driver 18 for SQL Server",  
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
        "Connection Timeout": "30"
    }
)

engine_azure = create_engine(url_azure, echo=False, pool_pre_ping=True)


def get_ts_session():
    with Session(engine_ts) as session:
        yield session


def get_cctv_session():
    with Session(engine_cctv) as session:
        yield session

def get_dts_session():
    with Session(engine_dts) as session:
        yield session

def get_dts_aws_session():
    with Session(engine_dts_aws) as session:
        yield session
        
def get_azure_session():
    with Session(engine_azure) as session:
        yield session

def create_all():
    SQLModel.metadata.create_all(bind=engine_cctv)
    SQLModel.metadata.create_all(bind=engine_ts)
    SQLModel.metadata.create_all(bind=engine_dts)
    SQLModel.metadata.create_all(bind=engine_dts_aws)
    SQLModel.metadata.create_all(bind=engine_azure)

def run_on_dual_dts(logic_func):
    results = []
    
    # 1. ejecucion Local (principal) -- DISABLED
    # try:
    #     with next(get_dts_session()) as session:
    #         results.append(logic_func(session))
    # except Exception as e:
    #     print(f"[DATABASE DUAL] Error en base de datos LOCAL (DTS): {e}")
    #     # Si la local falla, usualmente queremos que el error suba
    #     raise e
        
    # 2. ejecucion en AWS RDS (Ahora PRINCIPAL)
    try:
        with next(get_dts_aws_session()) as session:
            print("[DATABASE INSERT] Iniciando ejecucion en AWS RDS.")
            result = logic_func(session)
            results.append(result)
            print("[DATABASE INSERT] Ejecucion exitosa en AWS RDS.")
    except Exception as e:
        print(f"[DATABASE INSERT] Error en AWS RDS: {e}")
        raise e
        
    return results[0] if results else None


# ============================================================================
# UTILIDADES DE CONVERSION Y NORMALIZACION
# ============================================================================

def to_decimal(value):
    if isinstance(value, (np.integer, np.int64, np.int32)):
        return Decimal(int(value))
    if isinstance(value, (np.floating, np.float64, np.float32)):
        return Decimal(str(float(value)))
    return Decimal(str(value))


def normalize_date_column(date_series, column_name="FECHA", collector_name="unknown"):
    if pd.api.types.is_datetime64_any_dtype(date_series):
        return date_series
    
    date_formats = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%d/%m/%Y %I:%M:%S %p',
        '%d/%m/%Y %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y-%m-%d',
        '%m/%d/%Y %H:%M:%S',
    ]
    
    result = None
    for fmt in date_formats:
        try:
            converted = pd.to_datetime(date_series, format=fmt, errors='coerce')
            valid_count = converted.notna().sum()
            if valid_count > 0 and valid_count >= len(date_series) * 0.8:
                result = converted
                break
        except Exception:
            continue
    
    if result is None:
        try:
            result = pd.to_datetime(date_series, errors='coerce', dayfirst=True)
        except Exception:
            pass
    
    if result is not None:
        return result
    else:
        return pd.to_datetime(date_series, errors='coerce')


# ============================================================================
# FUNCIONES DE PERSISTENCIA (CONCILIACIONES, LIQUIDACIONES, RECORDS)
# ============================================================================

def insert_conciliations(
    collector,
    session,
    conciliations_types,
    from_date,
    to_date,
    amounts,
    amount_collectors,
    records_calimaco,
    records_collector,
    unreconciled_records_calimaco,
    unreconciled_records_collector,
    unreconciled_amount_calimaco,
    unreconciled_amount_collector
):
    from app.models.tbl_conciliation import TblConciliation
    difference_amounts = abs(amounts - amount_collectors)
    conciliations_state = difference_amounts == 0

    try:
        conciliations = TblConciliation(
            collector_id=int(collector),
            conciliations_type=int(conciliations_types),
            from_date=datetime.strptime(str(from_date), "%Y-%m-%d").date() if isinstance(from_date, str) else from_date,
            to_date=datetime.strptime(str(to_date), "%Y-%m-%d").date() if isinstance(to_date, str) else to_date,
            amount=to_decimal(amounts),
            amount_collector=to_decimal(amount_collectors),
            difference_amounts=to_decimal(difference_amounts),
            records_calimaco=int(records_calimaco),
            records_collector=int(records_collector),
            unreconciled_records_calimaco=int(unreconciled_records_calimaco),
            unreconciled_records_collector=int(unreconciled_records_collector),
            unreconciled_amount_calimaco=to_decimal(unreconciled_amount_calimaco),
            unreconciled_amount_collector=to_decimal(unreconciled_amount_collector),
            conciliations_state=bool(conciliations_state),
            created_at=datetime.now(pytz.timezone("America/Lima")),
            created_by=1,
            activo=True,
        )
        session.add(conciliations)
        session.commit()
        session.refresh(conciliations)
        return conciliations.id
    except Exception as e:
        session.rollback()
        raise Exception(f"Error al insertar conciliacion: {e}")


def insert_conciliation_files(session, conciliation_ids, conciliations_file_types, file_paths):
    from app.models.tbl_conciliation_file import TblConciliationFile
    try:
        conciliation_files = TblConciliationFile(
            conciliation_id=int(conciliation_ids),
            conciliation_files_type=int(conciliations_file_types),
            file_path=str(file_paths),
            created_at=datetime.now(pytz.timezone("America/Lima")),
            created_by=1,
        )
        session.add(conciliation_files)
        session.commit()
    except Exception as e:
        session.rollback()
        raise Exception(f"Error al insertar archivo: {e}")


def insert_liquidations(
    collector,
    session,
    liquidationsTypes,
    from_date,
    to_date,
    amountCollector,
    amountLiquidation,
    records_collector,
    records_liquidation,
    debit_amount_collector,
    debit_amount_liquidation,
    credit_amount_collector,
    credit_amount_liquidation,
    unreconciled_credit_amount_collector,
    unreconciled_credit_amount_liquidation,
    unreconciled_debit_amount_collector,
    unreconciled_debit_amount_liquidation,
    unreconciled_amount_collector,
    unreconciled_amount_liquidation
):
    from app.models.tbl_liquidation import TblLiquidation
    differenceAmounts = abs(amountCollector - amountLiquidation)
    liquidationsState = differenceAmounts == 0

    try:
        liquidations = TblLiquidation(
            collector_id=int(collector),
            liquidations_type=int(liquidationsTypes),
            from_date=datetime.strptime(str(from_date), "%Y-%m-%d").date() if isinstance(from_date, str) else from_date,
            to_date=datetime.strptime(str(to_date), "%Y-%m-%d").date() if isinstance(to_date, str) else to_date,
            amount_collector=to_decimal(amountCollector),
            amount_liquidation=to_decimal(amountLiquidation),
            records_collector=int(records_collector),
            records_liquidation=int(records_liquidation),
            debit_amount_collector=to_decimal(debit_amount_collector),
            debit_amount_liquidation=to_decimal(debit_amount_liquidation),
            credit_amount_collector=to_decimal(credit_amount_collector),
            credit_amount_liquidation=to_decimal(credit_amount_liquidation),
            unreconciled_credit_amount_collector=to_decimal(unreconciled_credit_amount_collector),
            unreconciled_credit_amount_liquidation=to_decimal(unreconciled_credit_amount_liquidation),            
            unreconciled_debit_amount_collector=to_decimal(unreconciled_debit_amount_collector),
            unreconciled_debit_amount_liquidation=to_decimal(unreconciled_debit_amount_liquidation),
            unreconciled_amount_collector=to_decimal(unreconciled_amount_collector),
            unreconciled_amount_liquidation=to_decimal(unreconciled_amount_liquidation),
            difference_amounts=to_decimal(differenceAmounts),
            liquidations_state=bool(liquidationsState),
            created_at=datetime.now(pytz.timezone("America/Lima")),
            created_by=1,
            activo=True,
        )
        session.add(liquidations)
        session.commit()
        session.refresh(liquidations)
        return liquidations.id
    except Exception as e:
        session.rollback()
        raise Exception(f"Error al insertar liquidacion: {e}")


def insert_liquidation_files(session, liquidationIds, liquidationsFileTypes, filePaths):
    from app.models.tbl_liquidation_file import TblLiquidationFile
    try:
        liquidation_files = TblLiquidationFile(
            liquidation_id=int(liquidationIds),
            liquidation_files_type=int(liquidationsFileTypes),
            file_path=str(filePaths),
            created_at=datetime.now(pytz.timezone("America/Lima")),
            created_by=1,
        )
        session.add(liquidation_files)
        session.commit()
    except Exception as e:
        session.rollback()
        raise Exception(f"Error al insertar archivo: {e}")


def bulk_upsert_collector_records_optimized(session, df, collector_id):
    from app.models.tbl_collector_record import TblCollectorRecord
    try:
        df_clean = df.copy()
        df_clean['collector_id'] = collector_id
        
        collector_names = {
            1: 'kashio', 2: 'monnet', 3: 'kushki', 4: 'niubiz',
            5: 'yape', 6: 'nuvei', 7: 'pagoefectivo', 8: 'safetypay', 9: 'tupay'
        }
        collector_name = collector_names.get(collector_id, f'collector_{collector_id}')
        
        df_clean['record_date'] = normalize_date_column(df_clean['FECHA'], 'FECHA', collector_name)
        df_clean = df_clean.dropna(subset=['record_date'])
        
        if len(df_clean) == 0:
            return
            
        df_clean['calimaco_id'] = df_clean['ID CALIMACO'].astype(str)
        df_clean['provider_id'] = df_clean['ID PROVEEDOR'].where(
            (df_clean['ID PROVEEDOR'].notna()) & (df_clean['ID PROVEEDOR'] != '-'), 'Sin Data'
        ).astype(str)
        df_clean['client_name'] = df_clean['CLIENTE'].where(df_clean['CLIENTE'].notna(), 'Sin Data').astype(str)
        df_clean['amount'] = df_clean['MONTO'].apply(to_decimal)
        df_clean['provider_status'] = df_clean['ESTADO PROVEEDOR'].astype(str)
        df_clean['activo'] = True
        
        records = df_clean[['collector_id', 'record_date', 'calimaco_id', 'provider_id', 'client_name', 'amount', 'provider_status', 'activo']].to_dict('records')
        
        batch_size = 5000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = insert(TblCollectorRecord).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=['collector_id', 'calimaco_id', 'amount'],
                set_={
                    'record_date': stmt.excluded.record_date,
                    'provider_id': stmt.excluded.provider_id,
                    'client_name': stmt.excluded.client_name,
                    'provider_status': stmt.excluded.provider_status, 
                    'updated_at': func.now()
                }
            )
            session.execute(stmt)
            if (i // batch_size + 1) % 5 == 0:
                session.commit()
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise


def bulk_upsert_calimaco_records_optimized(session, df, collector_id):
    from app.models.tbl_calimaco_record import TblCalimacoRecord
    try:
        df_clean = df.copy()
        collector_names = {
            1: 'kashio', 2: 'monnet', 3: 'kushki', 4: 'niubiz',
            5: 'yape', 6: 'nuvei', 7: 'pagoefectivo', 8: 'safetypay', 9: 'tupay'
        }
        collector_name = collector_names.get(collector_id, f'collector_{collector_id}')
        
        df_clean['record_date'] = normalize_date_column(df_clean['Fecha'], 'Fecha', f'{collector_name}_calimaco')
        df_clean['modification_date'] = normalize_date_column(df_clean['Fecha de modificación'], 'Fecha_modificacion', f'{collector_name}_calimaco')
        
        df_valid = df_clean.dropna(subset=['record_date'])
        if len(df_valid) == 0:
            return
            
        df_valid['collector_id'] = collector_id
        df_valid['calimaco_id'] = df_valid['ID'].astype(str)
        df_valid['status'] = df_valid['Estado'].astype(str)
        df_valid['user_id'] = df_valid['Usuario'].where(df_valid['Usuario'].notna(), 'Sin Data').astype(str)
        df_valid['amount'] = df_valid['Cantidad'].apply(to_decimal)
        df_valid['external_id'] = df_valid['ID externo'].where(df_valid['ID externo'].notna(), 'Sin Data').astype(str)
        df_valid['comments'] = df_valid['Comentarios'].where(df_valid['Comentarios'].notna(), 'Sin Comentarios').astype(str)
        df_valid['activo'] = True
        
        records = df_valid[['collector_id', 'calimaco_id', 'record_date', 'modification_date', 'status', 'user_id', 'amount', 'external_id', 'comments', 'activo']].to_dict('records')
        
        batch_size = 5000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = insert(TblCalimacoRecord).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=['collector_id', 'calimaco_id', 'status'],
                set_={
                    'record_date': stmt.excluded.record_date,
                    'modification_date': stmt.excluded.modification_date,
                    'user_id': stmt.excluded.user_id,
                    'amount': stmt.excluded.amount,
                    'external_id': stmt.excluded.external_id,
                    'comments': stmt.excluded.comments,
                    'updated_at': func.now()
                }
            )
            session.execute(stmt)
            if (i + batch_size) % 25000 == 0:
                session.commit()
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise


def update_collector_timestamp(session, collector_id):
    from app.models.tbl_collector import TblCollector
    try:
        # print(f"la fecha actual es : {datetime.now(pytz.timezone("America/Lima"))}")
        stmt = (
            session.query(TblCollector)
            .filter(TblCollector.id == collector_id)
            .update({
                'updated_at': datetime.now(pytz.timezone("America/Lima"))
            })
        )
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        raise


