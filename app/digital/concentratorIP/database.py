from sqlalchemy import  text
import pandas as pd
from datetime import datetime
import pytz
from app.common.database import get_dts_session

from app.models.bot_executions import *
from app.models.bots import *
from app.models.cases import *
from app.models.case_incident import *
from app.digital.concentratorIP.concentratorIP_SQL import query_total_ip as query_ti
from app.digital.concentratorIP.utils import *

exclude_cols = [
    "user", "first_name", "last_name", "email",
    "national_id_type", "national_id", "birthday", "status"
]

def get_total_ip_count(engine):
    """Obtiene el conteo total de IPs"""
    query_total_ip = query_ti
    df_total = pd.read_sql(query_total_ip, engine)
    return df_total['total_data'].iloc[0]

def get_ip_data(engine, query):
    """Obtiene los datos de IP desde la base de datos"""
    return pd.read_sql(query, engine)

def save_to_database(df, total_ip, total_registros):
    """Guarda los resultados en la base de datos"""
    with next(get_dts_session()) as session:
        # insertar ejecucion del bot
        execution_id = insert_bot_execution(session, total_ip, total_registros)
        
        # procesar cada grupo de IPs (solo si hay datos)
        if df is not None and len(df) > 0:
            for ip, group in df.groupby('ip'):
                case_id = insert_case(session, execution_id, len(group), ip)
                insert_incidents(session, case_id, group)
        
        # actualizar ultima ejecucion del bot
        update_bot_last_run(session)
        session.commit()

def insert_bot_execution(session, total_processed, total_detected):
    """Insertar en Bot_Executions"""
    bot_execution = Bot_Executions(
        bot_id = 1,
        executed_at=datetime.now(pytz.timezone("America/Lima")),
        total_processed_records=int(total_processed),
        total_detected_incidents=int(total_detected)
    )
    session.add(bot_execution)
    session.commit()
    session.refresh(bot_execution)  
    return bot_execution.id

def insert_case(session, execution_id, incident_count, ip):
    """Insertr en Case"""
    descripcion = (
        f"Concentracion de la IP {ip}, con {incident_count} registros en los ultimos 3 dias "
    )
    case = Cases(
        execution_id=execution_id,
        capture_date=datetime.now(pytz.timezone("America/Lima")),
        description=descripcion,
        state_id=1
    )
    session.add(case)
    session.commit()
    session.refresh(case)
    return case.id

def insert_incidents(session, case_id, group):
    """Insertar en Case_Incident"""


    upsert_cliente_stmt = text("""
        SELECT public.upsert_client(
            :first_name,
            :last_name,
            :email,
            :national_id_type,
            :national_id,
            :birthday,
            :calimaco_user,
            :mvt_id,
            :calimaco_status
        ) AS client_id
    """)

    for _, row in group.iterrows():
        # upsert cliente
        result = session.execute(upsert_cliente_stmt, {
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "email": row["email"],
            "national_id_type": row["national_id_type"],
            "national_id": row["national_id"],
            "birthday": row["birthday"],
            "calimaco_user": int(row["user"]),
            "mvt_id": None,
            "calimaco_status": row["status"]
        })
        client_id = result.scalar()

        # insertar incidente
        ##safe_dict = convert_row_to_json_safe_dict(row)
        safe_dict = convert_row_to_json_safe_dict_exclude(row, exclude_cols)
        incident = Case_Incident(
            case_id=case_id,
            data_json=safe_dict,
            client_id=client_id,
            channel_id=1
        )
        session.add(incident)
    session.commit()
        
def update_bot_last_run(session):
    """Actualiza Bots"""
    bot = session.query(Bots).filter(Bots.id == 1).first()
    if bot:
        bot.last_run = datetime.now(pytz.timezone("America/Lima"))
        session.commit()