from sqlalchemy import text
import pandas as pd
from datetime import datetime
import pytz
from app.common.database import get_dts_session, get_dts_aws_session


from app.models.tbl_bot_execution import *
from app.models.tbl_bot import *
from app.models.tbl_case import *
from app.models.tbl_case_incident import *
from app.digital.DNIcorrelatives.DNIcorrelatives_SQL import query_total_dni as query_td
from app.digital.DNIcorrelatives.utils import *

exclude_cols = [
    "user", "first_name", "last_name", "email",
    "national_id_type", "national_id", "birthday", "status"
]

def get_total_dni_count(engine):
    """obtiene el conteo total de dnis"""
    query_total_dni = query_td
    df_total = pd.read_sql(query_total_dni, engine)
    return df_total['total_data'].iloc[0]

def get_dni_data(engine, query):
    """obtiene los datos de dni desde la base de datos"""
    return pd.read_sql(query, engine)

def save_to_database(df, total_dni, total_registros):
    """guarda los resultados en la base de datos (Dual: DTS y AWS)"""
    sessions = [get_dts_session(), get_dts_aws_session()]
    
    for session_gen in sessions:
        try:
            with next(session_gen) as session:
                # insertar ejecucion del bot
                execution_id = insert_bot_execution(session, total_dni, total_registros)
                
                # procesar cada grupo de dnis (solo si hay datos)
                if df is not None and len(df) > 0:
                    for case_number, group in df.groupby('case_number'):
                        case_id = insert_case(session, execution_id, len(group))
                        insert_incidents(session, case_id, group)
                
                # actualizar ultima ejecucion del bot
                update_bot_last_run(session)
                session.commit()
                print(f"[DATABASE] Insercion exitosa en {'DTS' if session_gen == sessions[0] else 'AWS RDS'}")
        except Exception as e:
            print(f"[DATABASE] Error al insertar en {'DTS' if session_gen == sessions[0] else 'AWS RDS'}: {e}")
            continue

def insert_bot_execution(session, total_processed, total_detected):
    """Insertar en TblBotExecution"""
    bot_execution = TblBotExecution(
        bot_id = 2,
        executed_at=datetime.now(pytz.timezone("America/Lima")),
        total_processed_records=int(total_processed),
        total_detected_incidents=int(total_detected)
    )
    session.add(bot_execution)
    session.commit()
    session.refresh(bot_execution)  
    return bot_execution.id

def insert_case(session, execution_id, incident_count):
    """Insertar en TblCase"""
    descripcion = (
        f"DNI correlativos de las ultimas 48 horas. "
        f"Total de incidencias en este caso: {incident_count}"
    )
    case = TblCase(
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
    """Insertar en TblCaseIncident"""
    

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
        incident = TblCaseIncident(
            case_id=case_id,
            data_json=safe_dict,
            client_id=client_id,
            channel_id=1
        )
        session.add(incident)
    session.commit()

def update_bot_last_run(session):
    """Actualiza TblBot"""
    bot = session.query(TblBot).filter(TblBot.id == 2).first()
    if bot:
        bot.last_run = datetime.now(pytz.timezone("America/Lima"))
        session.commit()