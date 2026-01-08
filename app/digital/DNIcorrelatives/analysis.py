import pandas as pd
from datetime import datetime
import pytz

def analyze_correlative_dnis(df):
    """analiza y agrupa dnis correlativos"""
    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M')
    
    # crear columna de diferencia
    df['difference'] = pd.to_numeric(df['national_id'], errors='coerce').diff()
    
    # inicializar variables para agrupacion
    df['case_number'] = None
    group_num = 1
    
    # agrupar dnis correlativos
    for i in range(1, len(df)):
        if 1 <= df['difference'].iloc[i] <= 10:
            if pd.isna(df.loc[i - 1, 'case_number']):
                df.loc[i - 1, 'case_number'] = f"{current_time}_{group_num}"
            df.loc[i, 'case_number'] = df.loc[i - 1, 'case_number']
        else:
            group_num += 1
            df.loc[i, 'case_number'] = f"{current_time}_{group_num}"
    
    # filtrar solo los grupos con 2 o mas elementos
    group_counts = df['case_number'].value_counts()
    valid_groups = group_counts[group_counts >= 2].index
    df = df[df['case_number'].isin(valid_groups)].copy()
    
    # seleccionar columnas relevantes
    df_final = df[['case_number', 'player_id', 'user', 'first_name', 
                   'last_name', 'email', 'national_id', 'national_id_type', 'birthday',
                   'status', 'creation_date', 'withdrawal_count', 'deposit_count', 
                   'flag_promotion', 'flag_promotion_redeemed']]
    
    # ordenar grupos por fecha mas reciente
    max_dates = df_final.groupby('case_number')['creation_date'].max()
    ordered_groups = max_dates.sort_values(ascending=False).index.tolist()
    
    # categorizar y ordenar
    df_final = df_final.copy()
    df_final['case_number'] = pd.Categorical(df['case_number'], categories=ordered_groups, ordered=True)
    df_final = df_final.sort_values(by=['case_number', 'creation_date'], ascending=[True, True])
    
    # renombrar grupos
    first_appearance = df_final.drop_duplicates('case_number')
    ordered_groups = list(first_appearance['case_number'])
    case_number_map = {old: f"{current_time}_{i+1}" for i, old in enumerate(ordered_groups)}
    df_final['case_number'] = df_final['case_number'].map(case_number_map)
    
    return df_final