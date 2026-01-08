query_original ="""
WITH registros AS (
    SELECT 
        (CAST(db AS VARCHAR(3)) + '.' + CAST([user] AS VARCHAR(20))) idplayer, 
        [user], [first_name], [last_name], [alias], [email], 
        DATEADD(HOUR, -5, created_date) created_date,
        [regulatory_status], [national_id_type], 
        CAST([national_id] AS BIGINT) AS national_id_num,
        [national_id], [nationality],[birthday]
    FROM [dbo].[bronze_data_users]
    WHERE created_date >= DATEADD(HOUR, -48, GETDATE()) 
    AND national_id_type = 'DNI'
    AND ISNUMERIC([national_id]) = 1
    AND LEN([national_id]) = 8
),

dni_con_diferencias AS (
    SELECT 
        national_id,
        national_id_num,
        national_id_num - LAG(national_id_num, 1, national_id_num-11) 
            OVER (ORDER BY national_id_num) AS diff_anterior,
        LEAD(national_id_num, 1, national_id_num+11) 
            OVER (ORDER BY national_id_num) - national_id_num AS diff_siguiente
    FROM (SELECT DISTINCT national_id, national_id_num FROM registros) d
),

dni_secuencias AS (
    SELECT 
        national_id,
        national_id_num,
        CASE WHEN diff_anterior <= 10 OR diff_siguiente <= 10 
             THEN 1 ELSE 0 END AS en_secuencia
    FROM dni_con_diferencias
),

retiros as (
select a.[user], count(*) cantRetiros
from [dbo].[bronze_data_transactions] a inner join registros b on a.[user] = b.[user]
where a.type ='PAYOUT' and a.[status] = 'PROCESSED'
group by a.[user]
),
depositos as (
select a.[user], count(*) cantDepositos
from [dbo].[bronze_data_transactions] a inner join registros b on a.[user] = b.[user]
where a.type ='DEPOSIT' and a.[status] = 'SUCCESS'
group by a.[user]
),
promociones as (
select a.[user], count(*) flgProm, sum(case when [type] = 'REDEEM' then 1 else 0 end) flgPromCanjeada
from [dbo].[bronze_data_operations_promotions] a inner join registros b on a.[user] = b.[user]
group by a.[user]
)
SELECT 
	a.idplayer player_id,
    a.[user],
    a.[first_name],
    a.[last_name],
    a.[alias],
    a.[email],
	a.created_date as  creation_date,
    a.[regulatory_status] status,
    a.[national_id_type],
    a.[national_id],
    a.[nationality],
    a.[birthday],
    b.cantRetiros withdrawal_count,
    dep.cantDepositos deposit_count,
    CASE WHEN prom.flgProm >= 1 THEN 1 ELSE 0 END flag_promotion,
    CASE WHEN prom.flgPromCanjeada >= 1 THEN 1 ELSE 0 END flag_promotion_redeemed
FROM registros a 
LEFT JOIN retiros b ON a.[user] = b.[user]
LEFT JOIN depositos dep ON a.[user] = dep.[user]
LEFT JOIN promociones prom ON a.[user] = prom.[user]
JOIN dni_secuencias s ON a.national_id = s.national_id
WHERE s.en_secuencia = 1 
ORDER BY s.national_id_num
"""


query = """
WITH registros AS (
    SELECT 
        (CAST(db AS VARCHAR(3)) + '.' + CAST([user] AS VARCHAR(20))) idplayer, 
        [user], [first_name], [last_name], [alias], [email], 
        DATEADD(HOUR, -5, created_date) created_date,
        [regulatory_status], [national_id_type], 
        CAST([national_id] AS BIGINT) AS national_id_num,
        [national_id], [nationality],[birthday]
    FROM v_data_users
    WHERE created_date >= DATEADD(HOUR, -48, GETDATE()) 
    AND national_id_type = 'DNI'
    AND ISNUMERIC([national_id]) = 1
    AND LEN([national_id]) = 8
),

dni_con_diferencias AS (
    SELECT 
        national_id,
        national_id_num,
        national_id_num - LAG(national_id_num, 1, national_id_num-11) 
            OVER (ORDER BY national_id_num) AS diff_anterior,
        LEAD(national_id_num, 1, national_id_num+11) 
            OVER (ORDER BY national_id_num) - national_id_num AS diff_siguiente
    FROM (SELECT DISTINCT national_id, national_id_num FROM registros) d
),

dni_secuencias AS (
    SELECT 
        national_id,
        national_id_num,
        CASE WHEN diff_anterior <= 10 OR diff_siguiente <= 10 
             THEN 1 ELSE 0 END AS en_secuencia
    FROM dni_con_diferencias
),

retiros as (
select a.[user], count(*) cantRetiros
from v_data_transactions a inner join registros b on a.[user] = b.[user]
where a.type ='PAYOUT' and a.[status] = 'PROCESSED'
group by a.[user]
),
depositos as (
select a.[user], count(*) cantDepositos
from v_data_transactions a inner join registros b on a.[user] = b.[user]
where a.type ='DEPOSIT' and a.[status] = 'SUCCESS'
group by a.[user]
),
promociones as (
select a.[user], count(*) flgProm, sum(case when [type] = 'REDEEM' then 1 else 0 end) flgPromCanjeada
from v_data_operations_promotions a inner join registros b on a.[user] = b.[user]
group by a.[user]
)
SELECT 
	a.idplayer player_id,
    a.[user],
    a.[first_name],
    a.[last_name],
    a.[alias],
    a.[email],
	a.created_date as  creation_date,
    a.[regulatory_status] status,
    a.[national_id_type],
    a.[national_id],
    a.[nationality],
    a.[birthday],
    b.cantRetiros withdrawal_count,
    dep.cantDepositos deposit_count,
    CASE WHEN prom.flgProm >= 1 THEN 1 ELSE 0 END flag_promotion,
    CASE WHEN prom.flgPromCanjeada >= 1 THEN 1 ELSE 0 END flag_promotion_redeemed
FROM registros a 
LEFT JOIN retiros b ON a.[user] = b.[user]
LEFT JOIN depositos dep ON a.[user] = dep.[user]
LEFT JOIN promociones prom ON a.[user] = prom.[user]
JOIN dni_secuencias s ON a.national_id = s.national_id
WHERE s.en_secuencia = 1 
ORDER BY s.national_id_num
"""


query_total_dni_original = """
        SELECT COUNT(*) total_data FROM bronze_data_users
        WHERE created_date >= DATEADD(HOUR, -48, GETDATE()) 
            AND national_id_type = 'DNI'
            AND ISNUMERIC(national_id) = 1
            AND LEN(national_id) = 8
    """
    
query_total_dni = """
        SELECT COUNT(*) total_data FROM v_data_users
        WHERE created_date >= DATEADD(HOUR, -48, GETDATE()) 
            AND national_id_type = 'DNI'
            AND ISNUMERIC(national_id) = 1
            AND LEN(national_id) = 8
    """