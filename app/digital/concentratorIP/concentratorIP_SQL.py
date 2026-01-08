query_original = """
with listaIPs as (
	select last_ip,count(*) cant 
	from bronze_data_users 
	where DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
	 and last_ip is not null
	group by last_ip
	having count(*) >= 5
),
registros as (
	select 
	(FORMAT(DATEADD(hour, -5, GETDATE()), 'yyyyMMddHHmm') +'_'+ cast((DENSE_RANK() OVER (ORDER BY last_ip) ) as varchar(100))) as  numcaso,
	last_ip, [user] ,
	cast( db as varchar(2)) + '.' + cast( [user] as varchar(100)) usuario,
	first_name,last_name,email,created_date,modified_date,regulatory_status, national_id_type, national_id,nationality,mobile,birthday,
	verified flgVerificado, verification_type 
	from bronze_data_users 
	where DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
	and last_ip is not null
	--order by last_ip
),
depositos as (
	select a.[user], COUNT(*) cantDepos
	from [dbo].[bronze_data_transactions] a inner join registros b on a.[user] = b.[user]
	where a.type = 'DEPOSIT' and a.[status] = 'SUCCESS'
	GROUP BY a.[user]
	having count(*) > 0
),
retiros as (
	select a.[user], COUNT(*) cantRetiros
	from [dbo].[bronze_data_transactions] a inner join registros b on a.[user] = b.[user]
	where a.type = 'PAYOUT' and a.[status] = 'PROCESSED'
	GROUP BY a.[user]
	having count(*) > 0
),
balance as (
	select 
	SUM(CASE WHEN A.TYPE IN ('DELETE','PAYOUT','PROMOTION_EXPIRED','REDEEM','RESET','ROLLBACK','ROLLBACK_ROLLBACK','ROLLBACK_WINNING','WAGER') THEN -1*A.AMOUNT/100.0
		WHEN A.TYPE IN ('BONUS-WINNING','DEPOSIT','MANUAL','PROMOTION','WINNING') THEN 1*A.AMOUNT/100.0 END) balance,
	A.[user]
	from bronze_data_operations a inner JOIN registros B on a.[user] = b.[user]
	WHERE a.status not in ('CANCELLED','NEW') AND [type] not in ('REDEEM') 
	GROUP BY A.[user]
)
SELECT 
	a.numcaso case_number,
	a.last_ip ip,
	a.[user],
	a.usuario player_id,
	a.first_name,
	a.last_name,
	a.email,
	DATEADD(hour, -5, a.created_date) creation_date,
	a.modified_date modification_date,
	a.regulatory_status status,
	a.national_id_type,
	a.national_id,
	a.nationality ,
	a.mobile ,
	a.birthday,
	a.flgVerificado verification_flag,
	a.verification_type,
	b.cant ip_count, 
	c.cantDepos deposit_count,
	d.cantRetiros withdrawal_count,
	round(e.balance,2) balance
from registros a inner join listaIPs b on a.last_ip = b.last_ip
				left join depositos c on a.[user] = c.[user]
				left join retiros d on a.[user] = d.[user]
				left join balance e on a.[user] = e.[user]
order by b.cant desc ,a.last_ip,a.national_id
"""

query = """
with listaIPs as (
	select last_ip,count(*) cant 
	from v_data_users 
	where DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
	 and last_ip is not null
	group by last_ip
	having count(*) >= 5
),
registros as (
	select 
	(FORMAT(DATEADD(hour, -5, GETDATE()), 'yyyyMMddHHmm') +'_'+ cast((DENSE_RANK() OVER (ORDER BY last_ip) ) as varchar(100))) as  numcaso,
	last_ip, [user] ,
	cast( db as varchar(2)) + '.' + cast( [user] as varchar(100)) usuario,
	first_name,last_name,email,created_date,modified_date,regulatory_status, national_id_type, national_id,nationality,mobile,birthday,
	verified flgVerificado, verification_type 
	from v_data_users 
	where DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
	and last_ip is not null
	--order by last_ip
),
depositos as (
	select a.[user], COUNT(*) cantDepos
	from v_data_transactions a inner join registros b on a.[user] = b.[user]
	where a.type = 'DEPOSIT' and a.[status] = 'SUCCESS'
	GROUP BY a.[user]
	having count(*) > 0
),
retiros as (
	select a.[user], COUNT(*) cantRetiros
	from v_data_transactions a inner join registros b on a.[user] = b.[user]
	where a.type = 'PAYOUT' and a.[status] = 'PROCESSED'
	GROUP BY a.[user]
	having count(*) > 0
),
balance as (
	select 
	SUM(CASE WHEN A.TYPE IN ('DELETE','PAYOUT','PROMOTION_EXPIRED','REDEEM','RESET','ROLLBACK','ROLLBACK_ROLLBACK','ROLLBACK_WINNING','WAGER') THEN -1*A.AMOUNT/100.0
		WHEN A.TYPE IN ('BONUS-WINNING','DEPOSIT','MANUAL','PROMOTION','WINNING') THEN 1*A.AMOUNT/100.0 END) balance,
	A.[user]
	from v_data_operations a inner JOIN registros B on a.[user] = b.[user]
	WHERE a.status not in ('CANCELLED','NEW') AND [type] not in ('REDEEM') 
	GROUP BY A.[user]
)
SELECT 
	a.numcaso case_number,
	a.last_ip ip,
	a.[user],
	a.usuario player_id,
	a.first_name,
	a.last_name,
	a.email,
	DATEADD(hour, -5, a.created_date) creation_date,
	a.modified_date modification_date,
	a.regulatory_status status,
	a.national_id_type,
	a.national_id,
	a.nationality ,
	a.mobile ,
	a.birthday,
	a.flgVerificado verification_flag,
	a.verification_type,
	b.cant ip_count, 
	c.cantDepos deposit_count,
	d.cantRetiros withdrawal_count,
	round(e.balance,2) balance
from registros a inner join listaIPs b on a.last_ip = b.last_ip
				left join depositos c on a.[user] = c.[user]
				left join retiros d on a.[user] = d.[user]
				left join balance e on a.[user] = e.[user]
order by b.cant desc ,a.last_ip,a.national_id
"""


query_total_ip_original = """
        SELECT COUNT(*) total_data FROM bronze_data_users 
        WHERE DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
        AND last_ip IS NOT NULL
    """
    
query_total_ip = """
		SELECT COUNT(*) total_data FROM v_data_users 
        WHERE DATEADD(hour, -5, created_date) BETWEEN DATEADD(day, -3, GETDATE()) AND GETDATE() 
        AND last_ip IS NOT NULL
"""
    
