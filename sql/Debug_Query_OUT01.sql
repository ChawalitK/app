with
     analog_transition_kvab as (     
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC)
				  UNION
				(select ('2000-01-01 00:00:00'::timestamp with time zone) start_time, NULL,NULL,NULL)	
     ),     
     analog_transition_kvbc as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12220000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'      
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),     
     analog_transition_kvca as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12240000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),     
     analog_transition_ia as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),
     analog_transition_ib as (
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),     
     analog_transition_ic as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant
         where ant."KeyTag" IN (12180000)
         and ant."Timestamp" >= '2021-02-01 23:00:00'
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),
     analog_transition_mw as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12320000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),     
     analog_transition_mvar as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12300000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ), 
     analog_transition_pctpf as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'      
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC
     ),
     analog_transition_kvab_down as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,        
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00'
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),
     analog_transition_ia_down as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),     
     analog_transition_ib_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00'  
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),     
     analog_transition_ic_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12180000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),
     analog_transition_mw_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12320000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00'   
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),     
     analog_transition_mvar_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12300000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'       
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),
     analog_transition_pctpf_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" >= '2021-02-01 23:00:00'      
         and ant."Timestamp" <= '2021-02-02 23:59:59.999'
         and ant."Value" = '0.00'     
         ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),
		 first_records as(
			select
			('2000-01-01 00:00:00'::timestamp with time zone) start_time,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12200000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12220000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,			
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12240000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12140000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,			 
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12160000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,			 			 
			 (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12180000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12320000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12300000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as Mvar,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12280000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as pctPF
		 ),
     join_records as (
         select kvab.start_time ,
				 COALESCE(f_record.kVAB,kvab."Value") as kVAB,
				 COALESCE(f_record.kVBC,kvbc."Value") as kVBC,
				 COALESCE(f_record.kVCA,kvca."Value") as kVCA,     
         COALESCE((CASE WHEN f_record.IA <= 13.00  THEN NULL ELSE f_record.IA END),(CASE WHEN ia."Value" <= 13.00  THEN NULL ELSE ia."Value" END)) as IA,
         COALESCE((CASE WHEN f_record.IB <= 13.00  THEN NULL ELSE f_record.IB END),(CASE WHEN ib."Value" <= 13.00  THEN NULL ELSE ib."Value" END)) as IB,  
         COALESCE((CASE WHEN f_record.IC <= 13.00  THEN NULL ELSE f_record.IC END),(CASE WHEN ic."Value" <= 13.00  THEN NULL ELSE ic."Value" END)) as IC,
				 COALESCE(f_record.MW,mw."Value") as MW,
				 COALESCE(f_record.MVar,mvar."Value") as MVar,
         COALESCE((CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END),
				(CASE WHEN f_record.pctPF > 100 THEN f_record.pctPF/100 WHEN f_record.pctPF < -100 THEN f_record.pctPF/100 ELSE f_record.pctPF END))as pctPF,
         kvab_down."Value" as kVAB_down, 
         kvab_down."Timestamp" as kVAB_downtime, 
         kvab_down."start_time" as kVAB_downtime_start_time, 
         ia_down."Value" as IA_down, 
         ia_down."Timestamp" as IA_downtime, 
         ia_down."start_time" as IA_downtime_start_time, 
         ib_down."Value" as IB_down, 
         ib_down."Timestamp" as IB_downtime, 
         ib_down."start_time" as IB_downtime_start_time, 
         ic_down."Value" as IC_down, 
         ic_down."Timestamp" as IC_downtime, 
         ic_down."start_time" as IC_downtime_start_time, 
         mw_down."Value" as MW_down, 
         mw_down."Timestamp" as MW_downtime, 
         mw_down."start_time" as MW_downtime_start_time, 				 
         mvar_down."Value" as MVar_down, 
         mvar_down."Timestamp" as MVar_downtime, 
         mvar_down."start_time" as MVar_downtime_start_time,
         pctpf_down."Value" as pctPF_down, 
         pctpf_down."Timestamp" as pctPF_downtime, 
         pctpf_down."start_time" as pctPF_downtime_start_time, 
         kvab."Timestamp" as kVAB_firstseentime
         from analog_transition_kvab kvab
         LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time 
         LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time 
         LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time 
         LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time 
         LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time 
         LEFT JOIN analog_transition_mw mw ON kvab.start_time = mw.start_time 
         LEFT JOIN analog_transition_mvar mvar ON kvab.start_time = mvar.start_time 
         LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time 
         LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.start_time 
         LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.start_time 
         LEFT JOIN analog_transition_ib_down ib_down ON kvab.start_time = ib_down.start_time 
         LEFT JOIN analog_transition_ic_down ic_down ON kvab.start_time = ic_down.start_time
         LEFT JOIN analog_transition_mw_down mw_down ON kvab.start_time = mw_down.start_time 
         LEFT JOIN analog_transition_mvar_down mvar_down ON kvab.start_time = mvar_down.start_time 
         LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.start_time
				 LEFT JOIN first_records f_record ON kvab.start_time = f_record.start_time
				 Order by kvab.start_time ASC
     ),
     prior_ia as (
				 select start_time,     
         (select js_sub.IA from join_records js_sub where 
         js_sub.start_time < js.start_time and js_sub.IA is not null ORDER BY js_sub.start_time DESC limit 1) as priorIA
         from join_records js where IA IS NULL     
     ),
     prior_ib as (
				 select start_time,     
         (select js_sub.IB from join_records js_sub where 
         js_sub.start_time < js.start_time and js_sub.IB is not null ORDER BY js_sub.start_time DESC limit 1) as priorIB
         from join_records js where IB IS NULL     
     ),
     prior_ic as (
				 select start_time,     
         (select js_sub.IC from join_records js_sub where 
         js_sub.start_time < js.start_time and js_sub.IC is not null ORDER BY js_sub.start_time DESC limit 1) as priorIC
         from join_records js where IC IS NULL     
     ),
     prior_mw as (
         select start_time,     
         (select js_sub.MW from join_records js_sub where 
         js_sub.start_time < js.start_time and js_sub.MW is not null ORDER BY js_sub.start_time DESC limit 1) as priorMW
         from join_records js where MW IS NULL     
     ),
     prior_mvar as (
         select start_time,     
         (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12300000)
         and ant."Timestamp" < js.start_time and ant."Value" is not null     
         ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as priorMVar
         from join_records js where Mvar IS NULL     
     ),
     prior_pctpf as (
         select start_time,     
         (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12280000)
         and ant."Timestamp" < js.start_time and ant."Value" is not null     
         ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as priorpctPF
         from join_records js where pctPF IS NULL
     ),    
     result_records as (     
        select js.start_time,
        COALESCE(js.kVAB_down, js.kVAB) as kVAB,
        COALESCE(js.kVAB_down, js.kVBC) as kVBC,
        COALESCE(js.kVAB_down, js.kVCA) as kVCA,
        COALESCE(js.IA_down, COALESCE(js.IA, p_ia.priorIA)) as IA,
        COALESCE(js.IB_down, COALESCE(js.IB, p_ib.priorIB)) as IB,
        COALESCE(js.IC_down, COALESCE(js.IC, p_ic.priorIC)) as IC,
        COALESCE(js.MW_down, COALESCE(js.MW, p_mw.priorMW)) as MW,				
        COALESCE(js.MVar_down, COALESCE(js.MVar, p_mvar.priorMVar)) as MVar,				
        COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF)) as pctPF
        from join_records js
        LEFT JOIN prior_ia p_ia
        ON js.start_time = p_ia.start_time
        LEFT JOIN prior_ib p_ib
        ON js.start_time = p_ib.start_time
        LEFT JOIN prior_ic p_ic
        ON js.start_time = p_ic.start_time
				LEFT JOIN prior_mw p_mw
        ON js.start_time = p_mw.start_time
        LEFT JOIN prior_mvar p_mvar
        ON js.start_time = p_mvar.start_time
        LEFT JOIN prior_pctpf p_prior_pctpf
        ON js.start_time = p_prior_pctpf.start_time
     ),
     timeseries as (     
         select      
         (select '2021-02-02'::date ) + ( n || ' minutes')::interval start_time      
        from generate_series(0, (24*60), 30) n limit 48     
     )     
-- select * from prior_date_record;

-- select * from join_records;
select * from result_records;

-- select * from prior_ia;
-- select * from first_records;
-- select * from join_records;
-- select * from  prior_record
-- select * from prior_mw;
-- select * from prior_mw;
-- select * from result_records;
select * from prior_mw;