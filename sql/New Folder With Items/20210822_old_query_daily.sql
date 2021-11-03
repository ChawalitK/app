WITH analog_transition_kvab AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
	ant."Timestamp",
	ant."Value",ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12200000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_kvbc AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12220000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_kvca AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12240000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_kvab_down AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '30 min') end_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '60 min') extend_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12200000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" = '0.00'
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_ia AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12140000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_ia_down AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '30 min') end_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '60 min') extend_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12140000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" = '0.00'
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_ib AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12160000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_ic AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12180000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_pctpf AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12280000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" IS NOT NULL
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     analog_transition_pctpf_down AS
  (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '30 min') end_time,
     (date_trunc('hour', ant."Timestamp" + (30 || ' minutes'):: interval) + date_part('minute', ant."Timestamp" + (30 || ' minutes'):: interval):: int / 30 * interval '60 min') extend_time,
     ant."Timestamp",
     ant."Value",
     ant."TimestampIndex"
   FROM "AnalogTransition" ant
   WHERE ant."KeyTag" IN (12280000)
     AND ant."Timestamp" >= '2021-02-06 00:00:00'
     AND ant."Timestamp" <= '2021-02-06 23:59:59'
     AND ant."Value" = '0.00'
   ORDER BY start_time ASC, ant."Timestamp" ASC, ant."TimestampIndex" ASC),
     join_records AS
  (SELECT kvab.start_time,
          kvab."Value" AS kVAB,
          kvbc."Value" AS kVBC,
          kvca."Value" AS kVCA,
          (CASE
               WHEN ia."Value" <= 13.00 THEN 0.00
               ELSE ia."Value"
           END) AS IA,
          (CASE
               WHEN ib."Value" <= 13.00 THEN 0.00
               ELSE ib."Value"
           END) AS IB,
          (CASE
               WHEN ic."Value" <= 13.00 THEN 0.00
               ELSE ic."Value"
           END) AS IC,
          (CASE
               WHEN pctpf."Value" > 100 THEN pctpf."Value" / 100
               WHEN pctpf."Value" < -100 THEN pctpf."Value" / 100
               ELSE pctpf."Value"
           END) AS pctPF,
          kvab_down."Value" AS kVAB_down,
          kvab_down."Timestamp" AS kVAB_downtime,
          kvab_down."start_time" AS kVAB_downtime_start_time,
          kvab_down."end_time" AS kVAB_downtime_end_time,
          ia_down."Value" AS IA_down,
          ia_down."Timestamp" AS IA_downtime,
          ia_down."start_time" AS IA_downtime_start_time,
          ia_down."end_time" AS IA_downtime_end_time,
          pctpf_down."Value" AS pctPF_down,
          pctpf_down."Timestamp" AS pctPF_downtime,
          pctpf_down."start_time" AS pctPF_downtime_start_time,
          pctpf_down."end_time" AS pctPF_downtime_end_time,
          kvab."Timestamp" AS kVAB_firstseentime
   FROM analog_transition_kvab kvab
   LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time
   LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time
   LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time
   LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time
   LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time
   LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time
   LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.end_time
   LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.end_time
   LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.end_time),
     prior_ia AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" <= 13.00 THEN 0.00
                  ELSE "Value"
              END) AS priorIA
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12140000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 days')
           AND ant."Value" IS NOT NULL
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE IA IS NULL ),
     prior_ia_down AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" <= 13.00 THEN 0.00
                  ELSE "Value"
              END) AS priorIA
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12140000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 days')
           AND ant."Value" = '0.00'
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE IA IS NULL ),
     prior_ib AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" <= 13.00 THEN 0.00
                  ELSE "Value"
              END) AS priorIB
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12140000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 days')
           AND ant."Value" IS NOT NULL
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE IB IS NULL ),
     prior_ic AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" <= 13.00 THEN 0.00
                  ELSE "Value"
              END) AS priorIC
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12180000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 days')
           AND ant."Value" IS NOT NULL
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE IC IS NULL ),
     prior_pctpf AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" > 100 THEN "Value" / 100
                  WHEN "Value" < -100 THEN "Value" / 100
                  ELSE "Value"
              END) AS priorpctPF
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12280000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 day')
           AND ant."Value" IS NOT NULL
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE pctPF IS NULL ),
     prior_pctpf_down AS
  (SELECT start_time,
     (SELECT (CASE
                  WHEN "Value" > 100 THEN "Value" / 100
                  WHEN "Value" < -100 THEN "Value" / 100
                  ELSE "Value"
              END) AS priorpctPF
      FROM
        (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp"):: int / 30 * interval '30 min') start_time,
           ant."Timestamp",
           ant."Value",
           ant."TimestampIndex"
         FROM "AnalogTransition" ant
         WHERE ant."KeyTag" IN (12280000)
           AND ant."Timestamp" < js.start_time
           AND ant."Timestamp" > (js.start_time - INTERVAL '1 day')
           AND ant."Value" = '0.00'
         ORDER BY start_time DESC, ant."Timestamp" ASC, ant."TimestampIndex" ASC
         LIMIT 1) AS
      PRIOR)
   FROM join_records js
   WHERE pctPF IS NULL ),
     result_records AS
  (SELECT js.start_time,
          COALESCE(js.kVAB_down, js.kVAB) AS kVAB,
          COALESCE(js.kVAB_down, js.kVBC) AS kVBC,
          COALESCE(js.kVAB_down, js.kVCA) AS kVCA,
          COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) AS IA,
          COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) AS IB,
          COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))) AS IC,
          COALESCE(js.pctPF_down, COALESCE(js.pctPF, COALESCE(p_prior_pctpf_down.priorpctPF, p_prior_pctpf.priorpctPF))) AS pctPF,

     (SELECT ((1.732 *(((COALESCE(js.kVAB_down, js.kVAB)+ COALESCE(js.kVAB_down, js.kVBC)+ COALESCE(js.kVAB_down, js.kVCA))* 1000)/ 3) * ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) + COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) + COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))))/ 3) * (COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF))/ 100))/ 1000000)) AS MW,

     (SELECT ((1.732 *(((COALESCE(js.kVAB_down, js.kVAB)+ COALESCE(js.kVAB_down, js.kVBC)+ COALESCE(js.kVAB_down, js.kVCA))* 1000)/ 3) * ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) + COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) + COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))))/ 3) * (SIN(ACOS(COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF))/ 100))))/ 1000000)) AS MVar
   FROM join_records js
   LEFT JOIN prior_ia p_ia ON js.start_time = p_ia.start_time
   LEFT JOIN prior_ib p_ib ON js.start_time = p_ib.start_time
   LEFT JOIN prior_ic p_ic ON js.start_time = p_ic.start_time
   LEFT JOIN prior_pctpf p_prior_pctpf ON js.start_time = p_prior_pctpf.start_time
   LEFT JOIN prior_ia_down p_ia_down ON js.start_time = p_ia_down.start_time
   LEFT JOIN prior_pctpf_down p_prior_pctpf_down ON js.start_time = p_prior_pctpf_down.start_time),
     timeseries AS
  (SELECT
     (SELECT '2021-02-06' :: date) + (n || ' minutes'):: interval start_time
   FROM generate_series(0, (24 * 60), 30) n
   LIMIT 48)
SELECT ts.start_time AS start_time,
       rr.kVAB AS kVAB,
       rr.kVBC,
       rr.kVCA,
       rr.IA,
       rr.IB,
       rr.IC,
       rr.pctPF,
       rr.MW,
       rr.MVar
FROM result_records rr
FULL
  JOIN timeseries ts ON rr.start_time :: TIMESTAMP WITHOUT TIME ZONE = ts.start_time
WHERE ts.start_time >= '2021-02-06 00:00:00'
  AND ts.start_time <= '2021-02-06 23:59:59'
ORDER BY ts.start_time ASC