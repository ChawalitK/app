
  SELECT *
  from
    "AnalogTransition" ant 
  where
    ant."KeyTag" IN 
    (
      12480000
    )
    and ant."Timestamp" >= '2021-05-25 18:00:00' 
    and ant."Timestamp" <= '2021-05-26 23:59:59' 
  ORDER BY

    ant."Timestamp" ASC
