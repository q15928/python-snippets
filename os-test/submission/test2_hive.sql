SELECT reqtimeunix,
         from_unixtime(reqtimeunix, 'dd/MMM/yyyy HH:mm:ss Z') AS reqtimestamp,
         -- unlike presto, the first element has index 0 in Hive
         split(requesttext, '/')[10] AS file,
         split(split(requesttext, '.')[1], '?')[0] as extension,
         responsecode,
         bytesize,        
    CASE
      WHEN instr(useragent, 'Apple') = 1 THEN 'Native Apple'
      WHEN instr(useragent, 'Mozilla') = 1 THEN 'Browser'
      WHEN instr(useragent, 'EPLPlayer') = 1 THEN 'Native Android'
    END AS clienttype,
    CASE 
      WHEN instr(useragent, 'EPLPlayer') = 1 THEN split(split(useragent, ' ')[0], '/')[1]
      ELSE NULL
    END AS androidnativeappversion,
    CASE 
      WHEN instr(useragent, 'Apple') = 1 THEN split(split(useragent, '(')[1], ';')[0]
      ELSE NULL
    END AS applenativeappdevice,
    CASE
      WHEN instr(useragent, 'Apple') = 1 THEN 
        CASE
         WHEN instr(split(useragent, ';')[2], ' CPU') >= 1 THEN substr(split(split(useragent, ';')[2], ' like')[0], 6)
         ELSE trim(split(useragent, ';')[2])
         END
      WHEN instr(useragent, 'Mozilla') = 1 THEN split(split(useragent, '(')[1], ')')[0]
      WHEN instr(useragent, 'EPLPlayer') = 1 THEN split(split(useragent, ';')[1], ')')[0]
    END AS clientos,
    useragent,
    requesttext,
    cdn
FROM "raw_db_dev"."cdn_req"
