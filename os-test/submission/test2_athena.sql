SELECT reqtimeunix,
         concat(date_format(from_unixtime(reqtimeunix), '%d/%b/%Y:%T'), ' +0000') AS reqtimestamp,
         -- seems there is not timezone format '%z'
         split_part(requesttext, '/', 11) as file,
         -- split(requesttext, '/')[11] AS file,  -- Athena complains array out of boundary
         split(split(requesttext, '.')[2], '?')[1] as extension,
         responsecode,
         bytesize,        
    CASE
      WHEN strpos(useragent, 'Apple') = 1 THEN 'Native Apple'
      WHEN strpos(useragent, 'Mozilla') >= 1 THEN 'Browser' -- Strange, I got error if just use '= 1'
      WHEN strpos(useragent, 'EPLPlayer') = 1 THEN 'Native Android'
    END AS clienttype,
    CASE 
      WHEN strpos(useragent, 'EPLPlayer') = 1 THEN split(split(useragent, ' ')[1], '/')[2]
      ELSE NULL
    END AS androidnativeappversion,
    CASE 
      WHEN strpos(useragent, 'Apple') = 1 THEN split(split(useragent, '(')[2], ';')[1]
      ELSE NULL
    END AS applenativeappdevice,
    CASE
      WHEN strpos(useragent, 'Apple') = 1 THEN 
        CASE
         WHEN strpos(split(useragent, ';')[3], ' CPU') >= 1 THEN substr(split(split(useragent, ';')[3], ' like')[1], 6)
         ELSE trim(split(useragent, ';')[3])
         END
      WHEN strpos(useragent, 'Mozilla') >= 1 THEN split(split(useragent, '(')[2], ')')[1]
      WHEN strpos(useragent, 'EPLPlayer') = 1 THEN split(split(useragent, ';')[2], ')')[1]
    END AS clientos,
    useragent,
    requesttext,
    cdn
FROM "raw_db_dev"."cdn_req"
