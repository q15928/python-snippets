SELECT
  reqtimeunix,
  FORMAT_TIMESTAMP('%d/%b/%Y:%H:%M:%S %z', TIMESTAMP_SECONDS(reqtimeunix)) AS reqtimestamp,
  SPLIT(requesttext, '/')[
OFFSET
  (6)] AS file,
  SPLIT(SPLIT(requesttext, '.')[
  OFFSET
    (1)], '?')[
OFFSET
  (0)] AS extension,
  responsecode,
  bytesize,
  case 
  when starts_with(useragent, 'Apple') then 'Native Apple'
  when starts_with(useragent, 'Mozilla') then 'Browser'
  else 'Native Android'
  end as clienttype,
  useragent,requesttext,cdn

FROM
  my_dataset.cdn_request
LIMIT
  50