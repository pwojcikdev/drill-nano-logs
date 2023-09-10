-- All logs
SELECT *
FROM `*/full.log.json`;


-- Filter all logs with hash
SELECT *
FROM `*/full.log.json`
WHERE payload LIKE '%${hash_value}%';