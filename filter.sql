-- Filter overlap
SELECT *
FROM elections_overlap e
WHERE e.root = '86310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E886310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E8'
  AND e.e1_id = '0xd5d7'
  AND e.e1_node = 'rep_4'
ORDER BY e.e2_node, e.e2_started_timestamp ASC;


-- Filter election blocks
SELECT e.root       as root,
       e.id         as id,
       e.node       as node,
       e.block.hash as hash
FROM (SELECT *, FLATTEN(blocks) as block
      FROM elections_all
      WHERE root = '86310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E886310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E8'
        AND id = '0xd5d7'
        AND node = 'rep_4') e;


-- All logs
SELECT *
FROM `*/full.log.json`;


-- Filter all logs with hash
SELECT *
FROM `*/full.log.json`
WHERE payload LIKE '%CEBE78FF2FB74AA46C29B3CDCC7E6188D81883C95A5C530D0FF9F4027FDCB4C6%';