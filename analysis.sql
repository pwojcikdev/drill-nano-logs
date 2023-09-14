-- ELECTIONS

-- Count overlapping nodes
SELECT e.root                      as root,
       e.e1_id                     as id,
       e.e1_node                   as node,
       COUNT(*)                    as overlapping,
       COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
FROM elections_overlap e
GROUP BY e.root, e.e1_id, e.e1_node
ORDER BY overlapping DESC;


-- Count overlapping nodes, grouped
SELECT overlapping_nodes,
       COUNT(*) as cnt
FROM (SELECT e.root                      as root,
             e.e1_id                     as id,
             e.e1_node                   as node,
             COUNT(*)                    as overlapping,
             COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
      FROM elections_overlap e
      GROUP BY e.root, e.e1_id, e.e1_node
      ORDER BY overlapping DESC)
GROUP BY overlapping_nodes;


-- Filter election blocks
SELECT e.root       as root,
       e.id         as id,
       e.node       as node,
       e.block.hash as hash
FROM (SELECT *, FLATTEN(blocks) as block
      FROM elections_all
      WHERE root = '${root_value}'
        AND id = '${id_value}'
        AND node = '${node_value}') e;


-- SINGLE ELECTION

-- Overlapping elections
SELECT *
FROM elections_overlap
WHERE root = '${root_value}'
  AND e1_id = '${id_value}'
  AND e1_node = '${node_value}'
ORDER BY e2_node, e2_started_timestamp ASC;


-- NETWORK

-- Confirm acks received with target hash, by account
SELECT vote_account,
       COUNT(*) as cnt
FROM msg_processed_confirm_ack
WHERE vote_hash = '${hash_value}'
  AND node = '${node_value}'
GROUP BY vote_account;


-- Confirm acks sent with target hash, by node and target node
SELECT node, target_node, COUNT(*) as cnt, vote_account
FROM msg_sent_confirm_ack
WHERE vote_hash = '${hash_value}'
GROUP BY node, target_node, vote_account
ORDER BY node, target_node, cnt DESC;


-- VOTE GENERATION


-- Vote generations for hash
SELECT *
FROM elections_votes
WHERE hash = '${hash_value}'
ORDER BY node, tstamp ASC;


-- VOTES

-- Received overlapping votes (VIEW)
CREATE VIEW elections_overlapping_votes_received AS
WITH AggregatedResults AS
         (SELECT e.root,
                 e.id,
                 e.node,
                 COUNT(*)                                    as overlapping,
                 COALESCE(COUNT(DISTINCT m.vote_account), 0) as overlapping_acks
          FROM elections_not_confirmed e
                   LEFT JOIN msg_processed_confirm_ack m
                             ON m.vote_hash = e.blocks[0].hash
          WHERE m.node = e.node
            AND (m.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp)
          GROUP BY e.root, e.id, e.node, e.alive_seconds
          ORDER BY overlapping_acks DESC, e.alive_seconds DESC)

SELECT e.root,
       e.id,
       e.node,
       e.alive_seconds,
       COALESCE(ar.overlapping, 0)      as overlapping,
       COALESCE(ar.overlapping_acks, 0) as overlapping_acks
FROM elections_not_confirmed e
         LEFT JOIN AggregatedResults ar ON ar.id = e.id AND ar.node = e.node;


-- Received overlapping votes
SELECT *
FROM elections_overlapping_votes_received;


-- Received overlapping votes, grouped
SELECT overlapping_acks,
       COUNT(*) as cnt
FROM elections_overlapping_votes_received
GROUP BY overlapping_acks
ORDER BY overlapping_acks DESC;


-- Attempted overlapping votes (VIEW)
CREATE VIEW elections_overlapping_votes_attempted AS
WITH AggregatedResults AS
         (SELECT e.root,
                 e.id,
                 e.node,
                 e.alive_seconds,
                 COUNT(*)                            as overlapping,
                 COALESCE(COUNT(DISTINCT v.node), 0) as overlapping_attempts
          FROM elections_not_confirmed e
                   LEFT JOIN elections_votes v
                             ON v.hash = e.blocks[0].hash
          WHERE NOT v.node = e.node
            AND (v.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp)
          GROUP BY e.root, e.id, e.node, e.alive_seconds
          ORDER BY overlapping_attempts DESC, e.alive_seconds DESC)

SELECT e.root,
       e.id,
       e.node,
       e.alive_seconds,
       COALESCE(ar.overlapping, 0)          as overlapping,
       COALESCE(ar.overlapping_attempts, 0) as overlapping_attempts
FROM elections_not_confirmed e
         LEFT JOIN AggregatedResults ar ON ar.id = e.id AND ar.node = e.node;


-- Attempted overlapping votes
SELECT *
FROM elections_overlapping_votes_attempted;


-- Attempted overlapping votes, grouped
SELECT overlapping_attempts,
       COUNT(*) as cnt
FROM elections_overlapping_votes_attempted
GROUP BY overlapping_attempts
ORDER BY overlapping_attempts DESC;


-- Received overlapping votes and attempted overlapping votes
SELECT a.root,
       a.id,
       a.node,
       a.alive_seconds,
       a.overlapping as a_overlapping,
       a.overlapping_acks,
       b.overlapping as b_overlapping,
       b.overlapping_attempts
FROM elections_overlapping_votes_received a
         LEFT JOIN elections_overlapping_votes_attempted b
                   ON a.id = b.id AND a.node = b.node;


-- Received overlapping votes and attempted overlapping votes, grouped
SELECT a.overlapping_acks,
       b.overlapping_attempts,
       COUNT(*) as cnt
FROM elections_overlapping_votes_received a
         FULL JOIN elections_overlapping_votes_attempted b
                   ON a.id = b.id AND a.node = b.node
GROUP BY a.overlapping_acks, b.overlapping_attempts
ORDER BY a.overlapping_acks DESC, b.overlapping_attempts DESC;


-- Received acks without an attempt wtf
SELECT *
FROM (SELECT *
      FROM elections_overlapping_votes_received a
               FULL JOIN elections_overlapping_votes_attempted b
                         ON a.id = b.id AND a.node = b.node)
-- WHERE overlapping_attempts = 0 AND overlapping_acks > 0;
WHERE overlapping_acks > overlapping_attempts;