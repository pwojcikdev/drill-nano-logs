-- VIEWS

CREATE TABLE elections_all AS
SELECT started.dir0                      as node,
       started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root,
       stopped.election.confirmed        as confirmed,
       stopped.election.state            as state,
       stopped.election.behaviour        as behaviour,
       stopped.election.blocks           as blocks
FROM `*/active_transactions-active_started.log.json` started
         JOIN `*/active_transactions-active_stopped.log.json` stopped
              ON started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;


CREATE VIEW elections_overlap AS
SELECT e1.root              as root,
       e1.id                as e1_id,
       e1.node              as e1_node,
       e1.started_timestamp as e1_started_timestamp,
       e1.stopped_timestamp as e1_stopped_timestamp,
       e1.behaviour         as e1_behaviour,
       e2.id                as e2_id,
       e2.node              as e2_node,
       e2.started_timestamp as e2_started_timestamp,
       e2.stopped_timestamp as e2_stopped_timestamp,
       e2.behaviour         as e2_behaviour,
       e1.blocks            as blocks
FROM elections_all e1
         LEFT JOIN elections_all e2
                   ON e1.root = e2.root
WHERE NOT e1.node = e2.node
  AND e1.confirmed = 'false'
  AND e1.started_timestamp < e2.stopped_timestamp
  AND e1.stopped_timestamp > e2.started_timestamp;


CREATE VIEW msg_processed_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.message                   as message
FROM `*/network_processed-*.log.json` m;


CREATE VIEW msg_processed_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.type          as type,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/network_processed-confirm_ack.log.json` m;


CREATE VIEW msg_sent_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.channel.node_id           as target_node,
       m.dropped                   as dropped,
       m.message                   as message
FROM `*/channel_sent-*.json` m;


CREATE VIEW msg_sent_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.type          as type,
       m.channel.node_id              as target_node,
       m.dropped                      as dropped,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/channel_sent-confirm_ack.log.json` m;


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


CREATE VIEW elections_votes AS
SELECT CAST(v.tstamp AS TIMESTAMP) as tstamp,
       v.dir0                      as node,
       v.root,
       v.hash,
       v.type
FROM `*/election-broadcast_vote.log.json` v;


-- Vote generations for hash
SELECT *
FROM elections_votes
WHERE hash = '${hash_value}'
ORDER BY node, tstamp ASC;



-- VOTES

-- Received overlapping votes
SELECT e.root,
       e.id,
       e.node,
       e.alive_seconds,
       COUNT(*)                         as overlapping,
       COUNT(DISTINCT (m.vote_account)) as overlapping_acks
FROM elections_all e
         JOIN msg_processed_confirm_ack m
              ON m.vote_hash = e.blocks[0].hash AND m.node = e.node
                  AND m.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp
WHERE e.confirmed = 'false'
GROUP BY e.root, e.id, e.node, e.alive_seconds
ORDER BY overlapping_acks DESC, alive_seconds DESC;


-- Received overlapping votes, grouped
SELECT overlapping_acks,
       COUNT(*) as cnt
FROM (SELECT e.root,
             e.id,
             e.node,
             e.alive_seconds,
             COUNT(*)                         as overlapping,
             COUNT(DISTINCT (m.vote_account)) as overlapping_acks
      FROM elections_all e
               JOIN msg_processed_confirm_ack m
                    ON m.vote_hash = e.blocks[0].hash AND m.node = e.node
                        AND m.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp
      WHERE e.confirmed = 'false'
      GROUP BY e.root, e.id, e.node, e.alive_seconds)
GROUP BY overlapping_acks;


-- Attempted overlapping votes
SELECT e.root,
       e.id,
       e.node,
       e.alive_seconds,
       COUNT(*)                 as overlapping,
       COUNT(DISTINCT (v.node)) as overlapping_attempts
FROM elections_all e
         JOIN elections_votes v
              ON v.hash = e.blocks[0].hash AND NOT v.node = e.node
                  AND v.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp
WHERE e.confirmed = 'false'
GROUP BY e.root, e.id, e.node, e.alive_seconds
ORDER BY overlapping_attempts DESC, alive_seconds DESC;


-- Attempted overlapping votes, grouped
SELECT overlapping_attempts,
       COUNT(*) as cnt
FROM (SELECT e.root,
             e.id,
             e.node,
             e.alive_seconds,
             COUNT(*)                 as overlapping,
             COUNT(DISTINCT (v.node)) as overlapping_attempts
      FROM elections_all e
               JOIN elections_votes v
                    ON v.hash = e.blocks[0].hash AND NOT v.node = e.node
                        AND v.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp
      WHERE e.confirmed = 'false'
      GROUP BY e.root, e.id, e.node, e.alive_seconds
      ORDER BY overlapping_attempts DESC, alive_seconds DESC)
GROUP BY overlapping_attempts;