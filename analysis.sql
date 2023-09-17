-- ELECTIONS

-- Count overlapping nodes
SELECT e.root                      as root,
       e.id                        as id,
       e.node                      as node,
       COUNT(*)                    as overlapping,
       COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
FROM elections_overlap e
GROUP BY e.root, e.id, e.node
ORDER BY overlapping DESC;


-- Count overlapping nodes, grouped
SELECT overlapping_nodes,
       COUNT(*) as cnt
FROM (SELECT e.root                      as root,
             e.id                        as id,
             e.node                      as node,
             COUNT(*)                    as overlapping,
             COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
      FROM elections_overlap e
      GROUP BY e.root, e.id, e.node
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
  AND id = '${id_value}'
  AND node = '${node_value}'
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


-- Successful vote generator candidates
SELECT voted,
       COUNT(*) as cnt
FROM vote_generator_attempts
GROUP BY voted;


-- Attempted overlapping vote generator candidates
CREATE VIEW elections_overlapping_vote_generator AS
WITH AggregatedResults AS
         (SELECT e.root,
                 e.id,
                 e.node,
                 e.alive_seconds,
                 COUNT(*)                                                       as overlapping,
                 COALESCE(COUNT(DISTINCT v.node), 0)                            as overlapping_attempts,
                 COALESCE(COUNT(DISTINCT (
                     CASE WHEN v.voted = 'true' THEN v.node ELSE NULL END)), 0) as overlapping_attempts_successful,
                 COUNT(CASE WHEN v.voted = 'true' THEN 1 ELSE NULL END)         as vote_success,
                 COUNT(CASE WHEN v.voted = 'false' THEN 1 ELSE NULL END)        as vote_fail
          FROM elections_not_confirmed e
                   LEFT JOIN vote_generator_attempts v
                             ON v.hash = e.blocks[0].hash
          WHERE NOT v.node = e.node
            AND (v.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp)
          GROUP BY e.root, e.id, e.node, e.alive_seconds
          ORDER BY overlapping_attempts DESC, e.alive_seconds DESC)

SELECT e.root,
       e.id,
       e.node,
       e.alive_seconds,
       COALESCE(ar.overlapping, 0)                     as overlapping,
       COALESCE(ar.overlapping_attempts, 0)            as overlapping_attempts,
       COALESCE(ar.overlapping_attempts_successful, 0) as overlapping_attempts_successful,
       COALESCE(ar.vote_success, 0)                    as vote_success,
       COALESCE(ar.vote_fail, 0)                       as vote_fail
FROM elections_not_confirmed e
         LEFT JOIN AggregatedResults ar ON ar.id = e.id AND ar.node = e.node;


-- Successful overlapping attempts, grouped
SELECT overlapping_attempts,
       overlapping_attempts_successful,
       COUNT(*) as cnt
FROM elections_overlapping_vote_generator
GROUP BY overlapping_attempts, overlapping_attempts_successful
ORDER BY overlapping_attempts DESC, overlapping_attempts_successful DESC;


-- Successful overlapping attempts and received acks, grouped
SELECT overlapping_attempts,
       overlapping_attempts_successful,
       overlapping_acks,
       COUNT(*) as cnt
FROM elections_overlapping_vote_generator att
         FULL JOIN elections_overlapping_votes_received rcv
                   ON att.id = rcv.id AND att.node = rcv.node
GROUP BY overlapping_attempts, overlapping_attempts_successful, overlapping_acks
ORDER BY overlapping_attempts DESC, overlapping_attempts_successful DESC, overlapping_acks DESC;


-- Messages sent and received, grouped
WITH AggregatedSent AS
         (SELECT MIN(snt.tstamp)                   as tstamp,
                 snt.id                            as id,
                 snt.node                          as node,
                 COUNT(DISTINCT (snt.target_node)) as sent
          FROM msg_sent_all snt
          GROUP BY snt.id, snt.node),

     AggregatedReceived AS
         (SELECT MIN(snt.tstamp)            as tstamp,
                 snt.id                     as id,
                 snt.node                   as node,
                 COUNT(DISTINCT (rcv.node)) as processed
          FROM msg_sent_all snt
                   LEFT JOIN msg_processed_all rcv
                             ON snt.id = rcv.id
          GROUP BY snt.id, snt.node)

SELECT sent,
       processed,
       COUNT(*) as cnt
FROM AggregatedReceived arcv
         LEFT JOIN AggregatedSent asnt
                   ON asnt.id = arcv.id
GROUP BY sent, processed
ORDER BY sent DESC, processed DESC;


-- Messages sent and received, grouped by type
WITH AggregatedSent AS
         (SELECT MIN(snt.tstamp)                   as tstamp,
                 snt.id                            as id,
                 snt.node                          as node,
                 snt.type                          as type,
                 COUNT(DISTINCT (snt.target_node)) as sent
          FROM msg_sent_all snt
          GROUP BY snt.id, snt.node, snt.type),

     AggregatedReceived AS
         (SELECT MIN(snt.tstamp)            as tstamp,
                 snt.id                     as id,
                 snt.node                   as node,
                 snt.type                   as type,
                 COUNT(DISTINCT (rcv.node)) as processed
          FROM msg_sent_all snt
                   LEFT JOIN msg_processed_all rcv
                             ON snt.id = rcv.id
          GROUP BY snt.id, snt.node, snt.type)

SELECT arcv.type as type,
       sent,
       processed,
       COUNT(*)  as cnt
FROM AggregatedReceived arcv
         LEFT JOIN AggregatedSent asnt
                   ON asnt.id = arcv.id
GROUP BY type, sent, processed
ORDER BY type DESC, sent DESC, processed DESC;


-- Correlate each vote generator attempt with an election
CREATE VIEW vote_generator_attempts_to_election AS
SELECT att.tstamp as tstamp,
       att.node   as node,
       att.hash   as hash,
       att.voted  as voted,
       att.final  as final,
       att.block  as block,
       e.id       as e_id,
       e.node     as e_node,
       e.root     as e_root
FROM vote_generator_attempts att
         LEFT JOIN elections_all e
                   ON att.hash = e.blocks[0].hash AND att.node = e.node
WHERE (att.tstamp BETWEEN e.started_timestamp AND e.stopped_timestamp);


-- Overlapping elections with vote attempts info
CREATE VIEW elections_overlapping_with_attempts AS
WITH AggregatedOverlap AS
         (SELECT e.root                      as root,
                 e.id                        as id,
                 e.node                      as node,
                 e.behaviour                 as behaviour,
                 COUNT(*)                    as overlapping,
                 COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
          FROM elections_overlap e
          GROUP BY e.root, e.id, e.node, e.behaviour
          ORDER BY overlapping DESC),

     OverlappingResults AS
         (SELECT e.root                 as root,
                 e.id                   as id,
                 e.node                 as node,
                 agg.overlapping        as overlapping,
                 agg.overlapping_nodes  as overlapping_nodes,
                 e.e2_id                as e2_id,
                 e.e2_node              as e2_node,
                 e.e2_started_timestamp as e2_started_timestamp,
                 e.e2_stopped_timestamp as e2_stopped_timestamp,
                 e.e2_behaviour         as e2_behaviour
          FROM elections_overlap e
                   LEFT JOIN AggregatedOverlap agg
                             ON e.id = agg.id
          ORDER BY agg.overlapping_nodes DESC, agg.overlapping DESC, e.root, e.id, e.node, e.e2_node, e.e2_started_timestamp),

     OverlappingResultsWithAttempts AS
         (SELECT ovr.root                 as root,
                 ovr.id                   as id,
                 ovr.node                 as node,
                 ovr.overlapping          as overlapping,
                 ovr.overlapping_nodes    as overlapping_nodes,
                 ovr.e2_id                as e2_id,
                 ovr.e2_node              as e2_node,
                 ovr.e2_started_timestamp as e2_started_timestamp,
                 ovr.e2_stopped_timestamp as e2_stopped_timestamp,
                 ovr.e2_behaviour         as e2_behaviour,
                 att.tstamp               as att_tstamp,
                 att.hash                 as att_hash,
                 att.voted                as att_voted,
                 att.final                as att_final,
                 att.block.previous       as att_previous_hash
          FROM OverlappingResults ovr
                   LEFT JOIN vote_generator_attempts_to_election att
                             ON ovr.e2_id = att.e_id
          ORDER BY ovr.overlapping_nodes DESC, ovr.overlapping DESC, ovr.root, ovr.id, ovr.node, ovr.e2_node, ovr.e2_started_timestamp, att.tstamp),

     PreviousStarted AS
         (SELECT ovr.e2_node           as node,
                 ovr.att_previous_hash as previous_hash,
                 COUNT(*)              as cnt
          FROM OverlappingResultsWithAttempts ovr
                   JOIN elections_all e
                        ON e.blocks[0].hash = ovr.att_previous_hash AND e.node = ovr.e2_node
          GROUP BY ovr.e2_node, ovr.att_previous_hash)


SELECT ovr.root,
       ovr.id,
       ovr.node,
       ovr.overlapping,
       ovr.overlapping_nodes,
       ovr.e2_id,
       ovr.e2_node,
       ovr.e2_started_timestamp,
       ovr.e2_stopped_timestamp,
       ovr.e2_behaviour,
       ovr.att_tstamp,
       ovr.att_hash,
       ovr.att_voted,
       ovr.att_final,
       ovr.att_previous_hash,
       CASE
           WHEN ps.cnt > 0 THEN TRUE
           ELSE FALSE
           END as previous_started
FROM OverlappingResultsWithAttempts ovr
         LEFT JOIN PreviousStarted ps
                   ON ps.previous_hash = ovr.att_previous_hash AND ps.node = ovr.e2_node
ORDER BY ovr.overlapping_nodes DESC, ovr.overlapping DESC, ovr.root, ovr.id, ovr.node, ovr.e2_node, ovr.e2_started_timestamp, ovr.att_tstamp;

SELECT *
FROM elections_overlapping_with_attempts;


