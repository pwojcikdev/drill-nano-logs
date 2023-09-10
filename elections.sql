ALTER
SESSION SET `store.json.all_text_mode` = true;
ALTER
SESSION SET `drill.exec.hashjoin.fallback.enabled` = true;


-- Duplicate IDs
SELECT started.election.id as id,
       COUNT(*)            as cnt
FROM `rep_0/active_transactions-active_started.log.json` started
GROUP BY started.election.id
ORDER BY cnt DESC;


-- Started elections
SELECT CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       started.election.root             as root,
       started.election                  as started_election
FROM `rep_0/active_transactions-active_started.log.json` started
ORDER BY started_timestamp ASC;


-- Stopped elections
SELECT CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       stopped.election.root             as root,
       stopped.election                  as stopped_election
FROM `rep_0/active_transactions-active_stopped.log.json` stopped
ORDER BY stopped_timestamp ASC;


-- All
SELECT started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root
FROM `rep_0/active_transactions-active_started.log.json` started
         LEFT JOIN `rep_0/active_transactions-active_stopped.log.json` stopped
                   ON started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;


-- Never stopped
SELECT *
FROM (SELECT started.election.id               as id,
             CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
             CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
             started.election.root             as root
      FROM `rep_0/active_transactions-active_started.log.json` started
               LEFT JOIN `rep_0/active_transactions-active_stopped.log.json` stopped
                         ON started.election.id = stopped.election.id)
WHERE stopped_timestamp IS NULL
ORDER BY started_timestamp ASC;


-- Longest running
SELECT started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root,
       stopped.election.confirmed        as confirmed,
       stopped.election.state            as state,
       stopped.election.behaviour        as behaviour
FROM `rep_0/active_transactions-active_started.log.json` started
         JOIN `rep_0/active_transactions-active_stopped.log.json` stopped
              ON started.election.id = stopped.election.id
ORDER BY alive_seconds DESC, started_timestamp ASC;


-- Longest running (non expired)
SELECT started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root,
       stopped.election.confirmed        as confirmed,
       stopped.election.state            as state,
       stopped.election.behaviour        as behaviour
FROM `rep_0/active_transactions-active_started.log.json` started
         JOIN `rep_0/active_transactions-active_stopped.log.json` stopped
              ON started.election.id = stopped.election.id
WHERE NOT stopped.election.state = 'expired_unconfirmed'
ORDER BY alive_seconds DESC, started_timestamp ASC;


-- Longest running (expired)
SELECT started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root,
       stopped.election.confirmed        as confirmed,
       stopped.election.state            as state,
       stopped.election.behaviour        as behaviour
FROM `rep_0/active_transactions-active_started.log.json` started
         JOIN `rep_0/active_transactions-active_stopped.log.json` stopped
              ON started.election.id = stopped.election.id
WHERE stopped.election.confirmed = 'false'
ORDER BY alive_seconds DESC, started_timestamp ASC;


-- All on all reps
SELECT started.dir0                      as node,
       started.election.id               as id,
       CAST(started.tstamp AS TIMESTAMP) as started_timestamp,
       CAST(stopped.tstamp AS TIMESTAMP) as stopped_timestamp,
       TIMESTAMPDIFF(SECOND, CAST(started.tstamp AS TIMESTAMP), CAST(stopped.tstamp AS TIMESTAMP))
                                         as alive_seconds,
       started.election.root             as root,
       stopped.election.confirmed        as confirmed,
       stopped.election.state            as state,
       stopped.election.behaviour        as behaviour
FROM `*/active_transactions-active_started.log.json` started
         JOIN `*/active_transactions-active_stopped.log.json` stopped
              ON started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;


-- Using election view
SELECT *
FROM elections_all;


-- Expired unconfirmed all reps
SELECT *
FROM elections_all
WHERE confirmed = 'false';


-- WHERE startA < endB AND endA > startB

-- Overlap WIP
SELECT (CAST('2023-09-09 21:38:58' AS TIMESTAMP) < e1.stopped_timestamp AND CAST('2023-09-09 21:39:28' AS TIMESTAMP) > e1.started_timestamp) as overlap,
       *
FROM elections_all e1
WHERE e1.root = 'EC34C4DD07105618DD705366A861C2F56BCB425A7A817CBCBCCC940534248887EC34C4DD07105618DD705366A861C2F56BCB425A7A817CBCBCCC940534248887';


-- Overlap WIP
SELECT *
FROM elections_all e1
WHERE e1.root = 'EC34C4DD07105618DD705366A861C2F56BCB425A7A817CBCBCCC940534248887EC34C4DD07105618DD705366A861C2F56BCB425A7A817CBCBCCC940534248887'
  AND CAST('2023-09-09 21:38:58' AS TIMESTAMP)
    < e1.stopped_timestamp
  AND CAST('2023-09-09 21:39:28' AS TIMESTAMP)
    > e1.started_timestamp;


-- All with overlap info #BAD
SELECT *,
       (SELECT COUNT(*) as overlapping
        FROM elections_all e1
        WHERE e1.root = e2.root
          AND CAST e1.started_timestamp < e2.stopped_timestamp AND e1.stopped_timestamp > e2.started_timestamp)) as overlapping
FROM elections_all e2
WHERE confirmed = 'false';


-- All with overlap info
SELECT e2.id,
       e2.root,
       COUNT(e1.root) as overlapping
FROM elections_all e2
         LEFT JOIN elections_all e1
                   ON e1.root = e2.root
                       AND e1.started_timestamp < e2.stopped_timestamp
                       AND e1.stopped_timestamp > e2.started_timestamp
WHERE e2.confirmed = 'false'
GROUP BY e2.id, e2.root, e2.started_timestamp, e2.stopped_timestamp, e2.confirmed;


-- All with overlap info
SELECT e2.id,
       e2.root,
       e2.node
FROM elections_all e2
         LEFT JOIN elections_all e1
                   ON e1.root = e2.root
WHERE e2.confirmed = 'false'
GROUP BY e2.id, e2.root, e2.node;


-- All with overlap info
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
       e2.behaviour         as e2_behaviour
FROM elections_all e1
         LEFT JOIN elections_all e2
                   ON e1.root = e2.root
WHERE NOT e1.node = e2.node
  AND e1.confirmed = 'false'
  AND e1.started_timestamp < e2.stopped_timestamp
  AND e1.stopped_timestamp > e2.started_timestamp;


-- Test overlap info table
SELECT *
FROM elections_overlap e;


-- Overlap info count
SELECT e.root    as root,
       e.e1_id   as id,
       e.e1_node as node,
       COUNT(*)  as overlapping
FROM elections_overlap e
GROUP BY e.root, e.e1_id, e.e1_node
ORDER BY overlapping DESC;


-- Overlap info count by node
SELECT e.root                      as root,
       e.e1_id                     as id,
       e.e1_node                   as node,
       COUNT(*)                    as overlapping,
       COUNT(DISTINCT (e.e2_node)) as overlapping_nodes
FROM elections_overlap e
GROUP BY e.root, e.e1_id, e.e1_node
ORDER BY overlapping DESC;


-- Filter overlap
SELECT *
FROM elections_overlap e
WHERE e.root = '86310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E886310FE3E3E0F92B57066FF33B396E097146A694B6DA2D7023D296EF882765E8'
  AND e.e1_id = '0xd5d7'
  AND e.e1_node = 'rep_4'
ORDER BY e.e2_node, e.e2_started_timestamp ASC;


-- Elections with blocks
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
         JOIN
     `*/active_transactions-active_stopped.log.json` stopped
     ON
         started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;