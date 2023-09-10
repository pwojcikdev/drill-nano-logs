-- Create all elections (VIEW)
CREATE VIEW dfs.views.elections_all AS
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
         JOIN
     `*/active_transactions-active_stopped.log.json` stopped
     ON
         started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;


-- Create all elections (TABLE)
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
         JOIN
     `*/active_transactions-active_stopped.log.json` stopped
     ON
         started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id
ORDER BY started_timestamp ASC;


SELECT *
FROM elections_all;


-- Create overlap info (TABLE)
CREATE TABLE elections_overlap AS
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


SELECT *
FROM elections_overlap;