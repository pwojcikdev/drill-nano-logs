-- ELECTIONS

SELECT COUNT(*) as total
FROM `*/active_transactions-active_started.log.json` started
         JOIN `*/active_transactions-active_stopped.log.json` stopped
              ON started.dir0 = stopped.dir0 AND started.election.id = stopped.election.id;


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

SELECT *
FROM elections_all;


CREATE VIEW elections_not_confirmed AS
SELECT *
FROM elections_all
WHERE confirmed = 'false';

SELECT *
FROM elections_not_confirmed;


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

SELECT *
FROM elections_overlap;


CREATE VIEW elections_votes AS
SELECT CAST(v.tstamp AS TIMESTAMP) as tstamp,
       v.dir0                      as node,
       v.root,
       v.hash,
       v.type
FROM `*/election-broadcast_vote.log.json` v;

SELECT *
FROM elections_votes;


-- MESSAGES PROCESSED

SELECT *
FROM `*/network_processed-*.log.json`;


CREATE VIEW msg_processed_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.id         as id,
       m.message.header.type       as type,
       m.message                   as message
FROM `*/network_processed-*.log.json` m;


CREATE VIEW msg_processed_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.id            as id,
       m.message.header.type          as type,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/network_processed-confirm_ack.log.json` m;


-- MESSAGES SENT

SELECT *
FROM `*/channel_sent-*.json`;


CREATE VIEW msg_sent_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.id         as id,
       m.message.header.type       as type,
       m.channel.node_id           as target_node,
       m.dropped                   as dropped,
       m.message                   as message
FROM `*/channel_sent-*.json` m;

SELECT *
FROM msg_sent_all;


CREATE VIEW msg_sent_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.id            as id,
       m.message.header.type          as type,
       m.channel.node_id              as target_node,
       m.dropped                      as dropped,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/channel_sent-confirm_ack.log.json` m;

SELECT *
FROM msg_sent_confirm_ack;


-- VOTE GENERATOR

CREATE VIEW vote_generator_attempts AS
SELECT CAST(c.tstamp AS TIMESTAMP) as tstamp,
       c.dir0                      as node,
       c.should_vote               as voted,
       c.is_final                  as final,
       c.block.hash                as hash,
       c.block                     as block
FROM `*/vote_generator-candidate_processed.log.json` c;