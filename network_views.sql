-- RECEIVED

-- All processed messages
CREATE VIEW msg_processed_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.message                   as message
FROM `*/network-*.log.json` m;


SELECT *
FROM msg_processed_all;


-- Confirm acks processed, flattened
CREATE VIEW msg_processed_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.type          as type,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/network-confirm_ack.log.json` m;


SELECT *
FROM msg_processed_confirm_ack;


-- SENT

-- All sent messages
CREATE VIEW msg_sent_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.channel.node_id           as target_node,
       m.message                   as message
FROM `*/channel_sent-*.json` m;


SELECT *
FROM msg_sent_all;


-- Confirm acks sent
CREATE VIEW msg_sent_confirm_ack AS
SELECT CAST(m.tstamp AS TIMESTAMP)    as tstamp,
       m.dir0                         as node,
       m.message.header.type          as type,
       m.channel.node_id              as target_node,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM `*/channel_sent-confirm_ack.log.json` m;


-- Confirm acks sent (TABLE) #OLD
CREATE TABLE msg_sent_confirm_ack AS
SELECT m.tstamp,
       m.node,
       m.type,
       m.target_node,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM msg_sent_all m
WHERE m.type = 'confirm_ack';


SELECT *
FROM msg_sent_confirm_ack;