-- All processed messages
CREATE VIEW msg_processed_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.message                   as message
FROM `*/network-message_received.log.json` m;


SELECT *
FROM msg_processed_all;


-- Confirm acks processed, flattened
CREATE VIEW msg_processed_confirm_ack AS
SELECT m.tstamp,
       m.node,
       m.type,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM msg_processed_all m
WHERE m.type = 'confirm_ack';


SELECT *
FROM msg_processed_confirm_ack;


-- All sent messages
CREATE VIEW msg_sent_all AS
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.channel.node_id           as target_node,
       m.message                   as message
FROM `*/channel-message_sent.log.json` m;


SELECT *
FROM msg_sent_all;


-- Confirm acks sent
CREATE VIEW msg_sent_confirm_ack AS
SELECT m.tstamp,
       m.node,
       m.type,
       m.target_node,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM msg_sent_all m
WHERE m.type = 'confirm_ack';


-- Confirm acks sent (TABLE)
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


-- WTF

SELECT COUNT(*)
FROM msg_sent_all m
WHERE m.type = 'confirm_ack';


SELECT COUNT(*)
FROM msg_sent_confirm_ack;


SELECT COUNT(*)
FROM (SELECT m.tstamp,
             m.node,
             m.type,
             m.target_node,
             m.message.vote.account     as vote_account,
             m.message.vote.`timestamp` as vote_timestamp
--              FLATTEN(m.message.vote.hashes) as vote_hash
      FROM msg_sent_all m
      WHERE m.type = 'confirm_ack');


SELECT len
FROM (SELECT SIZE (m.message.vote.hashes) as len
      FROM msg_sent_all m
      WHERE m.type = 'confirm_ack' AND m.message.vote IS NOT NULL)
      GROUP BY len;


-- WTF

SELECT *
FROM msg_sent_confirm_ack;


SELECT typeof(vote_hash) as tpe
FROM msg_sent_confirm_ack
GROUP BY tpe;


SELECT CHAR_LENGTH(vote_hash) as len
FROM msg_sent_confirm_ack
ORDER BY len ASC;


SELECT CHAR_LENGTH(CAST(vote_hash as VARCHAR)) as len
FROM msg_sent_confirm_ack
GROUP BY len;


SELECT CAST(vote_hash as VARCHAR) as vote_hash
FROM msg_sent_confirm_ack
WHERE vote_hash IS NULL;


CREATE TABLE msg_sent_confirm_ack AS
SELECT m.tstamp,
       m.node,
       m.type,
       m.target_node,
       m.message.vote.account                          as vote_account,
       m.message.vote.`timestamp`                      as vote_timestamp,
       CAST(FLATTEN(m.message.vote.hashes) as VARCHAR) as vote_hash
FROM msg_sent_all m
WHERE m.type = 'confirm_ack';