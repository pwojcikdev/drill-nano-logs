-- All processed messages
SELECT CAST(m.tstamp AS TIMESTAMP) as tstamp,
       m.dir0                      as node,
       m.message.header.type       as type,
       m.message
FROM `*/network-message_received.log.json` m;


-- Confirm acks processed
SELECT m.tstamp,
       m.node,
       m.type,
       m.message.vote.account     as vote_account,
       m.message.vote.`timestamp` as vote_timestamp,
       m.message.vote.hashes      as vote_hashes
FROM msg_processed_all m
WHERE m.type = 'confirm_ack';


-- Confirm acks processed, flattened
SELECT m.tstamp,
       m.node,
       m.type,
       m.message.vote.account         as vote_account,
       m.message.vote.`timestamp`     as vote_timestamp,
       FLATTEN(m.message.vote.hashes) as vote_hash
FROM msg_processed_all m
WHERE m.type = 'confirm_ack';


-- Confirm acks processed with target hash
SELECT *
FROM msg_processed_confirm_ack
WHERE vote_hash = '${hash_value}'
  AND node = '${node_value}';


-- Confirm acks received with target hash, by account
SELECT vote_account,
       COUNT(*) as cnt
FROM msg_processed_confirm_ack
WHERE vote_hash = '${hash_value}'
  AND node = '${node_value}'
GROUP BY vote_account;


-- Confirm acks sent with target hash
SELECT *
FROM msg_sent_confirm_ack
WHERE vote_hash = '${hash_value}';


-- Confirm acks sent with target hash, by node and target node
SELECT node, target_node, COUNT(*) as cnt, vote_account
FROM msg_sent_confirm_ack
WHERE vote_hash = '${hash_value}'
GROUP BY node, target_node, vote_account
ORDER BY node, target_node, cnt DESC;