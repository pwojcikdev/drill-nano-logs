-- Check duplicate message ids (should be empty)
SELECT *
FROM (SELECT id,
             MIN(tstamp)                                          as tstamp_min,
             MAX(tstamp)                                          as tstamp_max,
             ABS(TIMESTAMPDIFF(SECOND, MIN(tstamp), MAX(tstamp))) AS tstamp_diff
      FROM msg_sent_all
      GROUP BY id)
WHERE tstamp_diff > 0;


-- Messages sent and received by the same node (should be empty)
SELECT snt.id as id snt.node as node, snt.type as type
FROM msg_sent_all snt
         LEFT JOIN msg_processed_all rcv
                   ON snt.id = rcv.id
WHERE snt.node = rcv.node;