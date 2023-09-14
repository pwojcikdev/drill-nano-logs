CREATE VIEW elections_votes AS
SELECT CAST(v.tstamp AS TIMESTAMP) as tstamp,
       v.dir0                      as node,
       v.root                      as root,
       v.winner.hash               as hash,
       v.type                      as type,
       v.winner                    as block
FROM `*/election-broadcast_vote.log.json` v;


SELECT *
FROM elections_votes;


-- Valid/invalid voting attempts
SELECT *
FROM elections_votes v
         JOIN blocks_confirmed b ON v.hash = b.hash;