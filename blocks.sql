-- All processed blocks
CREATE VIEW blocks_processed AS
SELECT CAST(tstamp AS TIMESTAMP) as tstamp,
       dir0                      as node,
       result,
       block
FROM `*/blockprocessor-block_processed.log.json`;


SELECT *
FROM blocks_processed;


-- Block database # INCOMPLETE (missing already processed blocks)
SELECT DISTINCT b.block.type     as block_type,
                b.block.hash     as block_hash,
                b.block.account  as block_account,
                b.block.previous as block_previous,
                b.block.link     as block_link,
                b.block.balance  as block_balance,
                CASE
                    WHEN NOT b.block.previous = '0000000000000000000000000000000000000000000000000000000000000000'
                        THEN b.block.previous
                    ELSE b.block.link
                    END          as previous_block
FROM blocks_processed b;


-- Confirmed blocks
CREATE VIEW blocks_confirmed AS
SELECT CAST(tstamp AS TIMESTAMP) as tstamp,
       dir0                      as node,
       b.block.hash              as hash,
       b.block                   as block
FROM `*/node-process_confirmed.log.json` b;