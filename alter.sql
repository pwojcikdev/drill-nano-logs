ALTER
SESSION SET `store.json.all_text_mode` = true;

ALTER
SESSION SET `drill.exec.hashjoin.fallback.enabled` = true;

-- Default
ALTER
SYSTEM SET `drill.exec.memory.operator.output_batch_size` = 16777216;

-- Increase
ALTER
SYSTEM SET `drill.exec.memory.operator.output_batch_size` = 16777216;

ALTER
SYSTEM SET `planner.memory.max_query_memory_per_node` = 25769803776;

ALTER
SYSTEM SET `exec.query.return_result_set_for_ddl` = false