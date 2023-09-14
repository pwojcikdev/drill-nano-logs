ALTER
SESSION SET `store.json.all_text_mode` = true;

ALTER
SYSTEM SET `store.json.all_text_mode` = true;


ALTER
SESSION SET `drill.exec.hashjoin.fallback.enabled` = true;

ALTER
SYSTEM SET `drill.exec.hashjoin.fallback.enabled` = true;


-- Default
ALTER
SESSION SET `drill.exec.memory.operator.output_batch_size` = 16777216;

-- Increase
ALTER
SESSION SET `drill.exec.memory.operator.output_batch_size` = 16777216;

ALTER
SESSION SET `planner.memory.max_query_memory_per_node` = 25769803776;

ALTER
SESSION SET `exec.query.return_result_set_for_ddl` = false;

-- Experimental
ALTER
SESSION SET `exec.enable_union_type` = true;

ALTER
SESSION SET `exec.errors.verbose` = true;


ALTER
SESSION SET `planner.enable_nljoin_for_scalar_only` = false;
