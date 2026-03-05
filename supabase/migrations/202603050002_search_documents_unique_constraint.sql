-- Add unique constraint on search_documents(tenant_id, entity_id, doc_type, content)
-- Required for bulk upsert via on_conflict=tenant_id,entity_id,doc_type,content.
--
-- Step 1: Remove duplicate rows (keep the most recently indexed one per group).
--         Necessary when running on a database that already has data from repeat runs.
delete from public.search_documents
where id in (
  select id from (
    select
      id,
      row_number() over (
        partition by tenant_id, entity_id, doc_type, content
        order by indexed_at desc
      ) as rn
    from public.search_documents
  ) ranked
  where rn > 1
);

-- Step 2: Add the unique constraint.
alter table public.search_documents
  add constraint uq_search_documents_tenant_entity_doc_content
  unique (tenant_id, entity_id, doc_type, content);
