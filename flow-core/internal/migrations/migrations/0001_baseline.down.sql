-- Down migration is a no-op. The baseline schema is never destroyed
-- programmatically — operators take a postgres backup if they want to
-- roll back schema changes.
SELECT 1;
