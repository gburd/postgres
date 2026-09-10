-- Fix B concurrency hammer: setup
CREATE TABLE IF NOT EXISTS hammer_docs (id serial primary key, xmldoc text);
TRUNCATE hammer_docs;
INSERT INTO hammer_docs (xmldoc)
SELECT '<doc><int>' || g || '</int><name>row' || g || '</name></doc>'
FROM generate_series(1, 500) g;
-- a few malformed documents interleaved, to exercise error paths concurrently
INSERT INTO hammer_docs (xmldoc) VALUES
  ('<doc><int>1</int'),           -- truncated, not well-formed
  ('not xml at all'),
  ('<doc><int>&</int></doc>');    -- bad entity
