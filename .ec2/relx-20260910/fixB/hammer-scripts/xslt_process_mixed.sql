-- Fix B error-path concurrency: xslt_process against malformed docs (must ERROR cleanly per-transaction, never crash)
DO $$
DECLARE
  d text;
BEGIN
  SELECT xmldoc INTO d FROM hammer_docs WHERE xmldoc NOT LIKE '<doc><int>%</int><name>%' ORDER BY random() LIMIT 1;
  BEGIN
    PERFORM length(xslt_process(
      d,
      $x$<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="yes" />
<xsl:template match="*">
  <xsl:copy>
     <xsl:copy-of select="@*" />
     <xsl:apply-templates />
  </xsl:copy>
</xsl:template>
</xsl:stylesheet>
$x$::text));
  EXCEPTION WHEN OTHERS THEN
    NULL; -- expected: malformed input document -> ERROR, caught here
  END;
END $$;
