-- Fix B concurrency hammer: xslt_process, well-formed docs only (pgbench aborts a client on any SQL ERROR)
SELECT length(xslt_process(
  xmldoc,
  $$<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="yes" />
<xsl:template match="*">
  <xsl:copy>
     <xsl:copy-of select="@*" />
     <xsl:apply-templates />
  </xsl:copy>
</xsl:template>
</xsl:stylesheet>
$$::text))
FROM hammer_docs WHERE id = 1 + (random() * 499)::int;
