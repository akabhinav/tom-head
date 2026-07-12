package io.chronodim.export;

/**
 * The frozen published column contract (R-PUB-3, §11.5).
 *
 * <p>Every published row = payload columns plus:
 * <pre>
 * _valid_from      timestamp (ISO-8601 UTC string in JSONL parts)
 * _valid_to        timestamp or null (null = open as of this belief)
 * _tx_time         timestamp
 * _txn_id          long
 * _op              INSERT | UPDATE | DELETE
 * _schema_version  int
 * </pre>
 *
 * The publisher appends one row per version record written by a transaction —
 * including superseding rewrites (corrections). Consumers derive latest-knowledge
 * SCD2 shape with the view below (also shipped as {@code scd2_view.sql} next to
 * the data):
 *
 * <pre>{@code
 * -- latest belief per (business key, _valid_from), then LEAD() for _valid_to
 * CREATE VIEW <table>_scd2 AS
 * WITH latest AS (
 *   SELECT *, ROW_NUMBER() OVER (PARTITION BY <bk>, _valid_from ORDER BY _tx_time DESC) rn
 *   FROM <source>
 * )
 * SELECT * EXCLUDE (rn),
 *        COALESCE(_valid_to, LEAD(_valid_from) OVER (PARTITION BY <bk> ORDER BY _valid_from)) AS _valid_to_derived,
 *        (_op <> 'DELETE' AND _valid_to IS NULL) AS _is_current
 * FROM latest WHERE rn = 1;
 * }</pre>
 */
public final class PublishedContract {
    private PublishedContract() {}

    public static final String VALID_FROM = "_valid_from";
    public static final String VALID_TO = "_valid_to";
    public static final String TX_TIME = "_tx_time";
    public static final String TXN_ID = "_txn_id";
    public static final String OP = "_op";
    public static final String SCHEMA_VERSION = "_schema_version";

    public static final String LOG_DIR = "_chronodim_log";
    public static final String DATA_DIR = "data";
    public static final String FINALIZED_DIR = "finalized";

    /**
     * Published column shape. CHRONODIM = full bitemporal log. DATABRICKS = the
     * {@code __START_AT}/{@code __END_AT} convention: END null = active row, no
     * operation column, delete tombstones not published (a deleted entity is a
     * closed final version with no successor). {@code _tx_time}/{@code _txn_id}
     * remain in both shapes — the commit log and belief-dedup need them.
     */
    public record Style(String startCol, String endCol, boolean includeOps) {
        public static final Style CHRONODIM = new Style(VALID_FROM, VALID_TO, true);
        public static final Style DATABRICKS = new Style("__START_AT", "__END_AT", false);

        public static Style of(io.chronodim.api.TableConfig.PublishColumnStyle s) {
            return s == io.chronodim.api.TableConfig.PublishColumnStyle.DATABRICKS ? DATABRICKS : CHRONODIM;
        }

        /** Preset + per-table overrides — any team's column convention. */
        public static Style forConfig(io.chronodim.api.TableConfig.PublishConfig p) {
            return new Style(p.effectiveStartColumn(), p.effectiveEndColumn(), p.effectiveIncludeOps());
        }

        public String name() {
            if (this.equals(DATABRICKS)) return "DATABRICKS";
            if (this.equals(CHRONODIM)) return "CHRONODIM";
            return "CUSTOM";
        }
    }

    /**
     * Hive-style partition path for a rendered row, e.g. {@code region=EU/year=2026}.
     * Partition column values stay in the row payload as well — Spark merges
     * directory-derived partition columns with the data schema (data wins), and
     * plain glob readers keep working.
     */
    public static String partitionPath(java.util.List<String> partitionBy, java.util.Map<String, Object> renderedRow) {
        if (partitionBy.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String col : partitionBy) {
            if (sb.length() > 0) sb.append('/');
            Object v = renderedRow.get(col);
            sb.append(col).append('=').append(v == null ? "__HIVE_DEFAULT_PARTITION__" : encodePartitionValue(String.valueOf(v)));
        }
        return sb.toString();
    }

    /** Hive-compatible percent-encoding of partition values for safe directory names. */
    public static String encodePartitionValue(String v) {
        StringBuilder sb = new StringBuilder(v.length());
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_' || c == ':' || c == '+') {
                sb.append(c);
            } else {
                for (byte b : String.valueOf(c).getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
                    sb.append('%').append(String.format("%02X", b));
                }
            }
        }
        return sb.toString();
    }

    /** SQL view template with placeholders {table}, {source}, {bk_cols}. */
    public static String viewTemplate(String table, String source, java.util.List<String> bkCols) {
        return viewTemplate(table, source, bkCols, Style.CHRONODIM);
    }

    public static String viewTemplate(String table, String source, java.util.List<String> bkCols, Style style) {
        String bk = String.join(", ", bkCols);
        String isCurrent = style.includeOps()
                ? "(_op <> 'DELETE' AND " + style.endCol() + " IS NULL)"
                : "(" + style.endCol() + " IS NULL)";
        return """
                -- ChronoDim SCD2 view over the append-only published log (R-PUB-3).
                -- Works in DuckDB, Databricks, Spark SQL and Trino.
                CREATE OR REPLACE VIEW %s_scd2 AS
                WITH latest AS (
                  SELECT s.*,
                         ROW_NUMBER() OVER (PARTITION BY %s, %s ORDER BY _tx_time DESC) AS _rn
                  FROM %s AS s
                )
                SELECT * EXCEPT (_rn, %s),
                       COALESCE(%s,
                                LEAD(%s) OVER (PARTITION BY %s ORDER BY %s)) AS %s,
                       %s AS _is_current
                FROM latest
                WHERE _rn = 1;
                """.formatted(table, bk, style.startCol(), source,
                style.endCol(), style.endCol(), style.startCol(), bk, style.startCol(), style.endCol(), isCurrent);
    }
}
