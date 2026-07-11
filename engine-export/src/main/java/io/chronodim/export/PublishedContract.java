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

    /** SQL view template with placeholders {table}, {source}, {bk_cols}. */
    public static String viewTemplate(String table, String source, java.util.List<String> bkCols) {
        String bk = String.join(", ", bkCols);
        return """
                -- ChronoDim SCD2 view over the append-only published log (R-PUB-3).
                -- Works in DuckDB, Databricks, Spark SQL and Trino.
                CREATE OR REPLACE VIEW %s_scd2 AS
                WITH latest AS (
                  SELECT s.*,
                         ROW_NUMBER() OVER (PARTITION BY %s, _valid_from ORDER BY _tx_time DESC) AS _rn
                  FROM %s AS s
                )
                SELECT * EXCEPT (_rn, _valid_to),
                       COALESCE(_valid_to,
                                LEAD(_valid_from) OVER (PARTITION BY %s ORDER BY _valid_from)) AS _valid_to,
                       (_op <> 'DELETE' AND _valid_to IS NULL) AS _is_current
                FROM latest
                WHERE _rn = 1;
                """.formatted(table, bk, source, bk);
    }
}
