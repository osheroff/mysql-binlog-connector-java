package com.github.shyiko.mysql.binlog.event;

/**
 * Mariadb ANNOTATE_ROWS_EVENT events accompany row events and describe the query which caused the row event
 * Enable this with --binlog-annotate-row-events (default on from MariaDB 10.2.4).
 * In the binary log, each Annotate_rows event precedes the corresponding Table map event.
 * Note the master server sends ANNOTATE_ROWS_EVENT events only if the Slave server connects
 * with the BINLOG_SEND_ANNOTATE_ROWS_EVENT flag (value is 2) in the COM_BINLOG_DUMP Slave Registration phase.
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/annotate_rows_event/">ANNOTATE_ROWS_EVENT</a> for the original doc
 */
public class AnnotateRowsEventData implements EventData {

    private String rowsQuery;

    public String getRowsQuery() {
        return rowsQuery;
    }

    public void setRowsQuery(String rowsQuery) {
        this.rowsQuery = rowsQuery;
    }

    @Override
    public String toString() {
        return "AnnotateRowsEventData{" +
            "rowsQuery='" + rowsQuery + '\'' +
            '}';
    }
}
