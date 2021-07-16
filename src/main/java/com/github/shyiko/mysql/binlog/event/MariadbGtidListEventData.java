package com.github.shyiko.mysql.binlog.event;

import com.github.shyiko.mysql.binlog.MariadbGtidSet;

/**
 * Logged in every binlog to record the current replication state
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid_list_event/">GTID_LIST_EVENT</a> for the original doc
 */
public class MariadbGtidListEventData implements EventData {

    private MariadbGtidSet mariaGTIDSet;

    public MariadbGtidSet getMariaGTIDSet() {
        return mariaGTIDSet;
    }

    public void setMariaGTIDSet(MariadbGtidSet mariaGTIDSet) {
        this.mariaGTIDSet = mariaGTIDSet;
    }

    @Override
    public String toString() {
        return "MariadbGtidListEventData{" +
            "mariaGTIDSet=" + mariaGTIDSet +
            '}';
    }
}
