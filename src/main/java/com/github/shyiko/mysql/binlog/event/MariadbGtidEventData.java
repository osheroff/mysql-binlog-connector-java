package com.github.shyiko.mysql.binlog.event;

/**
 * MariaDB and MySQL have different GTID implementations, and that these are not compatible with each other.
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid_event/">GTID_EVENT</a> for the original doc
 */
public class MariadbGtidEventData implements EventData {

    private long sequence;
    private long domainId;
    private long serverId;

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public long getDomainId() {
        return domainId;
    }

    public void setDomainId(long domainId) {
        this.domainId = domainId;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    @Override
    public String toString() {
        return domainId + "-" + serverId + "-" + sequence;
    }
}
