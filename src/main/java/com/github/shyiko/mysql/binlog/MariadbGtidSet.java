package com.github.shyiko.mysql.binlog;

import java.util.*;

/**
 * Mariadb Global Transaction ID
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid/">GTID</a> for the original doc
 */
public class MariadbGtidSet extends GtidSet {

    private Map<Long, MariaGtid> map = new HashMap<>();

    public MariadbGtidSet() {
        super(null); //
    }

    public MariadbGtidSet(String gtidSet) {
        super(null);
        if (gtidSet != null && gtidSet.length() > 0) {
            String[] gtids = gtidSet.replaceAll("\n", "").split(",");
            for (String gtid : gtids) {
                MariaGtid mariaGtid = MariaGtid.parse(gtid);
                map.put(mariaGtid.getDomainId(), mariaGtid);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MariaGtid gtid : map.values()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(gtid.toString());
        }
        return sb.toString();
    }

    @Override
    public Collection<UUIDSet> getUUIDSets() {
        throw new UnsupportedOperationException("Mariadb gtid not support this method");
    }

    @Override
    public UUIDSet getUUIDSet(String uuid) {
        throw new UnsupportedOperationException("Mariadb gtid not support this method");
    }

    @Override
    public UUIDSet putUUIDSet(UUIDSet uuidSet) {
        throw new UnsupportedOperationException("Mariadb gtid not support this method");
    }

    @Override
    public boolean add(String gtid) {
        MariaGtid mariaGtid = MariaGtid.parse(gtid);
        map.put(mariaGtid.getDomainId(), mariaGtid);
        return true;
    }

    public void add(MariaGtid gtid) {
        map.put(gtid.getDomainId(), gtid);
    }

    @Override
    public boolean isContainedWithin(GtidSet other) {
        throw new UnsupportedOperationException("Mariadb gtid not support this method");
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof MariadbGtidSet) {
            MariadbGtidSet that = (MariadbGtidSet) obj;
            return this.map.equals(that.map);
        }
        return false;
    }

    public static class MariaGtid {

        // {domainId}-{serverId}-{sequence}
        private long domainId;
        private long serverId;
        private long sequence;

        public MariaGtid(long domainId, long serverId, long sequence) {
            this.domainId = domainId;
            this.serverId = serverId;
            this.sequence = sequence;
        }

        public MariaGtid(String gtid) {
            String[] gtidArr = gtid.split("-");
            this.domainId = Long.parseLong(gtidArr[0]);
            this.serverId = Long.parseLong(gtidArr[1]);
            this.sequence = Long.parseLong(gtidArr[2]);
        }

        public static MariaGtid parse(String gtid) {
            return new MariaGtid(gtid);
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

        public long getSequence() {
            return sequence;
        }

        public void setSequence(long sequence) {
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MariaGtid mariaGtid = (MariaGtid) o;
            return domainId == mariaGtid.domainId &&
                serverId == mariaGtid.serverId &&
                sequence == mariaGtid.sequence;
        }

        @Override
        public String toString() {
            return String.format("%s-%s-%s", domainId, serverId, sequence);
        }
    }
}

