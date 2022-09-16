package com.github.shyiko.mysql.binlog;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Mariadb Global Transaction ID
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid/">GTID</a> for the original doc
 */
public class MariadbGtidSet extends GtidSet {
    /*
        we keep two maps; one of them contains the current GTID position for
        each domain.  The other contains all the "seen" GTID positions for each
        domain and can be used to compare against another gtid postion.
     */
    protected Map<Long, MariaGtid> positionMap = new HashMap<>();

    protected Map<Long, LinkedHashMap<Long, MariaGtid>> seenMap = new LinkedHashMap<>();

    public MariadbGtidSet() {
        super(null); //
    }

    /**
     * Initialize a new MariaDB gtid set from a string, like:
     * 0-1-24,0-555555-9709
     * DOMAIN_ID-SERVER_ID-SEQUENCE[,DOMAIN_ID-SERVER_ID-SEQUENCE]
     *
     * note that for duplicate domain ids it's "last one wins" for the current position
     * @param gtidSet a string representing the gtid set.
     */
    public MariadbGtidSet(String gtidSet) {
        super(null);
        if (gtidSet != null && gtidSet.length() > 0) {
            String[] gtids = gtidSet.replaceAll("\n", "").split(",");
            for (String gtid : gtids) {
                MariaGtid mariaGtid = MariaGtid.parse(gtid);

                positionMap.put(mariaGtid.getDomainId(), mariaGtid);
                addToSeenSet(mariaGtid);
            }
        }
    }

    static Pattern MARIA_GTID_PATTERN = Pattern.compile("^\\d+-\\d+-\\d+(?!.*:)");

    public static boolean isMariaGtidSet(String gtidSet) {
        return MARIA_GTID_PATTERN.matcher(gtidSet).find();
    }

    private void addToSeenSet(MariaGtid gtid) {
        if ( !this.seenMap.containsKey(gtid.domainId) ) {
            this.seenMap.put(gtid.domainId, new LinkedHashMap<>());
        }

        LinkedHashMap<Long, MariaGtid> domainMap = this.seenMap.get(gtid.domainId);
        domainMap.put(gtid.serverId, gtid);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MariaGtid gtid : positionMap.values()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(gtid.toString());
        }
        return sb.toString();
    }

    @Override
    public String toSeenString() {
        StringBuilder sb = new StringBuilder();
        for (Long domainID : seenMap.keySet()) {
            for( MariaGtid gtid: seenMap.get(domainID).values() ) {
                if (sb.length() > 0) {
                    sb.append(",");
                }

                sb.append(gtid.toString());
            }
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
        add(mariaGtid);
        return true;
    }

    public void add(MariaGtid gtid) {
        positionMap.put(gtid.getDomainId(), gtid);
        addToSeenSet(gtid);
    }

    /*
        we're trying to ask "is this position behind the other position?"
        - if we have a domain that the other doesn't, we're probably "ahead".
        - the inverse is true too
     */
    @Override
    public boolean isContainedWithin(GtidSet other) {
        if (!(other instanceof MariadbGtidSet))
            return false;

        MariadbGtidSet o = (MariadbGtidSet) other;

        for ( Long domainID : this.seenMap.keySet() ) {
            if ( !o.seenMap.containsKey(domainID) ) {
                return false;
            }

            LinkedHashMap<Long, MariaGtid> thisDomainMap = this.seenMap.get(domainID);
            LinkedHashMap<Long, MariaGtid> otherDomainMap = o.seenMap.get(domainID);

            for ( Long serverID : thisDomainMap.keySet() ) {
                if ( !otherDomainMap.containsKey(serverID)) {
                    return false;
                }

                MariaGtid thisGtid = thisDomainMap.get(serverID);
                MariaGtid otherGtid = otherDomainMap.get(serverID);
                if ( thisGtid.sequence >= otherGtid.sequence )
                    return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.seenMap.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof MariadbGtidSet) {
            MariadbGtidSet that = (MariadbGtidSet) obj;
            return this.positionMap.equals(that.positionMap);
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

