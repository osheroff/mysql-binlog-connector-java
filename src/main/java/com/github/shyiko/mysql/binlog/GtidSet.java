/*
 * Copyright 2015 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.MySqlGtid;

import java.util.*;
import java.util.regex.Pattern;

/**
 * GTID set as described in <a href="https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html">GTID
 * Concepts</a> of MySQL 8.4 Reference Manual.
 *
 * <pre>
 * gtid_set: uuid_set[,uuid_set]...
 * uuid_set: uuid:[tag:]interval[:[tag:]interval]...
 * uuid: hhhhhhhh-hhhh-hhhh-hhhh-hhhhhhhhhhhh, h: [0-9|A-F]
 * tag: [a-zA-Z_][a-zA-Z0-9_]{0,31}
 * interval: n[-n], (n &gt;= 1)
 * </pre>
 *
 * MySQL 8.4 (WL#15294) allows a GTID to carry a user-defined tag, in which case the tag is treated as part of the
 * identity of the transaction group. Tagged and untagged transactions from the same server are therefore tracked as
 * separate {@link UUIDSet}s, keyed by {@code uuid[:tag]}.
 *
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class GtidSet {

    /**
     * Tag grammar as defined by MySQL 8.4: 1-32 characters, leading letter or underscore.
     */
    private static final Pattern TAG_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{0,31}");

    /**
     * Keyed by {@code uuid} for untagged sets and {@code uuid:tag} for tagged ones, preserving encounter order.
     */
    private final Map<String, UUIDSet> map = new LinkedHashMap<String, UUIDSet>();

    private static String key(UUID uuid, String tag) {
        return tag == null || tag.isEmpty() ? uuid.toString() : uuid + ":" + tag;
    }

    public static GtidSet parse(String gtidStr) {
        if ( MariadbGtidSet.isMariaGtidSet(gtidStr) ) {
            return new MariadbGtidSet(gtidStr);
        } else {
            return new GtidSet(gtidStr);
        }
    }
    /**
     * @param gtidSet gtid set comprised of closed intervals (like MySQL's executed_gtid_set).
     */
    public GtidSet(String gtidSet) {
        String[] uuidSets = (gtidSet == null || gtidSet.isEmpty()) ? new String[0] :
            gtidSet.replace("\n", "").split(",");
        // intervals are collected per uuid[:tag] before being turned into UUIDSets, because a single uuid_set may
        // carry several tags (e.g. "<uuid>:tag_a:1-3:tag_b:8-52") and each of them is a distinct transaction group.
        Map<String, List<Interval>> intervalsByKey = new LinkedHashMap<String, List<Interval>>();
        Map<String, UUID> uuidByKey = new LinkedHashMap<String, UUID>();
        Map<String, String> tagByKey = new LinkedHashMap<String, String>();
        for (String uuidSet : uuidSets) {
            int uuidSeparatorIndex = uuidSet.indexOf(":");
            UUID sourceId = UUID.fromString(uuidSet.substring(0, uuidSeparatorIndex));
            String tag = "";
            String key = key(sourceId, tag);
            String[] rawIntervals = uuidSet.substring(uuidSeparatorIndex + 1).split(":");
            for (String interval : rawIntervals) {
                if (TAG_PATTERN.matcher(interval).matches()) {
                    // a tag applies to every interval that follows it, until the next tag
                    tag = interval;
                    key = key(sourceId, tag);
                    continue;
                }
                String[] is = interval.split("-");
                long[] split = new long[is.length];
                for (int i = 0, e = is.length; i < e; i++) {
                    split[i] = Long.parseLong(is[i]);
                }
                if (split.length == 1) {
                    split = new long[] {split[0], split[0]};
                }
                List<Interval> intervals = intervalsByKey.get(key);
                if (intervals == null) {
                    intervalsByKey.put(key, intervals = new ArrayList<Interval>());
                    uuidByKey.put(key, sourceId);
                    tagByKey.put(key, tag);
                }
                intervals.add(new Interval(split[0], split[1]));
            }
        }
        for (Map.Entry<String, List<Interval>> e : intervalsByKey.entrySet()) {
            String key = e.getKey();
            map.put(key, new UUIDSet(uuidByKey.get(key), tagByKey.get(key), e.getValue()));
        }
    }

    /**
     * Get an immutable collection of the {@link UUIDSet range of GTIDs for a single server}.
     * @return the {@link UUIDSet GTID ranges for each server}; never null
     */
    public Collection<UUIDSet> getUUIDSets() {
        return Collections.unmodifiableCollection(map.values());
    }

    /**
     * Find the {@link UUIDSet} for the server with the specified UUID.
     * @param uuid the UUID of the server
     * @return the {@link UUIDSet} for the identified server, or {@code null} if there are no GTIDs from that server.
     */
    public UUIDSet getUUIDSet(String uuid) {
        int tagSeparatorIndex = uuid.indexOf(":");
        if (tagSeparatorIndex == -1) {
            return map.get(UUID.fromString(uuid).toString());
        }
        UUID sourceId = UUID.fromString(uuid.substring(0, tagSeparatorIndex));
        return map.get(key(sourceId, uuid.substring(tagSeparatorIndex + 1)));
    }

    /**
     * Add or replace the UUIDSet
     * @param uuidSet UUIDSet to be added
     * @return the old {@link UUIDSet} for the server given in uuidSet param,
     *         or {@code null} if there are no UUIDSet for the given server.
     */
    public UUIDSet putUUIDSet(UUIDSet uuidSet) {
        return map.put(key(uuidSet.getServerId(), uuidSet.getTag()), uuidSet);
    }

    /**
     * @param gtid GTID ("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already there)
     */
    public boolean add(String gtid) {
        return add(MySqlGtid.fromString(gtid));
    }

    public void addGtid(Object gtid) {
        if (gtid instanceof MySqlGtid) {
            add((MySqlGtid) gtid);
        } else if (gtid instanceof String) {
            add((String) gtid);
        } else {
            throw new IllegalArgumentException(gtid + " not supported");
        }
    }

    private boolean add(MySqlGtid mySqlGtid) {
        String key = key(mySqlGtid.getServerId(), "");
        UUIDSet uuidSet = map.get(key);
        if (uuidSet == null) {
            map.put(key, uuidSet = new UUIDSet(mySqlGtid.getServerId(), new ArrayList<Interval>()));
        }
        return uuidSet.add(mySqlGtid.getTransactionId());
    }

    /**
     * Determine if the GTIDs represented by this object are contained completely within the supplied set of GTIDs.
     * Note that if two {@link GtidSet}s are equal, then they both are subsets of the other.
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all of the GTIDs in this set are equal to or completely contained within the supplied
     * set of GTIDs, or {@code false} otherwise
     */
    public boolean isContainedWithin(GtidSet other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (this.equals(other)) {
            return true;
        }
        for (UUIDSet uuidSet : map.values()) {
            UUIDSet thatSet = other.map.get(key(uuidSet.getServerId(), uuidSet.getTag()));
            if (!uuidSet.isContainedWithin(thatSet)) {
                return false;
            }
        }
        return true;
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
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.map.equals(that.map);
        }
        return false;
    }

    @Override
    public String toString() {
        // MySQL renders every transaction group of a server inside a single uuid_set, e.g.
        // "<uuid>:1-5:tag_a:1-3:tag_b:8-52", so tagged sets are folded back under their server's uuid.
        Map<UUID, StringBuilder> byServerId = new LinkedHashMap<UUID, StringBuilder>();
        for (UUIDSet uuidSet : map.values()) {
            StringBuilder sb = byServerId.get(uuidSet.getServerId());
            if (sb == null) {
                byServerId.put(uuidSet.getServerId(), sb = new StringBuilder(uuidSet.getServerId().toString()));
            }
            if (!uuidSet.getTag().isEmpty()) {
                sb.append(':').append(uuidSet.getTag());
            }
            if (uuidSet.intervals.isEmpty()) {
                sb.append(':');
            } else {
                sb.append(':').append(join(uuidSet.intervals, ":"));
            }
        }
        return join(byServerId.values(), ",");
    }

    public String toSeenString() {
        return this.toString();
    }

    private static String join(Collection<?> o, String delimiter) {
        if (o.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Object o1 : o) {
            sb.append(o1).append(delimiter);
        }
        return sb.substring(0, sb.length() - delimiter.length());
    }

    /**
     * A range of GTIDs for a single server with a specific UUID.
     * @see GtidSet
     */
    public static final class UUIDSet {

        private final UUID uuid;
        private final String tag;
        private final List<Interval> intervals;

        public UUIDSet(String uuid, List<Interval> intervals) {
            this(UUID.fromString(uuid), "", intervals);
        }

        public UUIDSet(UUID uuid, List<Interval> intervals) {
            this(uuid, "", intervals);
        }

        /**
         * @param uuid the UUID of the server that generated the GTIDs
         * @param tag the MySQL 8.4 GTID tag, or an empty string for untagged transactions; never null
         * @param intervals the transaction intervals
         */
        public UUIDSet(UUID uuid, String tag, List<Interval> intervals) {
            this.uuid = uuid;
            this.tag = tag == null ? "" : tag;
            this.intervals = intervals;
            if (intervals.size() > 1) {
                joinAdjacentIntervals(0);
            }
        }

        private boolean add(long transactionId) {
            int index = findInterval(transactionId);
            boolean addedToExisting = false;
            if (index < intervals.size()) {
                Interval interval = intervals.get(index);
                if (interval.start == transactionId + 1) {
                    interval.start = transactionId;
                    addedToExisting = true;
                } else
                if (interval.end + 1 == transactionId) {
                    interval.end = transactionId;
                    addedToExisting = true;
                } else
                if (interval.start <= transactionId && transactionId <= interval.end) {
                    return false;
                }
            }
            if (!addedToExisting) {
                intervals.add(index, new Interval(transactionId, transactionId));
            }
            if (intervals.size() > 1) {
                joinAdjacentIntervals(index);
            }
            return true;
        }

        /**
         * Collapses intervals like a-(b-1):b-c into a-c (only in index+-1 range).
         */
        private void joinAdjacentIntervals(int index) {
            for (int i = Math.min(index + 1, intervals.size() - 1), e = Math.max(index - 1, 0); i > e; i--) {
                Interval a = intervals.get(i - 1), b = intervals.get(i);
                if (a.end + 1 == b.start) {
                    a.end = b.end;
                    intervals.remove(i);
                }
            }
        }

        /**
         * @return index which is either a pointer to the interval containing v or a position at which v can be added
         */
        private int findInterval(long v) {
            int l = 0, p = 0, r = intervals.size();
            while (l < r) {
                p = (l + r) / 2;
                Interval i = intervals.get(p);
                if (i.end < v) {
                    l = p + 1;
                } else
                if (v < i.start) {
                    r = p;
                } else {
                    return p;
                }
            }
            if (!intervals.isEmpty() && intervals.get(p).end < v) {
                p++;
            }
            return p;
        }

        /**
         * Get the UUID for the server that generated the GTIDs.
         * @return the server's UUID; never null
         */
        @Deprecated
        public String getUUID() {
            return uuid.toString();
        }

        public UUID getServerId() {
            return uuid;
        }

        /**
         * Get the MySQL 8.4 GTID tag identifying this group of transactions.
         * @return the tag, or an empty string when the transactions are untagged; never null
         */
        public String getTag() {
            return tag;
        }

        /**
         * @return whether this set holds tagged (MySQL 8.4 {@code uuid:tag:interval}) transactions
         */
        public boolean isTagged() {
            return !tag.isEmpty();
        }

        /**
         * Get the intervals of transaction numbers.
         * @return the immutable transaction intervals; never null
         */
        public List<Interval> getIntervals() {
            return Collections.unmodifiableList(intervals);
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction
         * numbers from the set of transaction numbers in the supplied set.
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are equal to or a subset of the transaction
         * numbers of the supplied set, or false otherwise
         */
        public boolean isContainedWithin(UUIDSet other) {
            if (other == null) {
                return false;
            }
            if (!this.uuid.equals(other.uuid) || !this.tag.equals(other.tag)) {
                // not even the same server / transaction group ...
                return false;
            }
            if (this.intervals.isEmpty()) {
                return true;
            }
            if (other.intervals.isEmpty()) {
                return false;
            }
            // every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isContainedWithin(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false; // didn't find a match
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return 31 * uuid.hashCode() + tag.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.uuid.equals(that.uuid) &&
                    this.tag.equals(that.tag) &&
                    this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(uuid).append(':');
            if (!tag.isEmpty()) {
                sb.append(tag).append(':');
            }
            Iterator<Interval> iter = intervals.iterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                sb.append(iter.next());
            }
            return sb.toString();
        }
    }

    /**
     * An interval of contiguous transaction identifiers.
     * @see GtidSet
     */
    public static final class Interval implements Comparable<Interval> {

        private long start;
        private long end;

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         * {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied
         * interval's {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isContainedWithin(Interval other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Interval) {
                Interval that = (Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }

        @Override
        public int compareTo(Interval o) {
            return saturatedCast(this.start - o.start);
        }

        private static int saturatedCast(long value) {
            if (value > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (value < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
            return (int) value;
        }
    }

}
