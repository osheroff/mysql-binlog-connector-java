package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.event.MariadbGtidListEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Mariadb GTID_LIST_EVENT Fields
 * <pre>
 *  uint<4> Number of GTIDs
 *  GTID[0]
 *      uint<4> Replication Domain ID
 *      uint<4> Server_ID
 *      uint<8> GTID sequence ...
 * GTID[n]
 * </pre>
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid_event/">GTID_EVENT</a> for the original doc
 */
public class MariadbGtidListEventDataDeserializer implements EventDataDeserializer<MariadbGtidListEventData> {
    @Override
    public MariadbGtidListEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        MariadbGtidListEventData eventData = new MariadbGtidListEventData();
        long gtidLength = inputStream.readInteger(4);
        MariadbGtidSet mariaGTIDSet = new MariadbGtidSet();
        for (int i = 0; i < gtidLength; i++) {
            long domainId = inputStream.readInteger(4);
            long serverId = inputStream.readInteger(4);
            long sequence = inputStream.readLong(8);
            mariaGTIDSet.add(new MariadbGtidSet.MariaGtid(domainId, serverId, sequence));
        }
        eventData.setMariaGTIDSet(mariaGTIDSet);
        return eventData;
    }
}
