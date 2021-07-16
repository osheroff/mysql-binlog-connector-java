package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.MariadbGtidListEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidListEventDataDeserializerTest {

    private static final byte[] DATA = {1, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 87, 28, 0, 0, 0, 0, 0, 0, 77};

    private static final String GTID_SET_LIST = "MariadbGtidListEventData{mariaGTIDSet=0-102-7255}";

    @Test
    public void deserialize() throws IOException {
        MariadbGtidListEventDataDeserializer deserializer = new MariadbGtidListEventDataDeserializer();
        MariadbGtidListEventData eventData = deserializer.deserialize(new ByteArrayInputStream(DATA));
        assertEquals(GTID_SET_LIST, eventData.toString());
    }
}
