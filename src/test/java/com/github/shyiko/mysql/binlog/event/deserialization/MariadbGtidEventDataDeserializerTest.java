package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidEventDataDeserializerTest {

    private static final byte[] DATA = {-20, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 121};

    private static final String GTID_SET = "0-0-7404";

    @Test
    public void deserialize() throws IOException {
        MariadbGtidEventDataDeserializer deserializer = new MariadbGtidEventDataDeserializer();
        MariadbGtidEventData eventData = deserializer.deserialize(new ByteArrayInputStream(DATA));
        assertEquals(GTID_SET, eventData.toString());
    }
}
