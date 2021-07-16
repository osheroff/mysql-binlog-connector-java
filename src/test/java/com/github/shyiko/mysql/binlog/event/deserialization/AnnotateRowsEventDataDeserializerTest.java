package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class AnnotateRowsEventDataDeserializerTest {

    private static final byte[] DATA = {73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 102, 111, 111, 32, 115, 101, 116, 32, 105, 32, 61, 32, 50};

    private static final String sql = "INSERT INTO foo set i = 2";

    @Test
    public void deserialize() throws IOException {
        AnnotateRowsEventDataDeserializer deserializer = new AnnotateRowsEventDataDeserializer();
        AnnotateRowsEventData eventData = deserializer.deserialize(new ByteArrayInputStream(DATA));

        assertEquals(sql, eventData.getRowsQuery());
    }
}
