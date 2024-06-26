/*
 * Copyright 2013 Stanley Shyiko
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

import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogFileReaderIntegrationTest {

    @Test
    public void testNextEvent() throws Exception {
        BinaryLogFileReader reader = new BinaryLogFileReader(new GZIPInputStream(
                new FileInputStream("src/test/resources/mysql-bin.sakila.gz")));
        readAll(reader, 1462);
    }

    @Test
    public void testNextEventCompressed() throws Exception {
        BinaryLogFileReader reader = new BinaryLogFileReader(
            new FileInputStream("src/test/resources/mysql-bin.compressed"));
        readAll(reader, 5);
    }

    @Test
    public void testChecksumNONE() throws Exception {
        EventDeserializer eventDeserializer = new EventDeserializer();
        BinaryLogFileReader reader = new BinaryLogFileReader(
                new FileInputStream("src/test/resources/mysql-bin.checksum-none"), eventDeserializer);
        readAll(reader, 191);
    }

    @Test
    public void testChecksumCRC32() throws Exception {
        EventDeserializer eventDeserializer = new EventDeserializer();
        BinaryLogFileReader reader = new BinaryLogFileReader(
                new FileInputStream("src/test/resources/mysql-bin.checksum-crc32"), eventDeserializer);
        readAll(reader, 303);
    }

    @Test
    public void testChecksumCRC32WithCustomEventDataDeserializer() throws Exception {
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setEventDataDeserializer(EventType.FORMAT_DESCRIPTION, new NullEventDataDeserializer());
        BinaryLogFileReader reader = new BinaryLogFileReader(
                new FileInputStream("src/test/resources/mysql-bin.checksum-crc32"), eventDeserializer);
        readAll(reader, 303);
    }

    @Test
    public void testUnsupportedEventType() throws Exception {
        EventDeserializer eventDeserializer = new EventDeserializer();

        // mysql> SHOW BINLOG EVENTS IN 'mysql-bin.aurora-padding';
        // +--------------------------+------+----------------+-------------+---------------------------------------+
        // | Log_name                 | Pos  | Event_type     | End_log_pos | Info                                  |
        // +--------------------------+------+----------------+-------------+---------------------------------------+
        // | mysql-bin.aurora-padding |    4 | Format_desc    |         185 | Server ver: 5.7.12-log, Binlog ver: 4 |
        // | mysql-bin.aurora-padding |  185 | Previous_gtids |         216 |                                       |
        // | mysql-bin.aurora-padding |  216 | Anonymous_Gtid |         281 | SET @@SESSION.GTID_NEXT= 'ANONYMOUS'  |
        // | mysql-bin.aurora-padding |  281 | Aurora_padding |        1209 | Ignorable                             |
        // | mysql-bin.aurora-padding | 1209 | Query          |        1294 | BEGIN                                 |
        BinaryLogFileReader reader = new BinaryLogFileReader(
                new FileInputStream("src/test/resources/mysql-bin.aurora-padding"), eventDeserializer);
        try {
            for (int i = 0; i < 3; i++) {
                assertNotNull(reader.readEvent());
            }
            try {
                reader.readEvent();
            } catch (IOException e) {
                // this simulates the Debezium's event.processing.failure.handling.mode = warn
            }
            assertEquals(reader.readEvent().getHeader().getEventType(), EventType.QUERY);
        } finally {
            reader.close();
        }
    }

    private void readAll(BinaryLogFileReader reader, int expect) throws IOException {
        try {
            int numberOfEvents = 0;
            while ((reader.readEvent()) != null) {
                numberOfEvents++;
            }
            assertEquals(numberOfEvents, expect);
        } finally {
            reader.close();
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Not a valid binary log")
    public void testMagicHeaderCheck() throws Exception {
        new BinaryLogFileReader(new File("src/test/resources/mysql-bin.sakila.gz"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullEventDeserializerIsNotAllowed() throws Exception {
        new BinaryLogFileReader(new File("src/test/resources/mysql-bin.sakila.gz"), null);
    }

    @Test
    public void testDeserializationSuppressionByEventType() throws Exception {
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setEventDataDeserializer(EventType.XID, new NullEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
        BinaryLogFileReader reader = new BinaryLogFileReader(new GZIPInputStream(
            new FileInputStream("src/test/resources/mysql-bin.sakila.gz")), eventDeserializer);
        try {
            boolean n = true, b = true;
            for (Event event; (event = reader.readEvent()) != null && (n || b); ) {
                EventType eventType = event.getHeader().getEventType();
                if (eventType == EventType.XID) {
                    assertNull(event.getData());
                    n = false;
                } else
                if (eventType == EventType.QUERY) {
                    assertTrue(event.getData() instanceof ByteArrayEventData);
                    assertTrue(event.getData().toString().length() < 80); // making sure byte array isn't shown
                    b = false;
                }
            }
            assertFalse(n || b);
        } finally {
            reader.close();
        }
    }

}
