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
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer.UnixTime;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class AbstractRowsEventDataDeserializerTest {

    @Test
    public void testFrom() throws Exception {
        assetTimeEquals(UnixTime.from(2260, 8, 19, 0, 0, 0, 0),
            timestamp(2260, 8, 19, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(2016, 8, 19, 0, 0, 0, 0),
            timestamp(2016, 8, 19, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1970, 1, 1, 0, 0, 0, 0), 0);
        assetTimeEquals(UnixTime.from(1969, 1, 1, 0, 0, 0, 0),
            -365L * 24 * 60 * 60 * 1000);
        assetTimeEquals(UnixTime.from(1582, 10, 15, 0, 0, 0, 0),
            timestamp(1582, 10, 15, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1582, 10, 14, 0, 0, 0, 0),
            timestamp(1582, 10, 14, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1, 1, 1, 0, 0, 0, 0),
            timestamp(1, 1, 1, 0, 0, 0, 0));
    }

    /**
     * MySQL TIME is a signed duration, and the leading bit of the packed TIME2 payload carries that
     * sign. The payloads below were taken from a binlog written by MySQL 8.0.46 for the values named,
     * covering both signs and all three fractional-second widths.
     */
    @Test
    public void testDeserializeTimeV2Sign() throws Exception {
        assertEquals(deserializeTimeV2(0, 0x81, 0x02, 0x04), 58084000L);            // 16:08:04
        assertEquals(deserializeTimeV2(0, 0x86, 0x42, 0x04), 360484000L);           // 100:08:04
        assertEquals(deserializeTimeV2(0, 0x80, 0x00, 0x00), 0L);                   // 00:00:00
        assertEquals(deserializeTimeV2(0, 0x7f, 0xff, 0xff), -1000L);               // -00:00:01
        assertEquals(deserializeTimeV2(0, 0x7e, 0xfd, 0xfc), -58084000L);           // -16:08:04
    }

    @Test
    public void testDeserializeTimeV2SignWithFractionalSeconds() throws Exception {
        assertEquals(deserializeTimeV2(3, 0x81, 0x02, 0x04, 0x13, 0x88), 58084500L);        // 16:08:04.500
        assertEquals(deserializeTimeV2(6, 0x81, 0x02, 0x04, 0x07, 0xa1, 0x20), 58084500L);  // 16:08:04.500000
        assertEquals(deserializeTimeV2(3, 0x7e, 0xfd, 0xfb, 0xf6, 0x3c), -58084250L);       // -16:08:04.250
        assertEquals(deserializeTimeV2(6, 0x7e, 0xfd, 0xfb, 0xfc, 0x2f, 0x70), -58084250L); // -16:08:04.250000
    }

    private long deserializeTimeV2(int meta, int... payload) throws IOException {
        byte[] bytes = new byte[payload.length];
        for (int i = 0; i < payload.length; i++) {
            bytes[i] = (byte) payload[i];
        }
        AbstractRowsEventDataDeserializer<EventData> deserializer =
            new AbstractRowsEventDataDeserializer<EventData>(new HashMap<Long, TableMapEventData>()) {

                @Override
                public EventData deserialize(ByteArrayInputStream inputStream) {
                    throw new UnsupportedOperationException();
                }
            };
        deserializer.setDeserializeDateAndTimeAsLong(true);
        return (Long) deserializer.deserializeTimeV2(meta, new ByteArrayInputStream(bytes));
    }

    private void assetTimeEquals(long actual, long expected) {
        assertEquals(actual, expected, actual + " != " + expected +
            ", discrepancy: " + (actual - expected));
    }

    // checkstyle, please ignore ParameterNumber for the next line
    private long timestamp(int year, int month, int dayOfMonth, int hourOfDay,
            int minute, int second, int millis) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, millis);
        return c.getTimeInMillis();
    }

}
