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

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 */
public class TransactionPayloadEventDataDeserializerTest {

      /* DATA is a binary representation of following:
       TransactionPayloadEventData{compression_type=0, payload_size=451, uncompressed_size='960', payload:
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=QUERY, serverId=223344, headerLength=19, dataLength=57, nextPosition=0, flags=8}, data=QueryEventData{threadId=12, executionTime=0, errorCode=0, database='', sql='BEGIN'}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=TABLE_MAP, serverId=223344, headerLength=19, dataLength=63, nextPosition=0, flags=0}, data=TableMapEventData{tableId=84, database='demo', table='movies', columnTypes=3, 15, 3, 15, 15, 15, 15, 15, 15, 15, 15, columnMetadata=0, 1024, 0, 1024, 1024, 4096, 2048, 1024, 1024, 1024, 1024, columnNullability={}, eventMetadata=TableMapEventMetadata{signedness={}, defaultCharset=255, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null,visibility=null}}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=EXT_UPDATE_ROWS, serverId=223344, headerLength=19, dataLength=756, nextPosition=0, flags=0}, data=UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, includedColumns={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, rows=[
               {before=[1, Once Upon a Time in the West, 1968, Italy, Western, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures], after=[1, Once Upon a Time in the West, 1968, Italy, Western|Action, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures]}
           ]}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=XID, serverId=223344, headerLength=19, dataLength=8, nextPosition=0, flags=0}, data=XidEventData{xid=31}}
           }
      */
      private static final byte[] DATA = {
        2, 1, 0, 3, 3, -4, -64, 3, 1, 3, -4, -61, 1, 0, 40, -75, 47, -3, 0, 88, -68, 13, 0, -90, -34,
        97, 57, 96, 103, -108, 14, 32, 1, 32, 8, -126, 32, 120, 18, 103, 8, -126, -114, 45, -84, -15,
        -9, -66, 68, 74, -118, -40, 82, 68, -110, 16, 13, -122, 26, 35, 98, 20, 123, 16, 7, -5, -10, 69,
        -128, 37, 107, 91, -42, 50, -10, -116, -6, -79, 51, 11, 93, -14, 73, 10, 87, 0, 81, 0, 81, 0,
        -1, -95, 63, -53, -78, 76, -31, -116, -56, -15, -88, -70, 26, 36, -55, -28, -13, 44, 66, -60,
        56, 4, -3, 113, -122, -58, 35, -112, 8, 18, 41, 28, -37, -42, -96, -83, -124, -73, -75, 84, -29,
        -48, 41, 62, -15, -88, -70, 6, 72, -110, -55, 71, -63, -125, 3, -90, -14, 103, -111, 67, 1, -98,
        -3, -15, 71, -125, -126, 88, -108, -16, -1, -104, 7, 79, -24, 6, -66, -16, -57, 53, -113, -86,
        -117, 33, 73, 38, -97, -100, -68, 96, 125, -103, -40, 32, 92, 7, 111, 51, -71, 110, -37, -109,
        -44, 33, 42, -59, -99, 73, -49, -29, 69, 16, -71, 49, -18, 87, 73, 108, -35, -45, -54, 18, -41,
        41, 55, -22, -87, 37, -75, 81, 29, 117, -106, 67, -32, -73, 16, 91, -50, 29, 30, -89, -16, -31,
        0, 126, 7, 4, -120, 45, 39, -73, -126, -55, 45, -41, 106, 20, -87, -55, 125, 49, -56, -99, 120,
        -63, 11, 4, -116, 57, 100, -71, 87, -109, -35, 44, -34, 110, -66, -32, -36, 62, -55, -46, 77,
        54, -27, 40, -111, -39, -61, 73, 86, -34, 77, 16, -11, -70, 26, 110, -78, 93, -85, 68, 124, 75,
        -79, -62, 77, -70, -27, 110, -102, 104, -87, -61, -28, -59, -92, 16, 113, -87, 126, 112, -109,
        30, -86, -101, 19, 49, -22, -87, -44, 19, -55, -115, 41, 68, -68, -104, -38, 117, 34, -46, 81,
        98, 69, -123, -21, -1, -1, -65, -31, 4, 30, 85, 23, -125, 36, -103, 124, -70, -63, -119, 18, 5,
        96, -5, 58, 112, 106, 18, 9, -71, -45, -106, 62, -107, 120, -92, 57, -41, -106, 108, -50, -19,
        37, -101, 27, 55, -59, 35, 109, -102, 58, -82, -31, -37, 74, 54, -11, -108, -33, 86, 98, 67, 94,
        -117, 71, -55, 110, 79, 47, -79, 65, -27, -66, -60, 3, -53, 61, -75, -9, 58, 34, -69, 113, 18,
        0, 9, -123, 64, 53, 121, 75, 21, -68, 7, 33, -73, -30, -127, -103, 9, 17, 66, -49, 84, 65, 2,
        43, 16, -125, 0, 43, 55, 114, 109, 4, -50, -64, -62, -64, 99, 0, 28, -96, 53, -96, -13, 0, -68,
        1, 0, 0
      };

    // Compression type for Zstd is 0
    private static final int COMPRESSION_TYPE = 0;
    private static final int PAYLOAD_SIZE = 451;
    private static final int UNCOMPRESSED_SIZE = 960;
    private static final int NUMBER_OF_UNCOMPRESSED_EVENTS = 4;
    private static final String UNCOMPRESSED_UPDATE_EVENT =
      new StringBuilder()
          .append(
              "UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, includedColumns={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, rows=[\n")
          .append(
              "    {before=[1, Once Upon a Time in the West, 1968, Italy, Western, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures],")
          .append(
              " after=[1, Once Upon a Time in the West, 1968, Italy, Western|Action, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures]}\n")
          .append("]}")
          .toString();

    private static final byte[] UNCOMPRESSED_UPDATE_EVENT_BEFORE_ROW_0_BYTE_ARRAY = new byte[] {1, 0, 0, 0};

    @Test
    public void deserialize() throws IOException {
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();
        TransactionPayloadEventData transactionPayloadEventData =
            deserializer.deserialize(new ByteArrayInputStream(DATA));
          assertEquals(COMPRESSION_TYPE, transactionPayloadEventData.getCompressionType());
          assertEquals(PAYLOAD_SIZE, transactionPayloadEventData.getPayloadSize());
          assertEquals(UNCOMPRESSED_SIZE, transactionPayloadEventData.getUncompressedSize());
          assertEquals(NUMBER_OF_UNCOMPRESSED_EVENTS, transactionPayloadEventData.getUncompressedEvents().size());
          assertEquals(EventType.QUERY, transactionPayloadEventData.getUncompressedEvents().get(0).getHeader().getEventType());
          assertEquals(EventType.TABLE_MAP, transactionPayloadEventData.getUncompressedEvents().get(1).getHeader().getEventType());
          assertEquals(EventType.EXT_UPDATE_ROWS, transactionPayloadEventData.getUncompressedEvents().get(2).getHeader().getEventType());
          assertEquals(EventType.XID, transactionPayloadEventData.getUncompressedEvents().get(3).getHeader().getEventType());
          assertEquals(UNCOMPRESSED_UPDATE_EVENT, transactionPayloadEventData.getUncompressedEvents().get(2).getData().toString());
    }

    @Test
    public void deserializeUsingEventDeserializer() throws IOException {

        ByteArrayInputStream dataStream = new ByteArrayInputStream(DATA);

        // Mock create target TransactionPayloadEventData DATA event header
        final EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.TRANSACTION_PAYLOAD);
        eventHeader.setEventLength(DATA.length + 19L);
        eventHeader.setTimestamp(1646406641000L);
        eventHeader.setServerId(223344);


        EventHeaderDeserializer eventHeaderDeserializer = new EventHeaderDeserializer() {

            private long count = 0L;

            private EventHeaderDeserializer defaultEventHeaderDeserializer = new EventHeaderV4Deserializer();

            @Override
            public EventHeader deserialize(ByteArrayInputStream inputStream) throws IOException {
                if (count > 0) {
                    // uncompressed event header deserialize
                    return defaultEventHeaderDeserializer.deserialize(inputStream);
                }
                count++;
                // we need to return target TransactionPayloadEventData DATA event header we had mocked
                return eventHeader;
            }
        };

        EventDeserializer eventDeserializer = new EventDeserializer(eventHeaderDeserializer, new NullEventDataDeserializer());
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.INTEGER_AS_BYTE_ARRAY);

        Event event = eventDeserializer.nextEvent(dataStream);

        assertTrue(event.getHeader().getEventType() == EventType.TRANSACTION_PAYLOAD);
        assertTrue(event.getData() instanceof TransactionPayloadEventData);

        TransactionPayloadEventData transactionPayloadEventData = event.getData();
        assertEquals(COMPRESSION_TYPE, transactionPayloadEventData.getCompressionType());
        assertEquals(PAYLOAD_SIZE, transactionPayloadEventData.getPayloadSize());
        assertEquals(UNCOMPRESSED_SIZE, transactionPayloadEventData.getUncompressedSize());
        assertEquals(NUMBER_OF_UNCOMPRESSED_EVENTS, transactionPayloadEventData.getUncompressedEvents().size());
        assertEquals(EventType.QUERY, transactionPayloadEventData.getUncompressedEvents().get(0).getHeader().getEventType());
        assertEquals(EventType.TABLE_MAP, transactionPayloadEventData.getUncompressedEvents().get(1).getHeader().getEventType());
        assertEquals(EventType.EXT_UPDATE_ROWS, transactionPayloadEventData.getUncompressedEvents().get(2).getHeader().getEventType());
        assertEquals(EventType.XID, transactionPayloadEventData.getUncompressedEvents().get(3).getHeader().getEventType());
//        assertEquals(UNCOMPRESSED_UPDATE_EVENT, transactionPayloadEventData.getUncompressedEvents().get(2).getData().toString());
        assertTrue(transactionPayloadEventData.getUncompressedEvents().get(2).getData() instanceof UpdateRowsEventData);

        UpdateRowsEventData updateRowsEventData = transactionPayloadEventData.getUncompressedEvents().get(2).getData();
        assertEquals(1, updateRowsEventData.getRows().size());
        Serializable[] updateBefore = updateRowsEventData.getRows().get(0).getKey();
        assertEquals(UNCOMPRESSED_UPDATE_EVENT_BEFORE_ROW_0_BYTE_ARRAY, updateBefore[0]);
    }

}
