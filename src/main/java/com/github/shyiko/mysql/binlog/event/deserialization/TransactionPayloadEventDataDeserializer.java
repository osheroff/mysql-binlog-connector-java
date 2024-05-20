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

import com.github.luben.zstd.Zstd;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 * @author <a href="mailto:debjeet.sarkar@booking.com">Debjeet Sarkar</a>
 */
public class TransactionPayloadEventDataDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
    public static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
    public static final int OTW_PAYLOAD_SIZE_FIELD = 1;
    public static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
    public static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;

    private Supplier<EventDeserializer> eventDeserializerSupplier = new Supplier<EventDeserializer>() {
        @Override
        public EventDeserializer get() {
            return new EventDeserializer();
        }
    };

    public TransactionPayloadEventDataDeserializer customizeEventDeserializerSupplier(Supplier<EventDeserializer> supplier) {
        this.eventDeserializerSupplier = supplier;
        return this;
    }

    @Override
    public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        TransactionPayloadEventData eventData = new TransactionPayloadEventData();
        // Read the header fields from the event data
        while (inputStream.available() > 0) {
            int fieldType = 0;
            int fieldLen = 0;
            // Read the type of the field
            if (inputStream.available() >= 1) {
                fieldType = inputStream.readPackedInteger();
            }
            // We have reached the end of the Event Data Header
            if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }
            // Read the size of the field
            if (inputStream.available() >= 1) {
                fieldLen = inputStream.readPackedInteger();
            }
            switch (fieldType) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    // Fetch the payload size
                    eventData.setPayloadSize(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size
                    eventData.setUncompressedSize(inputStream.readPackedInteger());
                    break;
                default:
                    // Ignore unrecognized field
                    inputStream.read(fieldLen);
                    break;
            }
        }
        if (eventData.getUncompressedSize() == 0) {
            // Default the uncompressed to the payload size
            eventData.setUncompressedSize(eventData.getPayloadSize());
        }
        // set the payload to the rest of the input buffer
        eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

        // Decompress the payload
        byte[] src = eventData.getPayload();
        byte[] dst = ByteBuffer.allocate(eventData.getUncompressedSize()).array();
        Zstd.decompressByteArray(dst, 0, dst.length, src, 0, src.length);

        // Read and store events from decompressed byte array into input stream
        ArrayList<Event> decompressedEvents = new ArrayList<>();
        EventDeserializer transactionPayloadEventDeserializer = obtainEventDeserializer();
        ByteArrayInputStream destinationInputStream = new ByteArrayInputStream(dst);

        Event internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
        while(internalEvent != null) {
            decompressedEvents.add(internalEvent);
            internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
        }

        eventData.setUncompressedEvents(decompressedEvents);

        return eventData;
    }

    protected EventDeserializer obtainEventDeserializer() {
        return eventDeserializerSupplier.get();
    }

    public interface Supplier<V> {

        V get();

    }

}
