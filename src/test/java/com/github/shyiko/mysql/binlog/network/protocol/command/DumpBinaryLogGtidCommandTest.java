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
package com.github.shyiko.mysql.binlog.network.protocol.command;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Verifies that COM_BINLOG_DUMP_GTID omits MySQL 8.4 tagged GTIDs, which the command has no wire representation for.
 */
public class DumpBinaryLogGtidCommandTest {

    private static final String UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    // 1 (command) + 2 (flags) + 4 (server id) + 4 (filename length) + 0 (empty filename) + 8 (position)
    private static final int DATA_SIZE_OFFSET = 19;
    private static final int UUID_SET_COUNT_OFFSET = DATA_SIZE_OFFSET + 4;

    private static long readLittleEndian(byte[] b, int offset, int length) {
        long result = 0;
        for (int i = 0; i < length; i++) {
            result |= ((long) (b[offset + i] & 0xff)) << (8 * i);
        }
        return result;
    }

    private static byte[] serialize(String gtidSet) throws Exception {
        return new DumpBinaryLogGtidCommand(1, "", 4, new GtidSet(gtidSet)).toByteArray();
    }

    @Test
    public void testUntaggedGtidSetIsSerialized() throws Exception {
        byte[] payload = serialize(UUID + ":1-42");
        assertEquals(readLittleEndian(payload, UUID_SET_COUNT_OFFSET, 8), 1);
        // 8 (count) + 16 (uuid) + 8 (interval count) + 16 (one interval)
        assertEquals(readLittleEndian(payload, DATA_SIZE_OFFSET, 4), 48);
    }

    @Test
    public void testTaggedGtidSetIsSkipped() throws Exception {
        byte[] payload = serialize(UUID + ":mysqlsh:1-42");
        assertEquals(readLittleEndian(payload, UUID_SET_COUNT_OFFSET, 8), 0);
        assertEquals(readLittleEndian(payload, DATA_SIZE_OFFSET, 4), 8);
    }

    @Test
    public void testOnlyUntaggedPartOfMixedGtidSetIsSerialized() throws Exception {
        byte[] payload = serialize(UUID + ":1-42:mysqlsh:1-7");
        // the tagged group must not be folded into the untagged one, which would claim transactions we do not hold
        assertEquals(readLittleEndian(payload, UUID_SET_COUNT_OFFSET, 8), 1);
        assertEquals(readLittleEndian(payload, DATA_SIZE_OFFSET, 4), 48);
    }
}
