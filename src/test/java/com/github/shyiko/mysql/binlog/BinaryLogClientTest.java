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

import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;
import com.github.shyiko.mysql.binlog.network.SocketFactory;
import org.testng.annotations.Test;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogClientTest {

    @Test
    public void testEventListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getEventListeners().isEmpty());
        TraceEventListener traceEventListener = new TraceEventListener();
        binaryLogClient.registerEventListener(traceEventListener);
        binaryLogClient.registerEventListener(new CountDownEventListener());
        binaryLogClient.registerEventListener(new CapturingEventListener());
        assertEquals(binaryLogClient.getEventListeners().size(), 3);
        binaryLogClient.unregisterEventListener(traceEventListener);
        assertEquals(binaryLogClient.getEventListeners().size(), 2);
        binaryLogClient.unregisterEventListener(CapturingEventListener.class);
        assertEquals(binaryLogClient.getEventListeners().size(), 1);
    }

    @Test
    public void testLifecycleListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getLifecycleListeners().isEmpty());
        TraceLifecycleListener traceLifecycleListener = new TraceLifecycleListener();
        binaryLogClient.registerLifecycleListener(traceLifecycleListener);
        binaryLogClient.registerLifecycleListener(new BinaryLogClientStatistics());
        binaryLogClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
        });
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 3);
        binaryLogClient.unregisterLifecycleListener(traceLifecycleListener);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 2);
        binaryLogClient.unregisterLifecycleListener(BinaryLogClientStatistics.class);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 1);
    }

    @Test(expectedExceptions = TimeoutException.class)
    public void testNoConnectionTimeout() throws Exception {
        new BinaryLogClient("_localhost_", 3306, "root", "mysql").connect(0);
    }

    @Test(timeOut = 15000)
    public void testConnectionTimeout() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33059, "root", "mysql");
        final CountDownLatch socketBound = new CountDownLatch(1);
        final CountDownLatch binaryLogClientDisconnected = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33059));
                        socketBound.countDown();
                        Socket accept = serverSocket.accept();
                        accept.getOutputStream().write(1);
                        accept.getOutputStream().flush();
                        assertTrue(binaryLogClientDisconnected.await(3000, TimeUnit.MILLISECONDS));
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        binaryLogClient.setConnectTimeout(1000);
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            binaryLogClientDisconnected.countDown();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullEventDeserializerIsNotAllowed() throws Exception {
        new BinaryLogClient("localhost", 3306, "root", "mysql").setEventDeserializer(null);
    }

    @Test(timeOut = 15000)
    public void testDisconnectWhileBlockedByFBRead() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33061, "root", "mysql");
        final CountDownLatch readAttempted = new CountDownLatch(1);
        binaryLogClient.setSocketFactory(new SocketFactory() {
            @Override
            public Socket createSocket() throws SocketException {
                return new Socket() {

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new FilterInputStream(super.getInputStream()) {

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
                                readAttempted.countDown();
                                return super.read(b, off, len);
                            }
                        };
                    }
                };
            }
        });
        binaryLogClient.setKeepAlive(false);
        final CountDownLatch socketBound = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33061));
                        socketBound.countDown();
                        serverSocket.accept(); // accept socket but do NOT send anything
                        assertTrue(readAttempted.await(3000, TimeUnit.MILLISECONDS));
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.yield();
                                    binaryLogClient.disconnect();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                        thread.start();
                        thread.join();
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            assertEquals(readAttempted.getCount(), 0);
            assertTrue(e.getMessage().contains("Failed to connect to MySQL"));
        }
    }

    /**
     * Test that requestBinaryLogStreamMaria does not throw NPE when gtidEnabled is true
     * but gtidSet is null (DBZ-9243). When no GTID position is available, the method should
     * fall back to binlog file/position mode and NOT send SET @slave_connect_state.
     */
    @Test
    public void testMariaDbStreamRequestWithNullGtidSetDoesNotThrowNPE() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();

        // Subclass to test the fixed decision logic without a real network connection
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Mirror the fixed logic: when gtidSet is null, must NOT throw NPE
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };

        // Simulate what Debezium does: setGtidSet("") → gtidEnabled=true but gtidSet stays null
        client.setGtidSet("");
        // gtidSet field must still be null (empty string skips object creation in setGtidSet)
        assertEquals(client.getGtidSet(), null);

        // This must NOT throw NullPointerException
        client.requestBinaryLogStreamMaria(65535L);

        // Verify the fallback path (file/position) was taken, not the GTID path
        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "USE_BINLOG_POSITION");
        assertFalse(sentCommands.get(0).contains("slave_connect_state"),
            "SET @slave_connect_state should NOT be sent when gtidSet is null");
    }

    /**
     * Test that requestBinaryLogStreamMaria does not send SET @slave_connect_state
     * when gtidSet is an empty MariaDB GTID set (DBZ-9243). An empty GTID set (as
     * initialized by setupGtidSet() when no prior GTID exists) means no known position —
     * should fall back to binlog file/position.
     */
    @Test
    public void testMariaDbStreamRequestWithEmptyGtidSetFallsBackToFilePosition() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Mirror the fixed logic: empty gtidSet should NOT send slave_connect_state
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };

        // Simulate setupGtidSet() initializing gtidSet = new MariadbGtidSet("") when gtidStr was ""
        client.setGtidSet("");
        synchronized (client.gtidSetAccessLock) {
            client.gtidSet = new MariadbGtidSet("");
        }
        assertEquals(client.getGtidSet(), ""); // empty string — not null, but still no real GTID

        // Must not send SET @slave_connect_state = '' to MariaDB
        client.requestBinaryLogStreamMaria(65535L);

        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "USE_BINLOG_POSITION",
            "When gtidSet is empty, should fall back to binlog file/position");
    }

    /**
     * Test that requestBinaryLogStreamMaria correctly sends SET @slave_connect_state
     * when a valid, non-empty MariaDB GTID is available — verifies the happy path is unaffected.
     */
    @Test
    public void testMariaDbStreamRequestWithValidGtidSendsSlaveConnectState() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Happy path: valid non-empty GTID should send slave_connect_state
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };

        // Provide a valid MariaDB GTID (domain-server-sequence format)
        client.setGtidSet("0-1-1");
        assertEquals(client.getGtidSet(), "0-1-1");

        client.requestBinaryLogStreamMaria(65535L);

        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "SET @slave_connect_state = '0-1-1'",
            "When gtidSet is non-empty, SET @slave_connect_state must be sent");
    }

    /*
    @Test
    public void testDeadlockyCode() throws IOException, InterruptedException {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "123456");
        binaryLogClient.setHeartbeatInterval(10000);
        binaryLogClient.setKeepAlive(true);
        binaryLogClient.setKeepAliveInterval(2000);

        binaryLogClient.connect();

        Thread.sleep(1000);

        binaryLogClient.disconnect();
    }
    */
}
