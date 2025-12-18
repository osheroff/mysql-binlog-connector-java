package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.testng.Assert.*;

/**
 * @author <a href="mailto:pratik.pandey@booking.com">Pratik Pandey</a>
 * Integration test for TRANSACTION_PAYLOAD event deserialization.
 * Requires MySQL 8.0.20+ with binlog_transaction_compression enabled.
 * Run Using: MYSQL_VERSION=8.0 mvn test -Dtest=TransactionPayloadIntegrationTest
 */
public class TransactionPayloadIntegrationTest extends AbstractIntegrationTest {

    @Override
    protected MysqlOnetimeServerOptions getOptions() {
        MysqlOnetimeServerOptions options = super.getOptions();
        // Enable transaction compression (requires MySQL 8.0.20+)
        options.extraParams = "--binlog-transaction-compression=ON --binlog-transaction-compression-level-zstd=3";
        return options;
    }

    @Test
    public void testVeryLargeTransactionNear2GB() throws Exception {
        // This test simulates a transaction with uncompressed size approaching 2GB
        // to validate streaming decompression handles the upper limits correctly

        // Expected behavior:
        // - Uncompressed size can exceed 2GB (uses streaming decompression)
        // - Compressed size must stay under 2GB (Java array limit)

        if (!mysqlVersion.atLeast(8, 0)) {
            throw new SkipException("Transaction compression requires MySQL 8.0.20+");
        }

        CapturingEventListener capturingEventListener = new CapturingEventListener();
        client.registerEventListener(capturingEventListener);
        client.unregisterEventListener(eventListener);
        client.registerEventListener(eventListener);

        try {
            // Create table with large BLOB column to generate big transactions
            master.execute(new BinaryLogClientIntegrationTest.Callback<Statement>() {
                public void execute(Statement statement) throws SQLException {
                    statement.execute("drop table if exists very_large_txn_test");
                    // LONGTEXT can store up to 4GB, perfect for our test
                    statement.execute("create table very_large_txn_test (" +
                        "id int primary key, " +
                        "data1 LONGTEXT, " +
                        "data2 LONGTEXT, " +
                        "data3 LONGTEXT)");
                }
            });
            eventListener.waitForAtLeast(EventType.QUERY, 2, BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
            eventListener.reset();

            // Generate large repeating data that compresses well
            // We want uncompressed to be ~2-3GB but compressed to stay under 2GB
            final StringBuilder largeChunk = new StringBuilder();
            final String pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz!@#$%^&*()_+REPEATING_PATTERN_";

            // Create a 1MB chunk of repeating data (compresses very well)
            int targetChunkSize = 1024 * 1024; // 1MB target
            int iterations = targetChunkSize / pattern.length(); // Calculate exact iterations needed
            for (int i = 0; i < iterations; i++) {
                largeChunk.append(pattern);
            }
            final String chunk = largeChunk.toString();

            System.out.println("Starting very large transaction test...");
            System.out.println("Chunk size: " + chunk.length() + " bytes");

            long startTime = System.currentTimeMillis();

            // Insert rows with large data
            // With 3 columns of 1MB each per row, and good compression:
            // - ~700 rows = ~2GB uncompressed
            // - Should compress to ~200-500MB depending on compression ratio
            final int numRows = 800;
            System.out.println("Inserting " + numRows + " rows with ~3MB each...");

            master.execute(new BinaryLogClientIntegrationTest.Callback<Statement>() {
                public void execute(Statement statement) throws SQLException {
                    statement.execute("BEGIN");
                    for (int i = 0; i < numRows; i++) {
                        // Use prepared statement to handle large data efficiently
                        String sql = String.format(
                            "insert into very_large_txn_test values(%d, '%s', '%s', '%s')",
                            i, chunk, chunk, chunk
                        );
                        statement.execute(sql);

                        if (i % 50 == 0 && i > 0) {
                            System.out.println("  Inserted " + i + " rows (" +
                                ((long) i * 3 * chunk.length() / (1024 * 1024)) + " MB uncompressed)...");
                        }
                    }
                    System.out.println("Committing transaction...");
                    statement.execute("COMMIT");
                }
            });

            long insertTime = System.currentTimeMillis() - startTime;
            System.out.println("Transaction committed in " + (insertTime / 1000) + " seconds");

            // Wait for transaction payload event (give it more time for large transactions)
            long largeTimeout = BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT * 1000; // 30 seconds
            eventListener.waitFor(EventType.TRANSACTION_PAYLOAD, 1, largeTimeout);

            // Verify the large payload was handled correctly
            List<TransactionPayloadEventData> payloadEvents =
                capturingEventListener.getEvents(TransactionPayloadEventData.class);

            assertTrue(payloadEvents.size() > 0, "Should have captured TRANSACTION_PAYLOAD event");

            TransactionPayloadEventData payloadEventData = payloadEvents.get(0);
            assertNotNull(payloadEventData, "TRANSACTION_PAYLOAD event data should not be null");

            long uncompressedSize = payloadEventData.getUncompressedSize();
            long compressedSize = payloadEventData.getPayloadSize();

            // Validate sizes
            assertTrue(compressedSize > 0, "Compressed size should be > 0");
            assertTrue(compressedSize < Integer.MAX_VALUE,
                "Compressed size must be < 2GB (Java array limit): " + compressedSize);
            assertTrue(uncompressedSize > 1024L * 1024 * 1024,
                "Uncompressed size should be > 1GB: " + (uncompressedSize / (1024 * 1024)) + " MB");

            // Verify compression ratio
            double compressionRatio = (double) uncompressedSize / compressedSize;
            assertTrue(compressionRatio > 2.0,
                "Should have good compression ratio (>2x) for repetitive data, got: " +
                String.format("%.2fx", compressionRatio));

            // Verify all events were decompressed successfully via streaming
            List<Event> uncompressedEvents = payloadEventData.getUncompressedEvents();
            assertNotNull(uncompressedEvents, "Should have uncompressed events");
            assertFalse(uncompressedEvents.isEmpty(), "Should have decompressed events successfully");

            // Count WriteRowsEventData events
            int writeRowsCount = 0;
            for (Event event : uncompressedEvents) {
                if (event.getData() instanceof WriteRowsEventData) {
                    writeRowsCount++;
                }
            }
            assertTrue(writeRowsCount > 0, "Should have WriteRowsEventData in the payload");

            long totalTime = System.currentTimeMillis() - startTime;

            System.out.println("\n=== Very Large Transaction Test Results ===");
            System.out.printf("Rows inserted: %d%n", numRows);
            System.out.printf("Estimated uncompressed data: ~%d MB%n",
                ((long) numRows * 3 * chunk.length()) / (1024 * 1024));
            System.out.printf("Actual uncompressed size: %d MB%n",
                uncompressedSize / (1024 * 1024));
            System.out.printf("Compressed size: %.2f MB (%.1f%% of limit)%n",
                compressedSize / (1024.0 * 1024.0),
                (compressedSize * 100.0 / Integer.MAX_VALUE));
            System.out.printf("Compression ratio: %.2fx%n", compressionRatio);
            System.out.printf("Events decompressed: %d%n", uncompressedEvents.size());
            System.out.printf("Write events: %d%n", writeRowsCount);
            System.out.printf("Total time: %.1f seconds%n", totalTime / 1000.0);
            System.out.println("===========================================\n");

        } finally {
            client.unregisterEventListener(capturingEventListener);
        }
    }
}
