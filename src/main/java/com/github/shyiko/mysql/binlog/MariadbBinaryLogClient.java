package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.AnnotateRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.MariadbGtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.MariadbGtidListEventDataDeserializer;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.DumpBinaryLogCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;

import java.io.IOException;

/**
 * Mariadb replication stream client.
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbBinaryLogClient extends BinaryLogClient {

    private boolean useSendAnnotateRowsEvent;

    public MariadbBinaryLogClient(String username, String password) {
        super(username, password);
    }

    public MariadbBinaryLogClient(String schema, String username, String password) {
        super(schema, username, password);
    }

    public MariadbBinaryLogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    public MariadbBinaryLogClient(String hostname, int port, String schema, String username, String password) {
        super(hostname, port, schema, username, password);
    }

    @Override
    protected GtidSet buildGtidSet(String gtidSet) {
        return new MariadbGtidSet(gtidSet);
    }

    @Override
    protected void setupGtidSet() throws IOException {
        //Mariadb ignore
    }

    @Override
    protected void requestBinaryLogStream() throws IOException {
        long serverId = isBlocking() ? this.getServerId() : 0; // http://bugs.mysql.com/bug.php?id=71178
        Command dumpBinaryLogCommand;
        synchronized (gtidSetAccessLock) {
            if (gtidSet != null) {
                channel.write(new QueryCommand("SET @mariadb_slave_capability=4"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_connect_state = '" + gtidSet.toString() + "'"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_strict_mode = 0"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_ignore_duplicates = 0"));
                checkError(channel.read());
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, "", 0L, isUseSendAnnotateRowsEvent());

            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, getBinlogFilename(), getBinlogPosition());
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    @Override
    protected void ensureGtidEventDataDeserializer() {
        ensureEventDataDeserializer(EventType.ANNOTATE_ROWS, AnnotateRowsEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.MARIADB_GTID, MariadbGtidEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.MARIADB_GTID_LIST, MariadbGtidListEventDataDeserializer.class);
    }

    public boolean isUseSendAnnotateRowsEvent() {
        return useSendAnnotateRowsEvent;
    }

    public void setUseSendAnnotateRowsEvent(boolean useSendAnnotateRowsEvent) {
        this.useSendAnnotateRowsEvent = useSendAnnotateRowsEvent;
    }
}
