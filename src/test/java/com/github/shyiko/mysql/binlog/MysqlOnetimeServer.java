package com.github.shyiko.mysql.binlog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.logging.Logger;
import java.util.logging.Level;

public class MysqlOnetimeServer {
    private final MysqlOnetimeServerOptions options;
    public static int nextServerID = 1;
	public  final int SERVER_ID = MysqlOnetimeServer.nextServerID++;
    private final Logger logger = Logger.getLogger(getClass().getName());


	private Connection connection;
	private int port;
	private int serverPid;
	public String path;

	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	public MysqlOnetimeServer() {
	    this.options = new MysqlOnetimeServerOptions();
    }

    public MysqlOnetimeServer(MysqlOnetimeServerOptions options) {
	    this.options = options == null ? new MysqlOnetimeServerOptions() : options;
    }

	public void boot() throws Exception {
        final String dir = System.getProperty("user.dir");
        final String xtraParams = options.extraParams == null ? "" : options.extraParams;

		// By default, MySQL doesn't run under root. However, in an environment like Docker, the root user is the
		// only available user by default. By adding "--user=root" when the root user is used, we can make sure
		// the tests can continue to run.
		boolean isRoot = System.getProperty("user.name").equals("root");

		String gtidParams = "";
        if ( options.gtid ) {
			logger.info("In gtid test mode.");
			gtidParams =
				"--gtid-mode=ON " +
				"--log-slave-updates=ON " +
				"--enforce-gtid-consistency=true ";
		}
		String serverID = "";
		if ( !xtraParams.contains("--server_id") )
			serverID = "--server_id=" + options.serverID;

		String authPlugin = "";

		if ( getVersion().atLeast(8, 0) && !xtraParams.contains("--default-authentication-plugin")) {
			authPlugin = "--default-authentication-plugin=mysql_native_password";
		}

		String fullRowMetaData = "";
		if ( getVersion().atLeast(8, 0) && options.fullRowMetaData ) {
			fullRowMetaData = "--binlog-row-metadata=FULL";
		}

		ProcessBuilder pb = new ProcessBuilder(
			dir + "/src/test/onetimeserver",
			"--mysql-version=" + getVersionString(),
			"--log-slave-updates",
			"--log-bin=master",
			"--binlog_format=row",
			"--innodb_flush_log_at_trx_commit=0",
			serverID,
			"--character-set-server=utf8",
			"--sync_binlog=0",
			"--default-time-zone=+00:00",
			fullRowMetaData,
			isRoot ? "--user=root" : "",
			authPlugin,
			gtidParams
		);

		for ( String s : xtraParams.split(" ") ) {
			pb.command().add(s);
		}

		Process p = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

		p.waitFor();

		final BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

		new Thread(() -> {
            while (true) {
                String l = null;
                try {
                    l = errReader.readLine();
                } catch ( IOException e) {};

                if (l == null)
                    break;
                System.err.println(l);
            }
        }).start();

		String json = reader.readLine();
		String outputFile;
		try {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> output = mapper.readValue(json, MAP_STRING_OBJECT_REF);
			this.port = (Integer) output.get("port");
			this.serverPid = (Integer) output.get("server_pid");
			this.path = (String) output.get("mysql_path");
			outputFile = (String) output.get("output");
		} catch ( Exception e ) {
			logger.log(Level.SEVERE, "got exception while parsing " + json, e);
			throw(e);
		}


		resetConnection();
		this.connection.createStatement().executeUpdate("CREATE USER 'maxwell'@'127.0.0.1' IDENTIFIED BY 'maxwell'");
		this.connection.createStatement().executeUpdate("GRANT REPLICATION SLAVE on *.* to 'maxwell'@'127.0.0.1'");
		this.connection.createStatement().executeUpdate("GRANT ALL on *.* to 'maxwell'@'127.0.0.1'");
		this.connection.createStatement().executeUpdate("CREATE DATABASE if not exists test");
		logger.info("booted at port " + this.port + ", outputting to file " + outputFile);

		if ( options.masterServer != null ) {
		    this.setupSlave(options.masterServer.port);
        }
	}

	public void setupSlave(int masterPort) throws SQLException {
		Connection master = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + masterPort + "/mysql?useSSL=false", "root", "");
		ResultSet rs = master.createStatement().executeQuery("show master status");
		if ( !rs.next() )
			throw new RuntimeException("could not get master status");

		String file = rs.getString("File");
		Long position = rs.getLong("Position");
        rs.close();

		String changeSQL = String.format(
			"CHANGE MASTER to master_host = '127.0.0.1', master_user='maxwell', master_password='maxwell', "
			+ "master_log_file = '%s', master_log_pos = %d, master_port = %d",
			file, position, masterPort
		);
		logger.info("starting up slave: " + changeSQL);
		getConnection().createStatement().execute(changeSQL);
		getConnection().createStatement().execute("START SLAVE");


		rs.close();

        ResultSet status = query("show slave status");
        if ( !status.next() )
            throw new RuntimeException("could not get slave status");

        if ( status.getString("Slave_IO_Running").equals("No")
                || status.getString("Slave_SQL_Running").equals("No")) {
            throw new RuntimeException("could not start slave: " + dumpQuery("show slave status"));

        }
        status.close();
	}


	public String dumpQuery(String query) throws SQLException {
	    String result = "";
        ResultSet rs = getConnection().createStatement().executeQuery(query);
        rs.next();
        for ( int i = 1 ; i <= rs.getMetaData().getColumnCount() ; i++) {
            Object val = rs.getObject(i);
            String asString = val == null ? "null" : val.toString();
            result = result + rs.getMetaData().getColumnName(i) + ": " + asString  + "\n";
        }
        return result;
    }

	public void resetConnection() throws SQLException {
		this.connection = getNewConnection();
	}

	public Connection getNewConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/mysql?zeroDateTimeBehavior=convertToNull&useSSL=false", "root", "");
	}

	public Connection getConnection() {
		return connection;
	}

	public Connection getConnection(String defaultDB) throws SQLException {
		Connection conn = getNewConnection();
		conn.setCatalog(defaultDB);
		return conn;
	}

	public void execute(String query) throws SQLException {
		Statement s = getConnection().createStatement();
		s.executeUpdate(query);
		s.close();
	}

	private Connection cachedCX;
	public void executeCached(String query) throws SQLException {
		if ( cachedCX == null )
			cachedCX = getConnection();

		Statement s = cachedCX.createStatement();
		s.executeUpdate(query);
		s.close();
	}

	public void executeList(List<String> queries) throws SQLException {
		for (String q: queries) {
			if ( q.matches("^\\s*$") )
				continue;

			execute(q);
		}
	}

	public void executeList(String[] schemaSQL) throws SQLException {
		executeList(Arrays.asList(schemaSQL));
	}

	public void executeQuery(String sql) throws SQLException {
		getConnection().createStatement().executeUpdate(sql);
	}

	public ResultSet query(String sql) throws SQLException {
		return getConnection().createStatement().executeQuery(sql);
	}

	public int getPort() {
		return port;
	}

	public void shutDown() {
		try {
			Runtime.getRuntime().exec("kill " + this.serverPid);
		} catch ( IOException e ) {}
	}

    public static MysqlVersion getVersion() {
        String[] parts = getVersionString().split("\\.");
        return new MysqlVersion(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
    }

	private static String getVersionString() {
		String mysqlVersion = System.getenv("MYSQL_VERSION");
		return mysqlVersion == null ? "5.7" : mysqlVersion;
	}

	public void waitForSlaveToBeCurrent(MysqlOnetimeServer master) throws Exception {
		ResultSet ms = master.query("show master status");
		ms.next();
		String masterFile = ms.getString("File");
		Long masterPos = ms.getLong("Position");
		ms.close();

		while ( true ) {
			ResultSet rs = query("show slave status");
			rs.next();
			if ( rs.getString("Relay_Master_Log_File").equals(masterFile) &&
				rs.getLong("Exec_Master_Log_Pos") >= masterPos )
				return;

			Thread.sleep(200);
		}
	}
}
