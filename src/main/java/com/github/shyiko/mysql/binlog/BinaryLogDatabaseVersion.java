package com.github.shyiko.mysql.binlog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple class that provides details about the database server's version we're connected with.
 *
 * @author Chris Cranford
 */
public class BinaryLogDatabaseVersion {

    private static final Pattern VERSION = Pattern.compile("([0-9]*).([0-9]*).*");

    private final int major;
    private final int minor;
    private final String serverVersion;
    private final boolean mariaDb;

    public BinaryLogDatabaseVersion(String serverVersion) {
        this(-1, -1, serverVersion);
    }

    public BinaryLogDatabaseVersion(int major, int minor, String serverVersion) {
        this.major = major;
        this.minor = minor;
        this.serverVersion = serverVersion;
        this.mariaDb = serverVersion != null && serverVersion.toLowerCase().contains("mariadb");
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public boolean isMariaDb() {
        return mariaDb;
    }

    public boolean isGreaterThan(int major, int minor) {
        return this.major > major || (this.major == major && this.minor > minor);
    }

    public boolean isEqualTo(int major, int minor) {
        return this.major == major && this.minor == minor;
    }

    public boolean isGreaterThanOrEqualTo(int major, int minor) {
        return isGreaterThan(major, minor) || isEqualTo(major, minor);
    }

    @Override
    public String toString() {
        return serverVersion + " (major=" + major + ", minor=" + minor + ", mariadb=" + mariaDb + ")";
    }

    public static BinaryLogDatabaseVersion parse(String serverVersion) {
        if (serverVersion == null) {
            return new BinaryLogDatabaseVersion(serverVersion);
        }

        final Matcher matcher = VERSION.matcher(serverVersion);
        if (matcher.matches()) {
            final int major = Integer.parseInt(matcher.group(1));
            final int minor = Integer.parseInt(matcher.group(2));
            return new BinaryLogDatabaseVersion(major, minor, serverVersion);
        }
        return new BinaryLogDatabaseVersion(serverVersion);
    }
}
