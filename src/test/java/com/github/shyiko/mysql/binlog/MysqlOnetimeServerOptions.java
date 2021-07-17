package com.github.shyiko.mysql.binlog;

public class MysqlOnetimeServerOptions {
    public int serverID = MysqlOnetimeServer.nextServerID++;
    public boolean gtid = false;
    public MysqlOnetimeServer masterServer;
    public String extraParams;
    public boolean fullRowMetaData;
}
