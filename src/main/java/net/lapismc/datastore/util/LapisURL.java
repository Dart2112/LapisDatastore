package net.lapismc.datastore.util;

import net.lapismc.datastore.DataStore;

import java.io.File;

public class LapisURL {

    private final String location;
    private final String database;
    private final Integer port;
    private final Boolean useSSL;

    LapisURL(String location, Integer port, String database, boolean useSSL) {
        this.location = location;
        this.database = database;
        this.port = port;
        this.useSSL = useSSL;
    }

    public String getURL(DataStore.StorageType type, boolean includeDatabase) {
        String url = getProtocol(type) + location + (port != null ? ":" + port : "");
        return appendForType(type, url, includeDatabase);
    }

    public File getFile(DataStore.StorageType type) {
        switch (type) {
            case H2:
                return new File(location + ".mv.db");
            case SQLite:
                return new File(location + ".db");
        }
        return null;
    }

    public String getDatabase() {
        return database;
    }

    private String getProtocol(DataStore.StorageType type) {
        switch (type) {
            case MySQL:
                return "jdbc:mysql://";
            case H2:
                return "jdbc:h2:";
            case SQLite:
                return "jdbc:sqlite:";
        }
        return "";
    }

    private String appendForType(DataStore.StorageType type, String url, boolean includeDatabase) {
        switch (type) {
            case MySQL:
                url = url + (includeDatabase ? "/" + database : "") + (useSSL ? "?verifyServerCertificate=false&useSSL=true" : "?useSSL=false");
                break;
            case H2:
                url = url + ";TRACE_LEVEL_FILE=0;TRACE_LEVEL_SYSTEM_OUT=0;DB_CLOSE_ON_EXIT=FALSE;";
                break;
            case SQLite:
                url = url + ".db";
        }
        return url;
    }

}
