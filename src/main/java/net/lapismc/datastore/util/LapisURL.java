package net.lapismc.datastore.util;

import net.lapismc.datastore.DataStore;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class LapisURL {

    private String location, database;
    private Integer port;
    private Boolean useSSL;

    LapisURL(String location, Integer port, String database, boolean useSSL) {
        this.location = location;
        this.database = database;
        this.port = port;
        this.useSSL = useSSL;
    }

    public URL getURL(DataStore.StorageType type, boolean includeDatabase) {
        try {
            String url = getProtocol(type) + location + (port != null ? ":" + port : "");
            return new URL(appendForType(type, url, includeDatabase));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public URL getURL(DataStore.StorageType type) {
        return getURL(type, true);
    }

    public String getURLString(DataStore.StorageType type, boolean includeDatabase) {
        return getURL(type, includeDatabase).toString();
    }

    public File getFile(DataStore.StorageType type) {
        switch (type) {
            case H2:
                return new File(location + ".mv.db");
            case SQLite:
                return new File(location);
        }
        return null;
    }

    private String getProtocol(DataStore.StorageType type) {
        switch (type) {
            case MySQL:
                return "jdbc:mysql:";
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
                url = url + "TRACE_LEVEL_FILE=0;TRACE_LEVEL_SYSTEM_OUT=0;";
                break;
        }
        return url;
    }

}
