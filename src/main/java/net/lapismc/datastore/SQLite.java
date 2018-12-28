package net.lapismc.datastore;

import net.lapismc.datastore.util.LapisURL;
import net.lapismc.lapiscore.LapisCorePlugin;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class SQLite extends MySQL {

    private LapisURL url;

    public SQLite(LapisCorePlugin core, LapisURL url) {
        super(core);
        this.url = url;
        try {
            Class.forName("org.sqlite.JDBC").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public StorageType getStorageType() {
        return StorageType.SQLite;
    }

    @Override
    public void initialiseDataStore() {
        File f = url.getFile(StorageType.SQLite);
        if (!f.exists()) {
            try {
                f.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        super.initialiseDataStore();
    }

    @Override
    void getConnection(boolean includeDatabase) {
        try {
            conn = DriverManager.getConnection(url.getURL(getStorageType(), false));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
