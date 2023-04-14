package net.lapismc.datastore;

import net.lapismc.datastore.util.LapisURL;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class SQLite extends MySQL {

    private final LapisURL url;

    public SQLite(JavaPlugin core, LapisURL url) {
        super(core);
        this.url = url;
        try {
            Class.forName("org.sqlite.JDBC").getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
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
    Connection getConnection(boolean includeDatabase) {
        try {
            return DriverManager.getConnection(url.getURL(getStorageType(), false));
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
