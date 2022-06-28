package net.lapismc.datastore;

import net.lapismc.datastore.util.LapisURL;
import org.bukkit.plugin.java.JavaPlugin;
import org.h2.Driver;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class H2 extends MySQL {

    private final LapisURL url;

    public H2(JavaPlugin core, LapisURL url) {
        super(core);
        Driver.load();
        this.url = url;
    }

    @Override
    public StorageType getStorageType() {
        return StorageType.H2;
    }

    @Override
    public void initialiseDataStore() {
        File f = url.getFile(StorageType.H2);
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
