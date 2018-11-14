package net.lapismc.datastore;

import net.lapismc.datastore.util.ConnectionManager;
import net.lapismc.datastore.util.LapisURL;
import net.lapismc.lapiscore.LapisCorePlugin;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public abstract class H2 extends MySQL {

    private LapisURL url;

    public H2(LapisCorePlugin core, LapisURL url) {
        super(core);
        this.url = url;
        connectionManager = new ConnectionManager(url, getStorageType(), "", "");
        //Driver.load();
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
            conn = connectionManager.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
