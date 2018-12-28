package net.lapismc.datastore;

import net.lapismc.lapiscore.LapisCorePlugin;
import org.bukkit.Bukkit;

import java.util.List;

@SuppressWarnings("unused")
public abstract class DataStore {

    LapisCorePlugin core;
    @SuppressWarnings("WeakerAccess")
    public String valueSeparator = "º";

    DataStore(LapisCorePlugin core) {
        this.core = core;
    }

    public abstract void initialiseDataStore();

    public abstract StorageType getStorageType();

    public abstract void closeConnection();

    public abstract void shutdown();

    public abstract void addData(Table table, String values);

    public abstract void setData(Table table, String primaryKey, String primaryValue, String key, String value);

    public abstract Long getLong(Table table, String primaryKey, String value, String key);

    public abstract String getString(Table table, String primaryKey, String value, String key);

    public abstract Boolean getBoolean(Table table, String primaryKey, String value, String key);

    public abstract Object getObject(Table table, String primaryKey, String value, String key);

    public abstract List<Long> getLongList(Table table, String primaryKey, String value, String key);

    public abstract List<String> getStringList(Table table, String primaryKey, String value, String key);

    public abstract List<String> getEntireColumn(Table table, String key);

    public abstract List<String> getEntireTable(Table table);

    public abstract void removeData(Table table, String key, String value);

    protected abstract void dropTable(Table table);

    public void convertData(DataStore to, List<Table> tables) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            for (Table t : tables) {
                List<String> allRows = getEntireTable(t);
                for (String values : allRows) {
                    to.addData(t, values);
                }
                dropTable(t);
            }
        });
    }

    public enum StorageType {
        MySQL, H2, SQLite
    }

}
