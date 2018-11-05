package net.lapismc.datastore;

import net.lapismc.lapiscore.LapisCorePlugin;

import java.util.List;

@SuppressWarnings("unused")
public abstract class DataStore {

    LapisCorePlugin core;

    public DataStore(LapisCorePlugin core){
        this.core = core;
    }

    public abstract void initialiseDataStore();

    public abstract StorageType getStorageType();

    public abstract void closeConnection();

    public abstract void addData(String tableName, String valueNames, String values);

    public abstract void setData(String tableName, String primaryKey, String primaryValue, String key, String value);

    public abstract Long getLong(String tableName, String primaryKey, String value, String key);

    public abstract String getString(String tableName, String primaryKey, String value, String key);

    public abstract Boolean getBoolean(String tableName, String primaryKey, String value, String key);

    public abstract Object getObject(String tableName, String primaryKey, String value, String key);

    public abstract List<Long> getLongList(String tableName, String primaryKey, String value, String key);

    public abstract List<String> getStringList(String tableName, String primaryKey, String value, String key);

    public abstract List<String> getEntireColumn(String tableName, String key);

    public abstract void removeData(String tableName, String key, String value);

    protected abstract void dropTable(String tableName);

    public enum StorageType {
        MySQL, H2, Yaml;
    }

}
