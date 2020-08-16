package net.lapismc.datastore;

import net.lapismc.lapiscore.LapisCorePlugin;
import org.bukkit.Bukkit;

import java.util.List;

@SuppressWarnings("unused")
public abstract class DataStore {

    /**
     * This separator should be used when submitting values in the addData methods
     */
    @SuppressWarnings("WeakerAccess")
    public String valueSeparator = "º";
    private boolean isAsync = true;
    LapisCorePlugin core;

    DataStore(LapisCorePlugin core) {
        this.core = core;
    }

    /**
     * This will create and necessary files and generate tables
     */
    public abstract void initialiseDataStore();

    /**
     * Get the storage type
     *
     * @return Returns the type of this DataStore
     */
    public abstract StorageType getStorageType();

    /**
     * Check if operations are being done asynchronously
     *
     * @return true if operations are async, otherwise false
     */
    public boolean isAsync() {
        return isAsync;
    }

    /**
     * Enable or disable async operations
     *
     * @param isAsync true means operations will be completed asynchronously
     */
    public void setAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }

    /**
     * Closes any currently open connections to the database
     */
    public abstract void closeConnection();

    /**
     * Shutdown will close any connections and shutdown the connection manager
     */
    public abstract void shutdown();

    /**
     * Add data to a table, this may result in duplicate values
     *
     * @param table  The table you wish to add too
     * @param values The values you wish to add, must be in the correct order based on the table definition and be
     *               separated by the {@link #valueSeparator}, Must include all values of the table
     */
    public abstract void addData(Table table, String values);

    /**
     * Add or update data in a table, if used properly this will ensure that there are no duplicate
     * rows and allows mass value updating
     *
     * @param table        The table you wish to edit
     * @param primaryKey   The primary key of this table
     * @param primaryValue The value you wish to add/edit for in the primary key column
     * @param values       The values you wish to add, must be in the correct order based on the table definition and be
     *                     separated by the {@link #valueSeparator}, Must include all values of the table
     */
    public abstract void addData(Table table, String primaryKey, String primaryValue, String values);

    /**
     * Set a single value in a row
     *
     * @param table        The table you wish to edit
     * @param primaryKey   The primary key of the row you wish to edit
     * @param primaryValue The primary keys value for the row you wish to edit
     * @param key          The key of the value you wish to edit
     * @param value        The value you wish the key to be set too
     */
    public abstract void setData(Table table, String primaryKey, String primaryValue, String key, String value);

    /**
     * Get a value of the type Long
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns a long if the data is found or Null if not
     */
    public abstract Long getLong(Table table, String primaryKey, String value, String key);

    /**
     * Get a value of the type String
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns a string if the data is found or Null if not
     */
    public abstract String getString(Table table, String primaryKey, String value, String key);

    /**
     * Get a value of the type Boolean
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns a boolean if the data is found or Null if not
     */
    public abstract Boolean getBoolean(Table table, String primaryKey, String value, String key);

    /**
     * Get a value of the type Object
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns an object if the data is found or Null if not
     */
    public abstract Object getObject(Table table, String primaryKey, String value, String key);

    /**
     * Get a list of long, use a non unique primary key and value to get multiple values returned
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns a list of long if the data is found or Null if not
     */
    public abstract List<Long> getLongList(Table table, String primaryKey, String value, String key);

    /**
     * Get a list of strings, use a non unique primary key and value to get multiple values returned
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary key of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @param key        The key to the value you wish to access
     * @return Returns a list of string if the data is found or Null if not
     */
    public abstract List<String> getStringList(Table table, String primaryKey, String value, String key);

    /**
     * Get all the values in a column of a table
     *
     * @param table The table you wish to access
     * @param key   The column you wish to receive values from
     * @return Returns a list of string of all the values in the column
     */
    public abstract List<String> getEntireColumn(Table table, String key);

    /**
     * Returns a list of String that contains all the values in a row that matches the primary key and value
     *
     * @param table      The table you wish to access
     * @param primaryKey The primary value of the row you wish to access
     * @param value      The value of the primary key in the row you wish to access
     * @return Returns the first row that matches the criteria, an empty list if no row is found
     */
    public abstract List<String> getEntireRow(Table table, String primaryKey, String value);

    /**
     * Get a list of all rows in a table, the values are separated with {@link #valueSeparator}
     *
     * @param table The table you wish to retrieve
     * @return Returns a list of char separated strings containing all values in the table
     */
    public abstract List<String> getEntireTable(Table table);

    /**
     * Removes any row with value in the key column in table
     *
     * @param table The table you wish to edit
     * @param key   The key you wish to test for
     * @param value The value required for a record to be deleted
     */
    public abstract void removeData(Table table, String key, String value);

    /**
     * Removes all rows from the given table
     *
     * @param table the table to truncate
     */
    public abstract void removeAllData(Table table);

    /**
     * Drop an entire table
     *
     * @param table The table you wish to drop
     */
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
