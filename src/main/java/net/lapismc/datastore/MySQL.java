package net.lapismc.datastore;

import net.lapismc.datastore.util.ConnectionManager;
import net.lapismc.datastore.util.LapisURL;
import net.lapismc.lapiscore.LapisCorePlugin;
import org.bukkit.Bukkit;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class MySQL extends DataStore {

    Connection conn;
    ConnectionManager connectionManager;
    LapisURL url;

    MySQL(LapisCorePlugin core) {
        super(core);
    }

    public MySQL(LapisCorePlugin core, LapisURL url, String username, String password) {
        super(core);
        this.url = url;
        connectionManager = new ConnectionManager(core, url, getStorageType(), username, password);
    }

    @Override
    public void initialiseDataStore() {
        try {
            getConnection(false);
            createDatabase();
            getConnection(true);
            createTables(conn);
        } finally {
            closeConnection();
        }
    }

    @Override
    public StorageType getStorageType() {
        return StorageType.MySQL;
    }

    void getConnection(boolean includeDatabase) {
        try {
            closeConnection();
            conn = connectionManager.getConnection(includeDatabase);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeConnection() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        closeConnection();
        if (connectionManager != null)
            connectionManager.shutdown();
    }

    public void createDatabase() {
        if (getStorageType().equals(StorageType.MySQL)) {
            try {
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + url.getDatabase() + ";");
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }
    }

    @Override
    public void addData(Table table, String primaryKey, String primaryValue, String values) {
        runTask(() -> {
            if (primaryValue.equals(getString(table, primaryKey, primaryValue, primaryKey))) {
                //The data exists so we just update it with the following query
                //update TABLE set column1='value',column2='value',column3='value' where primaryKey='primaryValue'
                StringBuilder query = new StringBuilder("update " + table.getName() + " set ");
                //Get lists of the columns and the desired values
                List<String> columnNames = table.getValues();
                List<String> valuesList = Arrays.asList(values.split(valueSeparator));
                //Loop through and generate the query
                for (int i = 0; i < columnNames.size(); i++) {
                    query.append(columnNames.get(i)).append("='").append(valuesList.get(i)).append("',");
                }
                //Remove the final comma
                query.deleteCharAt(query.length() - 1);
                //Append the rest of the command
                query.append(" where ").append(primaryKey).append("='").append(primaryValue).append("'");
                try {
                    //Execute
                    getConnection(true);
                    Statement stmt = conn.createStatement();
                    stmt.execute(query.toString());
                    //Clean up
                    stmt.close();
                } catch (SQLException e) {
                    core.getLogger().warning("An error occurred adding data to the database for " + core.getName()
                            + ", Sometimes this is a false alarm, other times the data might not have been set." +
                            "If you experience errors, please report them with the following line.");
                    core.getLogger().warning(e.getMessage());
                } finally {
                    closeConnection();
                }
            } else {
                //The data doesn't exist so we add it
                boolean async = isAsync();
                setAsync(false);
                addData(table, values);
                setAsync(async);
            }
        });
    }

    @Override
    public void addData(Table table, String values) {
        runTask(() -> {
            try {
                getConnection(true);
                String valuesQuery = getQuery(table.getCommaSeparatedValues());
                String sql = "INSERT INTO " + table.getName() + "(" + table.getCommaSeparatedValues() + ") VALUES(" + valuesQuery + ")";
                PreparedStatement preStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                int i = 1;
                for (String s : values.split(valueSeparator)) {
                    preStatement.setString(i, s);
                    i++;
                }
                preStatement.execute();
                preStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        });
    }

    @Override
    public void setData(Table table, String primaryKey, String primaryValue, String key, String value) {
        runTask(() -> {
            try {
                getConnection(true);
                String sqlUpdate = "UPDATE " + table.getName() + " SET " + key + " = ? WHERE " + primaryKey + " = ?";
                PreparedStatement preStatement = conn.prepareStatement(sqlUpdate);
                preStatement.setString(1, value);
                preStatement.setString(2, primaryValue);
                preStatement.execute();
                preStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        });
    }

    @Override
    public Long getLong(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        if (incrementIfValid(rs)) {
            try {
                return rs.getLong(key);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }
        return null;
    }

    @Override
    public String getString(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        if (incrementIfValid(rs)) {
            try {
                return rs.getString(key);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }
        return null;
    }

    @Override
    public Boolean getBoolean(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        if (incrementIfValid(rs)) {
            try {
                return rs.getInt(key) == 1;
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }
        return null;
    }

    @Override
    public Object getObject(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        if (incrementIfValid(rs)) {
            try {
                return rs.getObject(key);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }
        return null;
    }

    @Override
    public List<Long> getLongList(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        List<Long> list = new ArrayList<>();
        try {
            while (incrementIfValid(rs)) {
                list.add(rs.getLong(key));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return list;
    }

    @Override
    public List<String> getStringList(Table table, String primaryKey, String value, String key) {
        ResultSet rs = getResults(table, primaryKey, value);
        List<String> list = new ArrayList<>();
        getStringListFromResultSet(key, rs, list);
        return list;
    }

    @Override
    public List<String> getEntireColumn(Table table, String key) {
        ResultSet rs = getEntireTableAsResultSet(table);
        List<String> list = new ArrayList<>();
        getStringListFromResultSet(key, rs, list);
        return list;
    }

    @Override
    public List<String> getEntireRow(Table table, String primaryKey, String value) {
        ResultSet rs = getEntireTableAsResultSet(table);
        List<String> list = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            //loop over each row in the table
            while (incrementIfValid(rs)) {
                //Find one that matches the criteria
                if (rs.getString(primaryKey).equals(value)) {
                    //Loop over each column in the table and add its value
                    for (int i = 1; i <= columnCount; i++) {
                        list.add(rs.getString(i));
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    private void getStringListFromResultSet(String key, ResultSet rs, List<String> list) {
        try {
            while (incrementIfValid(rs)) {
                list.add(rs.getString(key));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    @Override
    public void removeData(Table table, String key, String value) {
        runTask(() -> {
            try {
                getConnection(true);
                String sqlUpdate = "DELETE FROM " + table.getName() + " WHERE " + key + " = ?";
                PreparedStatement preStatement = conn.prepareStatement(sqlUpdate);
                preStatement.setString(1, value);
                preStatement.execute();
                preStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        });
    }

    @Override
    public void removeAllData(Table table) {
        runTask(() -> {
            try {
                getConnection(true);
                String sqlUpdate = "TRUNCATE " + table.getName();
                Statement stmt = conn.createStatement();
                stmt.execute(sqlUpdate);
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        });
    }

    @Override
    protected void dropTable(Table table) {
        runTask(() -> {
            try {
                getConnection(true);
                String sqlUpdate = "DROP TABLE " + table.getName();
                Statement stmt = conn.createStatement();
                stmt.execute(sqlUpdate);
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        });
    }

    @Override
    public List<String> getEntireTable(Table table) {
        List<String> list = new ArrayList<>();
        ResultSet rs = getEntireTableAsResultSet(table);
        while (incrementIfValid(rs)) {
            try {
                StringBuilder entry = new StringBuilder();
                for (String columnName : table.getValues()) {
                    entry.append(rs.getString(columnName)).append(valueSeparator);
                }
                if (entry.length() > 1)
                    entry.deleteCharAt(entry.length() - 1);
                list.add(entry.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    private ResultSet getResults(Table table, String primaryKey, String value) {
        try {
            getConnection(true);
            String sqlUpdate = "SELECT * FROM " + table.getName() + " WHERE " + primaryKey + " = ?";
            PreparedStatement preStatement = conn.prepareStatement(sqlUpdate);
            preStatement.setString(1, value);
            return preStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ResultSet getEntireTableAsResultSet(Table table) {
        try {
            getConnection(true);
            Statement stmt = conn.createStatement();
            String sql = "SELECT * FROM " + table.getName();
            return stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean incrementIfValid(ResultSet rs) {
        try {
            if (rs == null) {
                return false;
            }
            if (rs.isClosed()) {
                return false;
            }
            if (rs.isAfterLast()) {
                return false;
            }
            try {
                return rs.next();
            } catch (NullPointerException e) {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void runTask(Runnable task) {
        if (isAsync()) {
            Bukkit.getScheduler().runTaskAsynchronously(core, task);
        } else {
            task.run();
        }
    }

    public abstract void createTables(Connection conn);

    private String getQuery(String values) {
        StringBuilder query = new StringBuilder();
        String[] valuesArray = values.split(",");
        for (String ignored : valuesArray) {
            query.append("?,");
        }
        return query.substring(0, query.toString().length() - 1);
    }

}
