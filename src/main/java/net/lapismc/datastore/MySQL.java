package net.lapismc.datastore;

import net.lapismc.datastore.util.LapisURL;
import net.lapismc.lapiscore.LapisCorePlugin;
import org.bukkit.Bukkit;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public abstract class MySQL extends DataStore {

    Connection conn;
    private LapisURL url;
    private String username, password;

    MySQL(LapisCorePlugin core) {
        super(core);
    }

    public MySQL(LapisCorePlugin core, LapisURL url, String username, String password) {
        super(core);
        this.url = url;
        this.username = username;
        this.password = password;
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialiseDataStore() {
        try {
            getConnection(false);
            createTables(conn);
        } catch (SQLException e) {
            e.printStackTrace();
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
            conn = DriverManager.getConnection(url.getURLString(StorageType.MySQL, includeDatabase), username, password);
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
    public void addData(Table table, String values) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            try {
                getConnection(true);
                String valuesQuery = getQuery(table.getCommaSeparatedValues());
                String sql = "INSERT INTO " + table.getName() + "(" + table.getCommaSeparatedValues() + ") VALUES(" + valuesQuery + ")";
                PreparedStatement preStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                int i = 1;
                for (String s : values.split("#")) {
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
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
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
        try {
            while (incrementIfValid(rs)) {
                list.add(rs.getString(key));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return list;
    }

    @Override
    public List<String> getEntireColumn(Table table, String key) {
        ResultSet rs = getEntireTableAsResultSet(table);
        List<String> list = new ArrayList<>();
        try {
            while (incrementIfValid(rs)) {
                list.add(rs.getString(key));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return list;
    }

    @Override
    public void removeData(Table table, String key, String value) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
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
    protected void dropTable(Table table) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
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
                    entry.append(rs.getString(columnName)).append(",");
                }
                entry = new StringBuilder(entry.substring(0, entry.length() - 1));
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
            if (!rs.isBeforeFirst()) {
                return false;
            }
            try {
                return rs.next();
            } catch (NullPointerException e) {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    abstract void createTables(Connection conn) throws SQLException;

    private String getQuery(String values) {
        StringBuilder query = new StringBuilder();
        String[] valuesArray = values.split(",");
        for (String ignored : valuesArray) {
            query.append("?,");
        }
        return query.toString().substring(0, query.toString().length() - 1);
    }
}
