package net.lapismc.datastore;

import net.lapismc.lapiscore.LapisCorePlugin;
import org.bukkit.Bukkit;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public abstract class MySQL extends DataStore {

    private String url, username, password, database;
    private Connection conn;

    public MySQL(LapisCorePlugin core, String url, String username, String password, String database) {
        super(core);
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
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

    private void getConnection(boolean includeDatabase) {
        try {
            conn = DriverManager.getConnection(url + (includeDatabase ? database : "") +
                    "?verifyServerCertificate=false&useSSL=true", username, password);
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
    public void addData(String tableName, String valueNames, String values) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            try {
                getConnection(true);
                String valuesQuery = getQuery(valueNames);
                String sql = "INSERT INTO " + tableName + "(" + valueNames + ") VALUES(" + valuesQuery + ")";
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
    public void setData(String tableName, String primaryKey, String primaryValue, String key, String value) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            try {
                getConnection(true);
                String sqlUpdate = "UPDATE " + tableName + " SET " + key + " = ? WHERE " + primaryKey + " = ?";
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
    public Long getLong(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public String getString(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public Boolean getBoolean(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public Object getObject(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public List<Long> getLongList(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public List<String> getStringList(String tableName, String primaryKey, String value, String key) {
        ResultSet rs = getResults(tableName, primaryKey, value);
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
    public List<String> getEntireColumn(String tableName, String key) {
        ResultSet rs = getEntireTable(tableName);
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
    public void removeData(String tableName, String key, String value) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            try {
                getConnection(true);
                String sqlUpdate = "DELETE FROM " + tableName + " WHERE " + key + " = ?";
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
    protected void dropTable(String tableName) {
        Bukkit.getScheduler().runTaskAsynchronously(core, () -> {
            try {
                getConnection(true);
                String sqlUpdate = "DROP TABLE " + tableName;
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

    private ResultSet getResults(String tableName, String primaryKey, String value) {
        try {
            getConnection(true);
            String sqlUpdate = "SELECT * FROM " + tableName + " WHERE " + primaryKey + " = ?";
            PreparedStatement preStatement = conn.prepareStatement(sqlUpdate);
            preStatement.setString(1, value);
            return preStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ResultSet getEntireTable(String table) {
        try {
            getConnection(true);
            Statement stmt = conn.createStatement();
            String sql = "SELECT * FROM " + table;
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

    public abstract void createTables(Connection conn) throws SQLException;

    private String getQuery(String values) {
        StringBuilder query = new StringBuilder();
        String[] valuesArray = values.split(",");
        for (String ignored : valuesArray) {
            query.append("?,");
        }
        return query.toString().substring(0, query.toString().length() - 1);
    }
}
