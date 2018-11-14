package net.lapismc.datastore.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.lapismc.datastore.DataStore;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionManager {

    private HikariDataSource ds;

    public ConnectionManager(LapisURL url, DataStore.StorageType type, String driverClass, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverClass);
        ds = new HikariDataSource();
        ds.setJdbcUrl(url.getURL(type));
        ds.setUsername(username);
        ds.setPassword(password);
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public void shutdown() {
        ds.close();
    }
}
