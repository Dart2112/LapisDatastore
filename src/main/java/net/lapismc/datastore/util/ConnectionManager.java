package net.lapismc.datastore.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.lapismc.datastore.DataStore;
import net.lapismc.lapiscore.LapisCorePlugin;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class ConnectionManager {

    private HikariDataSource ds;
    private HikariConfig config;
    private HikariDataSource noDB;

    public ConnectionManager(LapisCorePlugin core, LapisURL url, DataStore.StorageType type, String username, String password) {
        config = new HikariConfig();
        config.setPoolName(core.getName() + "-hikari");


        //Different datasource for accessing the server without the db
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(1);
        config.setMaxLifetime(TimeUnit.MINUTES.toMillis(1));
        config.setDriverClassName(getDriverClass(type));
        config.setUsername(username);
        config.setPassword(password);
        config.setJdbcUrl(url.getURL(type, false));
        noDB = new HikariDataSource(config);

        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setJdbcUrl(url.getURL(type, true));
        if (!type.equals(DataStore.StorageType.MySQL))
            ds = new HikariDataSource(config);
    }

    private String getDriverClass(DataStore.StorageType type) {
        switch (type) {
            case MySQL:
                return "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
            case H2:
                return Driver.class.getName();
            case SQLite:
                return "org.sqlite.JDBC";
        }
        return "";
    }

    public Connection getConnection(boolean includeDatabase) throws SQLException {
        if (includeDatabase) {
            if (ds == null) {
                ds = new HikariDataSource(config);
            }
            return ds.getConnection();
        } else {
            return noDB.getConnection();
        }
    }

    public void shutdown() {
        ds.close();
    }
}
