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

    public ConnectionManager(LapisCorePlugin core, LapisURL url, DataStore.StorageType type, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(core.getName() + "-hikari");

        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setMaxLifetime(TimeUnit.MINUTES.toMillis(1));
        config.setDriverClassName(getDriverClass(type));
        config.setJdbcUrl(url.getURL(type));
        config.setUsername(username);
        config.setPassword(password);

        ds = new HikariDataSource(config);
        ds.setUsername(username);
        ds.setPassword(password);
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

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public void shutdown() {
        ds.close();
    }
}
