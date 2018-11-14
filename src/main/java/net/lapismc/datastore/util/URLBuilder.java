package net.lapismc.datastore.util;

@SuppressWarnings("unused")
public class URLBuilder {

    private String location = null;
    private Integer port = null;
    private String database = null;
    private Boolean useSSL = true;

    public URLBuilder setLocation(String location) {
        this.location = location;
        return this;
    }

    public URLBuilder setPort(Integer port) {
        this.port = port;
        return this;
    }

    public URLBuilder setDatabase(String database) {
        this.database = database;
        return this;
    }

    public URLBuilder setUseSSL(Boolean useSSL) {
        this.useSSL = useSSL;
        return this;
    }

    public LapisURL build() {
        return new LapisURL(location, port, database, useSSL);
    }

}
