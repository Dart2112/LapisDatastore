package net.lapismc.datastore.util;

public class URLBuilder {

    private String protocol;
    private String location;
    private Integer port;
    private String database;
    private Boolean useSSL;

    public URLBuilder setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

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
