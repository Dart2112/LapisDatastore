package net.lapismc.datastore;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public abstract class Table {

    private String name;
    private List<String> values;

    public Table(String name, String... values) {
        this.name = name;
        this.values = new ArrayList<>(Arrays.asList(values));
    }

    public abstract void createTable(Connection conn);

    public String getName() {
        return name;
    }

    public List<String> getValues() {
        return values;
    }

    public String getCommaSeparatedValues() {
        StringBuilder s = new StringBuilder();
        for (String value : values) {
            s.append(value);
        }
        return s.toString();
    }

    @Override
    public String toString() {
        return name;
    }

}
