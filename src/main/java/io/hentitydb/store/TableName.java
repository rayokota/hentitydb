package io.hentitydb.store;

import java.util.Objects;

public class TableName {

    public static final String DEFAULT_NAMESPACE = "default";

    private final String namespace;
    private final String name;

    public TableName(String name) {
        int index = name.indexOf(":");
        if (index > 0) {
            this.namespace = name.substring(0, index);
            this.name = name.substring(index + 1);
        } else {
            this.namespace = DEFAULT_NAMESPACE;
            this.name = name;
        }
    }

    public TableName(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    /**
     * Returns the namespace of the table.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }


    /**
     * Returns the name of the table.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return namespace + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableName tableName = (TableName) o;

        if (!Objects.equals(name, tableName.name)) return false;
        if (!Objects.equals(namespace, tableName.namespace)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
