package io.hentitydb.store;

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
     */
    public String getNamespace() {
        return namespace;
    }


    /**
     * Returns the name of the table.
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

        if (name != null ? !name.equals(tableName.name) : tableName.name != null) return false;
        if (namespace != null ? !namespace.equals(tableName.namespace) : tableName.namespace != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
