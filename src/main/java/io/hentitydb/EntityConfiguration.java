package io.hentitydb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class EntityConfiguration {
    private final Configuration config;
    private boolean autoTableCreation = false;
    private String namespacePrefix;
    private int regionCount = 1;
    private String jarFilePath;
    private String compression = "GZ";
    private boolean testMode = false;

    public EntityConfiguration() {
        this(HBaseConfiguration.create());
    }

    public EntityConfiguration(Configuration config) {
        this.config = config;
    }

    public Configuration getHConfiguration() {
        return config;
    }

    public boolean getAutoTableCreation() {
        return autoTableCreation;
    }

    public void setAutoTableCreation(boolean autoTableCreation) {
        this.autoTableCreation = autoTableCreation;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public String getJarFilePath() {
        return jarFilePath;
    }

    public void setJarFilePath(String jarFilePath) {
        this.jarFilePath = jarFilePath;
    }

    public String getNamespacePrefix() {
        return namespacePrefix;
    }

    public void setNamespacePrefix(String namespacePrefix) {
        this.namespacePrefix = namespacePrefix;
    }

    public int getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(int regionCount) {
        this.regionCount = regionCount;
    }

    public boolean getTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }
}



