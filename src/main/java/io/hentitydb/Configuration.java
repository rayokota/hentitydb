package io.hentitydb;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class Configuration {
    @NotNull
    private Map<String, String> properties = Maps.newLinkedHashMap();

    @JsonProperty
    private boolean autoTableCreation = false;

    @JsonProperty
    private String namespacePrefix;

    @JsonProperty
    private int regionCount = 1;

    @JsonProperty
    private String jarFilePath;

    @JsonProperty
    private String compression = "GZ";

    @JsonProperty
    private boolean testMode = false;

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
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



