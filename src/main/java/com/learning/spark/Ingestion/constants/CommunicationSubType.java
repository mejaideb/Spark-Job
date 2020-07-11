package com.learning.spark.Ingestion.constants;

import java.util.Arrays;

public enum CommunicationSubType {
    HOME("home"), OFFICE("office"), CLIENT("client");

    private String value;

    private CommunicationSubType(String value) {
        this.value = value;
    }

    public static CommunicationSubType fromValue(String value) {
        for (CommunicationSubType communicationSubType : values()) {
            if (communicationSubType.value.equalsIgnoreCase(value)) {
                return communicationSubType;
            }
        }
        throw new IllegalArgumentException(
                "Unknown enum type " + value + ", Allowed values are " + Arrays.toString(values()));
    }

    @Override
    public String toString() {
        return value;
    }
}
