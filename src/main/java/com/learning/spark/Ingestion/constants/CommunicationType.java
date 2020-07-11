package com.learning.spark.Ingestion.constants;

import java.util.Arrays;

public enum CommunicationType {
    EMAIL("email"), MOBILE("mobile"), PHONE("phone");

    private String value;

    private CommunicationType(String value) {
        this.value = value;
    }

    public static CommunicationType fromValue(String value) {
        for (CommunicationType communicationType : values()) {
            if (communicationType.value.equalsIgnoreCase(value)) {
                return communicationType;
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
