package com.learning.spark.Ingestion.constants;

import java.util.Arrays;

public enum AddressType {
    HOME("home"),
    OFFICE("office"),
    BILLING("billing");

    private String value;

    private AddressType(String value) {
        this.value = value;
    }

    public static AddressType fromValue(String value) {
        for (AddressType addressType : values()) {
            if (addressType.value.equalsIgnoreCase(value)) {
                return addressType;
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
