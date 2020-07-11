package com.learning.spark.Ingestion.constants;

import java.util.Arrays;

public enum Gender {
    MALE("male"), FEMALE("female"), NOT_DISCLOSED("notDisclosed");

    private String value;

    private Gender(String value) {
        this.value = value;
    }

    public static Gender fromValue(String value) {
        for (Gender gender : values()) {
            if (gender.value.equalsIgnoreCase(value)) {
                return gender;
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
