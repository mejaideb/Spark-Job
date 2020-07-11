package com.learning.spark.Ingestion.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Preference {
    private Map<String, Object> attributes;
    private String preferenceType;
    private String membershipId;
    private String description;
    private String value;
    private Boolean hasOpted;

}
