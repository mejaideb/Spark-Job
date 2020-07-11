package com.learning.spark.Ingestion.models;

import com.learning.spark.Ingestion.constants.AddressType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Address {

    private AddressType type;
    private String addressLine1;
    private String addressLine2;
    private String addressLine3;
    private String streetName;
    private String houseNo;
    private String houseName;
    private String flatNo;
    private String city;
    private String cityCode;
    private String state;
    private String stateCode;
    private String country;
    private String countryCode;
    private String postalCode;
    private String county;
}
