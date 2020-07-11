package com.learning.spark.Ingestion.models;

import com.learning.spark.Ingestion.constants.Gender;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Getter
@Setter
public class Customer implements Serializable {

    private String externalRefId;

    private String prefix;
    private String firstName;
    private String middleName;

    private String lastName;
    private String suffix;
    private String salutation;
    private Gender gender;

    private String dateOfBirth;
    private Set<Address> addresses;
    private String status;
    private List<Preference> preferences;
    private List<Communication> communication;
    private Instant activeFrom;
    private Instant activeTo;
    private Boolean isActive;
    private Boolean isWebEnabled;
    private Instant webEnableDate;
    private Map<String, Object> attributes;
    private Instant memberJoinDate;
    private String addresseeLine1;
    private String addresseeLine2;
    private String addresseeLine3;

}
