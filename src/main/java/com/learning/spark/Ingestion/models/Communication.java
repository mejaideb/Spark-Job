package com.learning.spark.Ingestion.models;

import com.learning.spark.Ingestion.constants.CommunicationSubType;
import com.learning.spark.Ingestion.constants.CommunicationType;
import lombok.*;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Communication {

    private CommunicationType communicationType;
    private CommunicationSubType communicationSubType;
    private String communicationValue;
    private Map<String, Object> attributes;
    private Boolean isPrimary;

}
