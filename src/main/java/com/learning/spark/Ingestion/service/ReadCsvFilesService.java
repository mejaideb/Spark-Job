package com.learning.spark.Ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.spark.Ingestion.constants.ApplicationConstants;
import com.learning.spark.Ingestion.constants.CommunicationType;
import com.learning.spark.Ingestion.constants.Gender;
import com.learning.spark.Ingestion.messaging.MessagePublisher;
import com.learning.spark.Ingestion.models.Address;
import com.learning.spark.Ingestion.models.Communication;
import com.learning.spark.Ingestion.models.Customer;
import com.learning.spark.Ingestion.models.Preference;
import com.learning.spark.Ingestion.models.kafka.KafkaModel;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.*;

public class ReadCsvFilesService implements Serializable {


    public void readCsvFiles(String path) throws JsonProcessingException {
        SparkSession sparkSession = SparkSession.builder().appName("demo").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(path).toDF();
        Dataset<Customer> customerDataSet = dataset.map((MapFunction<Row, Customer>) this::getCustomerObject, Encoders.bean(Customer.class));
        MessagePublisher.publishMessageToKafka(customerDataSet);
    }

    private Customer getCustomerObject(Row row) throws JsonProcessingException {
        Customer customer = new Customer();
        Map<String, Object> attributes = new HashMap<>();
        setCustomerFields(row, customer);
        setCustomersChild(row, customer, attributes);
        return customer;

    }

    private void setCustomersChild(Row row, Customer customer, Map<String, Object> attributes) {
        CustomizeCustomerObject.setAttributes(row, customer, attributes);
        CustomizeCustomerObject.setCommunication(row, customer);
        CustomizeCustomerObject.setAddressForCustomer(row, customer);
        CustomizeCustomerObject.setPreferences(row, customer);
    }

    private void setCustomerFields(Row row, Customer customer) {
        customer.setFirstName(row.getAs("USER_FIRST_NM").toString());

        RandomStringUtils.randomAlphanumeric(10);
        customer.setSalutation(row.getAs("USER_NAME_TITLE").toString());
        customer.setLastName(row.getAs("USER_LAST_NM").toString());

        if (Objects.nonNull(row.getAs("REGISTRATION_ID").toString())) {
            int maxLengthOfExternalRefId = 10;
            int lengthOfRegId = row.getAs("REGISTRATION_ID").toString().length();
            String randomAlphanumeric = RandomStringUtils.randomAlphanumeric(maxLengthOfExternalRefId - lengthOfRegId);
            String registration_id = row.getAs("REGISTRATION_ID").toString();
            customer.setExternalRefId(registration_id.concat(randomAlphanumeric));
        } else
            customer.setExternalRefId(RandomStringUtils.randomAlphanumeric(10));
        customer.setAddresseeLine1(row.getAs("USER_FIRST_NM").toString().concat(" ").concat(row.getAs("USER_LAST_NM").toString()));
        customer.setBrandName("RBS_IN");
        String registration_id = row.getAs("REGISTRATION_ID").toString();
        int length = registration_id.length();


        if (Objects.nonNull(row.getAs("USER_GENDER_CD").toString())) {
            if (row.getAs("USER_GENDER_CD").toString().equals("M"))
                customer.setGender(Gender.MALE);
            else if (row.getAs("USER_GENDER_CD").toString().equals("F"))
                customer.setGender(Gender.FEMALE);
            else
                customer.setGender(Gender.NOT_DISCLOSED);
        }
        customer.setDateOfBirth(row.getAs("USER_DOB").toString());
    }
}
