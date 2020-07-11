package com.learning.spark.Ingestion.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.spark.Ingestion.constants.ApplicationConstants;
import com.learning.spark.Ingestion.models.Customer;
import com.learning.spark.Ingestion.models.kafka.KafkaModel;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;
import java.util.UUID;

public class MessagePublisher implements Serializable {

    public static void publishMessageToKafka(Dataset<Customer> customerDataSet) {
        Dataset<KafkaModel> objectToJsonDataSet = getKafkaModelDataSet(customerDataSet);
        objectToJsonDataSet.toDF(ApplicationConstants.ID,ApplicationConstants.VALUE).selectExpr(ApplicationConstants.KAFKA_KEY_CAST_VALUE, ApplicationConstants.KAFKA_CAST_VALUE)
                .write().format(ApplicationConstants.KAFKA)
                .option(ApplicationConstants.TOPIC, ApplicationConstants.TOPIC_NAME)
                .option(ApplicationConstants.KAFKA_BOOTSTRAP_SERVERS, ApplicationConstants.SERVER_PORT)
                .save();
    }

    private static Dataset<KafkaModel> getKafkaModelDataSet(Dataset<Customer> customerDataSet) {
        return customerDataSet.map((MapFunction<Customer,KafkaModel>) MessagePublisher::createCustomerModel, Encoders.bean(KafkaModel.class));
    }

    public static KafkaModel createCustomerModel(Customer customer) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String customerJson = mapper.writeValueAsString(customer);
        KafkaModel kafkaModel = getKafkaModel(customerJson);
        return kafkaModel;
    }

    private static KafkaModel getKafkaModel(String customerJson) {
        KafkaModel kafkaModel = new KafkaModel();
        kafkaModel.setKey(UUID.randomUUID().toString());
        kafkaModel.setValue(customerJson);
        return kafkaModel;
    }
}
