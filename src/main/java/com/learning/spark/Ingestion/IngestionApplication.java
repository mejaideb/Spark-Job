package com.learning.spark.Ingestion;

import com.learning.spark.Ingestion.service.ReadCsvFilesService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class IngestionApplication {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		try {
			ReadCsvFilesService readCsvFilesService = new ReadCsvFilesService();
			readCsvFilesService.readCsvFiles("C:\\Users\\jmandal\\Desktop\\POC\\Ingestion\\src\\main\\resources\\data.csv");
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

}
