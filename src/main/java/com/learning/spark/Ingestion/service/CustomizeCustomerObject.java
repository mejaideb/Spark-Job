package com.learning.spark.Ingestion.service;

import com.learning.spark.Ingestion.constants.AddressType;
import com.learning.spark.Ingestion.constants.CommunicationSubType;
import com.learning.spark.Ingestion.constants.CommunicationType;
import com.learning.spark.Ingestion.models.Address;
import com.learning.spark.Ingestion.models.Communication;
import com.learning.spark.Ingestion.models.Customer;
import com.learning.spark.Ingestion.models.Preference;
import org.apache.spark.sql.Row;

import java.util.*;

public class CustomizeCustomerObject {

    public static void setAttributes(Row row, Customer customer, Map<String, Object> attributes) {
        attributes.put("registrationId", row.getAs("REGISTRATION_ID").toString());
        attributes.put("primaryEmail",row.getAs("USER_EMAIL_ADDR").toString());
        attributes.put("idFromSource", row.getAs("MSISDN").toString());
        attributes.put("userAccountCD", row.getAs("USER_ACCOUNT_CD").toString());
        attributes.put("userSourceContactID", row.getAs("USER_SRC_CONTACT_ID").toString());
        attributes.put("userUnderAgeFlag", row.getAs("USER_UNDERAGE_FLAG").toString());
        attributes.put("payerAccountCD", row.getAs("PAYER_ACCOUNT_CD").toString());
        attributes.put("payerSourceContactID", row.getAs("PAYER_SRC_CONTACT_ID").toString());
        attributes.put("payerNameTitle", row.getAs("PAYER_NAME_TITLE").toString());
        attributes.put("payerFirstName", row.getAs("PAYER_FIRST_NM").toString());
        attributes.put("payerLastName", row.getAs("PAYER_LAST_NM").toString());
        attributes.put("payerPostCode", row.getAs("PAYER_POSTCODE").toString());
        attributes.put("payerCountry", row.getAs("PAYER_COUNTRY").toString());
        attributes.put("payerDateOfBirth", row.getAs("PAYER_DOB").toString());
        attributes.put("payerUnderAgeFlag", row.getAs("PAYER_UNDERAGE_FLAG").toString());
        attributes.put("payerGenderCD", row.getAs("PAYER_GENDER_CD").toString());
        attributes.put("ownerAccountCD", row.getAs("OWNER_ACCOUNT_CD").toString());
        attributes.put("ownerSourceContactID", row.getAs("OWNER_SRC_CONTACT_ID").toString());
        attributes.put("ownerNameTitle", row.getAs("OWNER_NAME_TITLE").toString());
        attributes.put("ownerFirstName", row.getAs("OWNER_FIRST_NM").toString());
        attributes.put("ownerLastName", row.getAs("OWNER_LAST_NM").toString());
        attributes.put("ownerPostCode", row.getAs("OWNER_POSTCODE").toString());
        attributes.put("ownerCountry", row.getAs("OWNER_COUNTRY").toString());
        attributes.put("ownerDateOfBirth", row.getAs("OWNER_DOB").toString());
        attributes.put("ownerUnderAgeFlag", row.getAs("OWNER_UNDERAGE_FLAG").toString());
        attributes.put("ownerGenderCD", row.getAs("OWNER_GENDER_CD").toString());
        attributes.put("payerPermPrefWhiteMail", row.getAs("PAYER_PERM_PREF_WHITEMAIL").toString());
        attributes.put("payerPermPrefEmail", row.getAs("PAYER_PERM_PREF_EMAIL").toString());
        attributes.put("payerPermPrefSurvey", row.getAs("PAYER_PERM_PREF_SURVEY").toString());
        attributes.put("ownerPermPrefWhiteMail", row.getAs("OWNER_PERM_PREF_WHITEMAIL").toString());
        attributes.put("ownerPermPrefEmail", row.getAs("OWNER_PERM_PREF_EMAIL").toString());
        attributes.put("ownerPermPrefSurvey", row.getAs("OWNER_PERM_PREF_SURVEY").toString());
        attributes.put("payerEmail", row.getAs("PAYER_EMAIL_ADDR").toString());
        attributes.put("ownerEmail", row.getAs("OWNER_EMAIL_ADDR").toString());
        attributes.put("numberOfContactOnService", row.getAs("NUM_OF_CONTACT_ON_SERVICE").toString());
        attributes.put("numberOfContactOnAccount", row.getAs("NUM_OF_CONTACT_ON_ACCOUNT").toString());
        attributes.put("numberOfConnectionServiceOnAccount", row.getAs("NUM_OF_CONN_SERVICE_ON_ACCOUNT").toString());
        attributes.put("userContactNotOwnerContact", row.getAs("USER_CONTACT_NOT_OWNER_CONTACT").toString());
        attributes.put("userEmailNotOwnerEmail", row.getAs("USER_EMAIL_NOT_OWNER_EMAIL").toString());
        attributes.put("userPermPrefWhiteMail", row.getAs("USER_PERM_PREF_WHITEMAIL").toString());
        attributes.put("sourcePermPrefStartDate", row.getAs("SOURCE_PERM_PREF_START_DT").toString());
        attributes.put("isOldestServiceOnAccount", row.getAs("IS_OLDEST_SERVICE_ON_ACCOUNT").toString());
        attributes.put("connectedMobileCountOnAccount", row.getAs("CONNECTED_MOB_CNT_ON_ACCOUNT").toString());
        attributes.put("connectedMBBCountOnAccount", row.getAs("CONNECTED_MBB_CNT_ON_ACCOUNT").toString());
        attributes.put("connectedFLCountOnAccount", row.getAs("CONNECTED_FL_CNT_ON_ACCOUNT").toString());
        attributes.put("connectedFBCountOnAccount", row.getAs("CONNECTED_FB_CNT_ON_ACCOUNT").toString());
        attributes.put("prepayCustomerFlag", row.getAs("PREPAY_CUSTOMER_FLAG").toString());
        attributes.put("prepay6WeekPurchases", row.getAs("PREPAY_6_WEEK_PURCHASES").toString());
        attributes.put("appVersion", row.getAs("APP_VERSION").toString());
        attributes.put("pushOptInFlag", row.getAs("PUSH_OPT_IN_FLAG").toString());
        attributes.put("appUninstalledFlag", row.getAs("APP_UNINSTALLED_FLAG").toString());
        attributes.put("lastAppUsedDate", row.getAs("LAST_APP_USED_DT").toString());
        attributes.put("deviceBrandName", row.getAs("DEVICE_BRAND_NAME").toString());
        attributes.put("deviceModelName", row.getAs("DEVICE_MODEL_NAME").toString());
        attributes.put("lastActivityDate", row.getAs("LAST_ACTIVITY_DT").toString());
        attributes.put("lastRoamingDate", row.getAs("LAST_ROAMING_DATE").toString());
        attributes.put("lastRoamingLocation", row.getAs("LAST_ROAMING_LOCATION").toString());
        attributes.put("mAF_MSISDN", row.getAs("MAF_MSISDN").toString());
        attributes.put("mAFDiscountMSISDN", row.getAs("MAF_DISCOUNT_MSISDN").toString());
        attributes.put("mAFAccount", row.getAs("MAF_ACCOUNT").toString());
        attributes.put("mAFAccountDiscountMSISDN", row.getAs("MAF_ACCOUNT_DISCOUNT_MSISDN").toString());
        attributes.put("nPSLatestScore", row.getAs("NPS_LATEST_SCORE").toString());
        attributes.put("latestNPSDate", row.getAs("LATEST_NPS_DATE").toString());
        attributes.put("oOBDataSpendLastDate", row.getAs("OOB_DATA_SPEND_LAST_DATE").toString());
        attributes.put("oOBDataSpendLastAmount", row.getAs("OOB_DATA_SPEND_LAST_AMOUNT").toString());
        attributes.put("oOBDataSpend3Months", row.getAs("OOB_DATA_SPEND_3_MTHS").toString());
        attributes.put("lastContactDate", row.getAs("LAST_CONTACT_DATE").toString());
        attributes.put("contactsCount7Days", row.getAs("CONTACTS_COUNT_7_DAYS").toString());
        attributes.put("contactsCount30Days", row.getAs("CONTACTS_COUNT_30_DAYS").toString());
        attributes.put("contactsCount90Days", row.getAs("CONTACTS_COUNT_90_DAYS").toString());
        attributes.put("contractEndDate", row.getAs("CONTRACT_END_DATE").toString());
        attributes.put("deviceOS", row.getAs("DEVICE_OS").toString());
        attributes.put("deviceOSVersion", row.getAs("DEVICE_OS_VERSION").toString());
        attributes.put("pPMinutesDomestic", row.getAs("PP_MINS_DOMESTIC").toString());
        attributes.put("pPSMSDomestic", row.getAs("PP_SMS_DOMESTIC").toString());
        attributes.put("pPSDataDomestic", row.getAs("PP_DATA_DOMESTIC").toString());
        attributes.put("pPMinutesRoaming", row.getAs("PP_MINS_ROAMING").toString());
        attributes.put("pPSMSRoaming", row.getAs("PP_SMS_ROAMING").toString());
        attributes.put("pPDataRoaming", row.getAs("PP_DATA_ROAMING").toString());
        attributes.put("collectionsFlag", row.getAs("COLLECTIONS_YN").toString());
        attributes.put("customerType", row.getAs("CUSTOMER_TYPE").toString());
        attributes.put("consumerSegment", row.getAs("CONSUMER_SEGMENT").toString());
        attributes.put("namedUserID", row.getAs("NAMED_USER_ID").toString());
        attributes.put("connectionDate", row.getAs("CONNECTION_DT").toString());
        customer.setAttributes(attributes);

    }

    public static void setCommunication(Row row, Customer customer) {
        List<Communication> communicationArrayList = new ArrayList<>();

        if (Objects.nonNull(row.getAs("USER_EMAIL_ADDR").toString())) {
            Communication communication = new Communication();
            communication.setCommunicationValue(row.getAs("USER_EMAIL_ADDR").toString());
            communication.setCommunicationSubType(CommunicationSubType.CLIENT);
            communication.setCommunicationType(CommunicationType.EMAIL);
            communicationArrayList.add(communication);

        }
        if (Objects.nonNull(row.getAs("USER_ACCOUNT_CD").toString())) {
            Communication communication = new Communication();
            communication.setCommunicationValue(row.getAs("USER_ACCOUNT_CD").toString());
            communication.setCommunicationSubType(CommunicationSubType.CLIENT);
            communication.setCommunicationType(CommunicationType.MOBILE);
            communicationArrayList.add(communication);
        }
        customer.setCommunication(communicationArrayList);

    }

    public static void setAddressForCustomer(Row row, Customer customer) {
        Set<Address> addressSet = new HashSet<>();
        Address address = new Address();
        if (Objects.nonNull(row.getAs("USER_POSTCODE").toString())) {
            address.setPostalCode(row.getAs("USER_POSTCODE").toString());
            address.setCountry(row.getAs("USER_COUNTRY").toString());
            address.setType(AddressType.HOME);
        }
        addressSet.add(address);

        customer.setAddresses(addressSet);
    }

    public static void setPreferences(Row row, Customer customer) {
        List<Preference> preferenceArrayList = new ArrayList<>();

        if (Objects.nonNull(row.getAs("USER_PERM_PREF_EMAIL").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("email");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("USER_PERM_PREF_EMAIL").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_SMS").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("sms");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_SMS").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_CALL").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("call");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_CALL").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_MMS").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("mms");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_MMS").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("USER_PERM_PREF_SURVEY").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("survey");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("USER_PERM_PREF_SURVEY").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_LOCATION").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("location");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_LOCATION").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_BASIC_PROF").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("profile");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_BASIC_PROF").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        if (Objects.nonNull(row.getAs("SERVICE_PERM_PREF_TRAFFIC").toString())) {
            Preference preference = new Preference();
            preference.setPreferenceType("traffic");
            preference.setValue(row.getAs("USER_EMAIL_ADDR").toString());
            if (row.getAs("SERVICE_PERM_PREF_TRAFFIC").toString().equals("Y"))
                preference.setHasOpted(true);
            else
                preference.setHasOpted(false);
            preferenceArrayList.add(preference);
        }

        customer.setPreferences(preferenceArrayList);
    }
}
