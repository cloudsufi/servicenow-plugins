package io.cdap.plugin.bigquery.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import org.apache.commons.lang3.RandomStringUtils;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.Random;

public class Test {

    public static void main(String[] args) throws IOException, InterruptedException {
        createTempSourceBQTableForReceivingSlipLineTable();
    }

    public static void createTempSourceBQTableForReceivingSlipLineTable() throws IOException, InterruptedException {
        String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
        String bqSourceTable = "testTable" + stringUniqueId;
        String receivingSlipLineRecordUniqueNumber = "ProcRecSlip" + stringUniqueId;

        BigQueryClient.getSoleQueryResult("create table `SN_test_automation." + bqSourceTable + "` as " +
                "SELECT * FROM UNNEST([ STRUCT('" + receivingSlipLineRecordUniqueNumber + "' " +
                "AS number, (DATETIME '2022-06-08 00:00:00')  AS received)])");
    }
}
