/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.aiplatform.fs;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

import com.google.cloud.aiplatform.v1.BoolArray;
import com.google.cloud.aiplatform.v1.DoubleArray;
import com.google.cloud.aiplatform.v1.FeatureValue;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList.FeatureNameValuePair;
import com.google.cloud.aiplatform.v1.StringArray;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// How to run this integration test
// mvn verify -Dclirr.skip=true  -DskipITs=false  -DprojectId=<project ID>
// -DfeatureOnlineStoreId=<FOS ID> -DfeatureViewId=<FV ID>
@RunWith(JUnit4.class)
public class E2EDirectClientIT {

    private static final String PROJECT_ID = System.getProperty("projectId");
    private static final String FEATURE_ONLINE_STORE_ID = System.getProperty("featureOnlineStoreId");
    private static final String FEATURE_VIEW_ID = System.getProperty("featureViewId");

    private static FeatureOnlineStoreDirectClient client;
    private static String featureViewPath;
    // Static test data based on the actual feature values in the test project.
    private static Map<FetchFeatureValuesRequest, FetchFeatureValuesResponse> testData;

    @BeforeClass
    public static void setUpClass() throws Exception {
        featureViewPath =
                String.format(
                        "projects/%s/locations/us-central1/featureOnlineStores/%s/featureViews/%s",
                        PROJECT_ID, FEATURE_ONLINE_STORE_ID, FEATURE_VIEW_ID);
        client = FeatureOnlineStoreDirectClient.create(featureViewPath);

        setupTestData();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void single_fetch_test() throws Exception {
        Map.Entry<FetchFeatureValuesRequest, FetchFeatureValuesResponse> entry =
                testData.entrySet().iterator().next();
        FetchFeatureValuesRequest request = entry.getKey();
        FetchFeatureValuesResponse expected = entry.getValue();

        FetchFeatureValuesResponse response = client.fetchFeatureValues(request);
        // Assert response
        assertThat(response).ignoringRepeatedFieldOrder().isEqualTo(expected);
    }

    @Test
    public void batch_fetch_test() throws Exception {
        // Create a batch of requests
        List<FetchFeatureValuesRequest> requests = new ArrayList<>(testData.keySet());
        // List of expected requests
        List<FetchFeatureValuesResponse> expected = new ArrayList<>(testData.values());

        List<FetchFeatureValuesResponse> actual = client.batchFetchFeatureValues(requests);
        // Assert response
        assertThat(actual).ignoringRepeatedFieldOrder().containsExactlyElementsIn(expected);
    }

    public static void setupTestData() {
        FetchFeatureValuesRequest request10001 = FetchFeatureValuesRequest.newBuilder()
            .setFeatureView(featureViewPath)
            .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
            .setDataKey(FeatureViewDataKey.newBuilder().setKey("id_10001").build())
            .build();

      FetchFeatureValuesResponse expected10001 =
          FetchFeatureValuesResponse.newBuilder().setKeyValues(
              FeatureNameValuePairList.newBuilder()
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_required")
                              .setValue(FeatureValue.newBuilder().setBoolValue(true).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_repeated")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBoolArrayValue(
                                          BoolArray.newBuilder()
                                              .addValues(true)
                                              .addValues(true)
                                              .build())
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_required")
                              .setValue(FeatureValue.newBuilder().setInt64Value(0).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_required")
                              .setValue(FeatureValue.newBuilder().setDoubleValue(1.2).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string")
                              .setValue(FeatureValue.newBuilder().setStringValue("value1a").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_required")
                              .setValue(FeatureValue.newBuilder().setStringValue("value2a").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value1"))
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes_required")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value2"))
                                      .build())
                              .build())
                      .build())
              .build();

      FetchFeatureValuesRequest request20002 =
          FetchFeatureValuesRequest.newBuilder()
              .setFeatureView(featureViewPath)
              .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
              .setDataKey(FeatureViewDataKey.newBuilder().setKey("id_20002").build())
              .build();
      FetchFeatureValuesResponse expected20002 =
          FetchFeatureValuesResponse.newBuilder()
              .setKeyValues(
                  FeatureNameValuePairList.newBuilder()
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_required")
                              .setValue(FeatureValue.newBuilder().setBoolValue(false).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int")
                              .setValue(FeatureValue.newBuilder().setInt64Value(0).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_required")
                              .setValue(FeatureValue.newBuilder().setInt64Value(0).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_required")
                              .setValue(FeatureValue.newBuilder().setDoubleValue(1.2).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_repeated")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setDoubleArrayValue(
                                          DoubleArray.newBuilder()
                                              .addValues(2.3)
                                              .addValues(3.456)
                                              .build()))
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string")
                              .setValue(FeatureValue.newBuilder().setStringValue("value1b").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_required")
                              .setValue(FeatureValue.newBuilder().setStringValue("value2b").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value1"))
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes_required")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value2"))
                                      .build())
                              .build())
                      .build())
              .build();

      FetchFeatureValuesRequest request30003 =
          FetchFeatureValuesRequest.newBuilder()
              .setFeatureView(featureViewPath)
              .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
              .setDataKey(FeatureViewDataKey.newBuilder().setKey("id_30003").build())
              .build();
      FetchFeatureValuesResponse expected30003 =
          FetchFeatureValuesResponse.newBuilder()
              .setKeyValues(
                  FeatureNameValuePairList.newBuilder()
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_required")
                              .setValue(FeatureValue.newBuilder().setBoolValue(false).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bool_repeated")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBoolArrayValue(
                                          BoolArray.newBuilder()
                                              .addValues(true)
                                              .addValues(true)
                                              .build())
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int")
                              .setValue(FeatureValue.newBuilder().setInt64Value(0).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_required")
                              .setValue(FeatureValue.newBuilder().setInt64Value(1).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_int_repeated")
                              .setValue(FeatureValue.newBuilder().build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float")
                              .setValue(FeatureValue.newBuilder().setDoubleValue(0.1).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_required")
                              .setValue(FeatureValue.newBuilder().setDoubleValue(1.2).build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_float_repeated")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setDoubleArrayValue(
                                          DoubleArray.newBuilder()
                                              .addValues(2.3)
                                              .addValues(3.456)
                                              .build()))
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string")
                              .setValue(FeatureValue.newBuilder().setStringValue("").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_required")
                              .setValue(FeatureValue.newBuilder().setStringValue("value2c").build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_string_repeated")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setStringArrayValue(
                                          StringArray.newBuilder()
                                              .addValues("value3a")
                                              .addValues("value4a")
                                              .build())
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value1"))
                                      .build())
                              .build())
                      .addFeatures(
                          FeatureNameValuePair.newBuilder()
                              .setName("test_bytes_required")
                              .setValue(
                                  FeatureValue.newBuilder()
                                      .setBytesValue(ByteString.copyFromUtf8("value2"))
                                      .build())
                              .build())
                      .build())
              .build();

      testData =
          ImmutableMap.of(
              request10001, expected10001, request20002, expected20002, request30003, expected30003);
    }
}
