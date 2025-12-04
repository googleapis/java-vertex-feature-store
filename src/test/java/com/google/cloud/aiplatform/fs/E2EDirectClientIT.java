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

import org.junit.Test;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;

// How to run this integration test
// mvn verify -Dclirr.skip=true  -DskipITs=false  -DprojectId=<project ID> -DfeatureOnlineStoreId=<FOS ID> -DfeatureViewId=<FV ID>
public class E2EDirectClientIT {

    private static final String PROJECT_ID = System.getProperty("projectId");
    private static final String FEATURE_ONLINE_STORE_ID = System.getProperty("featureOnlineStoreId");
    private static final String FEATURE_VIEW_ID = System.getProperty("featureViewId");

    @Test
    public void single_fetch_request() throws Exception {
      System.out.println("integration test is executed");
      String fv_path = String.format(
              "projects/%s/locations/us-central1/featureOnlineStores/%s/featureViews/%s",
              PROJECT_ID, FEATURE_ONLINE_STORE_ID, FEATURE_VIEW_ID);
      System.out.println("fv_path");
      FeatureOnlineStoreDirectClient client;
      try {
        client = FeatureOnlineStoreDirectClient.create(fv_path);
        FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
                .setFeatureView(fv_path)
                .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
                .setDataKey(FeatureViewDataKey.newBuilder().setKey("id_10001").build())
                .build();
        long startTime = System.nanoTime();
        FetchFeatureValuesResponse response = client.fetchFeatureValues(request);
        long endTime = System.nanoTime();
        double durationMillis = (endTime - startTime) / 1000000.0;
        System.out.println(response.toString());
        System.out.printf("Execution time: %.3f milliseconds%n", durationMillis);
      } catch (Exception e) {
        System.out.println("Error fetching features " + e);
      }
    }
}
