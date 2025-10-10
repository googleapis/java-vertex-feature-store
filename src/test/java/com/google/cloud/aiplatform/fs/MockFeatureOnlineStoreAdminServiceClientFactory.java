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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceSettings;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.junit.After;

// Mock class to initialize the mock FeatureOnlineStoreAdminServiceClient.
 class MockFeatureOnlineStoreAdminServiceClientFactory implements FeatureOnlineStoreAdminServiceClientFactory {

  private final MockFeatureOnlineStoreAdminService mockService;
  private FeatureOnlineStoreAdminServiceClient client;
  private final MockServiceHelper mockServiceHelper;

  MockFeatureOnlineStoreAdminServiceClientFactory() throws IOException {
    mockService = new MockFeatureOnlineStoreAdminService();
    mockServiceHelper = new MockServiceHelper(
        UUID.randomUUID().toString(),
        Arrays.<MockGrpcService>asList(mockService));
    mockServiceHelper.start();
    LocalChannelProvider channelProvider = mockServiceHelper.createChannelProvider();
    FeatureOnlineStoreAdminServiceSettings settings =
        FeatureOnlineStoreAdminServiceSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();
     this.client = FeatureOnlineStoreAdminServiceClient.create(settings);
   }

  @After
  public void tearDown() throws Exception {
    mockServiceHelper.stop();
    client.close();
  }

  @Override
  public FeatureOnlineStoreAdminServiceClient createClient(String region) throws IOException {
    return this.client;
  }

  MockFeatureOnlineStoreAdminService getMockService() {
    return this.mockService;
  }
}
