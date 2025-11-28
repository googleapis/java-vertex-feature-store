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

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceSettings;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

// Factory class for FeatureOnlineStoreAdminServiceClient.
interface FeatureOnlineStoreAdminServiceClientFactory {

  static final String ENDPOINT = "aiplatform.googleapis.com:443";

  FeatureOnlineStoreAdminServiceClient createClient(String region) throws IOException;
}

// Default factory class. It receives region at runtime and creates a client to access the real
// admin endpoint.
class DefaultFeatureOnlineStoreAdminServiceClientFactory implements FeatureOnlineStoreAdminServiceClientFactory {
  Logger logger = Logger.getLogger(DefaultFeatureOnlineStoreAdminServiceClientFactory.class.getName());
    private final CredentialsProvider credentialsProvider;

    public DefaultFeatureOnlineStoreAdminServiceClientFactory() {
        this.credentialsProvider = null;
    }

    public DefaultFeatureOnlineStoreAdminServiceClientFactory(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
  @Override
  public FeatureOnlineStoreAdminServiceClient createClient(String region) throws IOException {
    String regionalEndpoint = String.format("%s-%s", region, ENDPOINT);
    if (this.credentialsProvider != null) {
        FeatureOnlineStoreAdminServiceSettings settings =
            FeatureOnlineStoreAdminServiceSettings.newBuilder()
                .setCredentialsProvider(this.credentialsProvider)
                .setEndpoint(regionalEndpoint)
                .build();
        logger.log(Level.INFO, String.format("Connecting to: %s", regionalEndpoint));
        return FeatureOnlineStoreAdminServiceClient.create(settings);
    }
    FeatureOnlineStoreAdminServiceSettings settings =
        FeatureOnlineStoreAdminServiceSettings.newBuilder().setEndpoint(regionalEndpoint).build();
    logger.log(Level.INFO, String.format("Connecting to: %s", regionalEndpoint));
    return FeatureOnlineStoreAdminServiceClient.create(settings);
  }
}
