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

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceSettings;
import com.google.cloud.aiplatform.v1.GenerateFetchAccessTokenRequest;
import com.google.cloud.aiplatform.v1.GenerateFetchAccessTokenResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BigtableClientManager {

  Logger logger = Logger.getLogger(FeatureOnlineStoreDirectClient.class.getName());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final AtomicReference<BigtableClient> clientRef = new AtomicReference<BigtableClient>();

  private final String projectId;
  private final String instanceId;
  private final String featureView;
  private final String location;
  private final String appProfile;

  private final Optional<DirectClientSettings> settings;

  public BigtableClientManager(
      CloudBigtableSpec btSpec,
      FeatureViewSpec fvSpec,
      String featureView,
      String location,
      Optional<DirectClientSettings> settings) {
    this.projectId = btSpec.tenantProjectId;
    this.instanceId = btSpec.instanceId;
    this.featureView = featureView;
    this.location = location;
    this.appProfile = fvSpec.readAppProfileId;
    this.settings = settings;
    createConnection();

    // Schedule the refresh every 45 minutes
    scheduler.scheduleAtFixedRate(this::createConnection, 45, 45, TimeUnit.MINUTES);
  }
  private void createConnection() {
    logger.log(Level.FINE, "Refreshing Bigtable connection...");
    // Generate access token
    String accessToken = null;
    try {
      accessToken = generateAccessToken();
    } catch (Exception e) {
      if (e instanceof NotFoundException) {
        throw (NotFoundException) e;
      } else if (e instanceof PermissionDeniedException) {
        throw (PermissionDeniedException) e;
      } else {
        logger.log(Level.SEVERE, String.format("Generating access token failed: %s", e));
        throw new RuntimeException(e);
      }
    }
    // Create a new Bigtable client and close old client.
    BigtableClient newClient =
        BigtableClient.create(this.projectId, this.instanceId, accessToken, appProfile, settings);
    BigtableClient oldClient = clientRef.getAndSet(newClient);
    if (oldClient != null) {
      CompletableFuture.runAsync(() -> {
        try {
          Thread.sleep(10000); // Wait for 10 sec before closing old connection.
          oldClient.close();
          logger.log(Level.FINE, "Old Bigtable connection closed.");
        } catch (Exception e) {
          logger.log(Level.FINE, "Error closing old connection: " + e.getMessage());
        }
      });
    }
  }

  private String generateAccessToken() throws Exception {
    String endpoint = String.format("%s-aiplatform.googleapis.com:443", location);
    FeatureOnlineStoreServiceSettings setting = FeatureOnlineStoreServiceSettings
        .newBuilder()
        .setEndpoint(endpoint)
        .build();
    FeatureOnlineStoreServiceClient onlineServiceClient = FeatureOnlineStoreServiceClient.create(setting);
    GenerateFetchAccessTokenRequest request =
        GenerateFetchAccessTokenRequest.newBuilder()
            .setFeatureView(featureView)
            .build();
    GenerateFetchAccessTokenResponse response =
        onlineServiceClient.generateFetchAccessToken(request);
    return response.getAccessToken();
  }

  public BigtableClient getClient() {
    return clientRef.get();
  }

  public void shutdown() {
    scheduler.shutdown();
    BigtableClient client = clientRef.getAndSet(null);
    if (client != null) {
      client.close();
    }
    logger.log(Level.INFO,"Bigtable connection and scheduler shut down.");
  }
}
