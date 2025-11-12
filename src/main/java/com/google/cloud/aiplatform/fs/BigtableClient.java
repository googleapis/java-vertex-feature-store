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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A client to interact with Bigtable.
 */
public class BigtableClient {

  private final BigtableDataClient bigtableDataClient;
  private static final String defaultColumn = "default";
  private static final String directWriteColumn = "dw";

  public static BigtableClient create(
      String projectId, String instanceId, String accessToken, String appProfile) {
    return new BigtableClient(projectId, instanceId, accessToken, appProfile, Optional.empty());
  }

  public static BigtableClient create(
      String projectId,
      String instanceId,
      String accessToken,
      String appProfile,
      Optional<DirectClientSettings> settings) {
    return new BigtableClient(projectId, instanceId, accessToken, appProfile, settings);
  }

  BigtableClient(
      String projectId,
      String instanceId,
      String accessToken,
      String appProfile,
      Optional<DirectClientSettings> settings) {
    try {
      // Expiration time needs to be null. If set to any time even far ahead in the future, creating
      // Bigtable client fails.
      AccessToken token = new AccessToken(accessToken, null);
      GoogleCredentials credentials = GoogleCredentials.create(token);
      BigtableDataSettings.Builder settingsBuilder;
      if (settings.isPresent()) {
        // Let DirectClientSettings generate a BigtableDataSettings builder and apply all the configs.
        settingsBuilder = settings.get().toBigtableSettingsBuilder();
      } else {
        settingsBuilder = BigtableDataSettings.newBuilder();
      }
      settingsBuilder
              .setProjectId(projectId)
              .setInstanceId(instanceId)
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .setAppProfileId(appProfile)
              // Disable client side metric reporting;
              .setMetricsProvider(NoopMetricsProvider.INSTANCE);

      bigtableDataClient = BigtableDataClient.create(settingsBuilder.build());
    } catch (IOException e) {
      throw new VerifyException(e);
    }
  }

  @VisibleForTesting
    // Constructor used for MockBigtableClient.
  BigtableClient() {
    bigtableDataClient = null;
  }

  public void close() {
    this.bigtableDataClient.close();
  }

  private Filter buildFilter(InternalFetchRequest request) {
    if (request.featureViewSpec.continuousSyncEnabled) {
      return FILTERS
          .chain()
          .filter(FILTERS.family().regex(request.featureViewId))
          .filter(FILTERS.limit().cellsPerColumn(1));
    } else {
      return FILTERS
          .chain()
          .filter(
              FILTERS
                  .qualifier()
                  .rangeWithinFamily(request.featureViewId)
                  .startClosed(defaultColumn)
                  .endClosed(directWriteColumn))
          .filter(FILTERS.limit().cellsPerColumn(1));
    }
  }

  public Row fetchData(InternalFetchRequest request) throws Exception {
    Filter filter = buildFilter(request);
    return bigtableDataClient.readRow(
        TableId.of(request.cloudBigtableSpec.tableId), request.dataKey, filter);
  }

  /**
   * Fetches multiple rows from Bigtable based on the provided InternalFetchRequest.
   * This method assumes that {@code request.dataKeys} is populated.
   *
   * @param request An {@link InternalFetchRequest} with {@code dataKeys} set.
   * @return A {@link List} of {@link Row} objects for the keys found in Bigtable. Keys not found
   *     in Bigtable will not have corresponding entries in the returned list.
   * @throws IllegalArgumentException if {@code request.dataKeys} is null or empty.
   * @throws Exception for other underlying issues during the Bigtable read.
   */
  public List<Row> batchFetchData(InternalFetchRequest request) throws Exception {
    if (request.dataKeys == null || request.dataKeys.isEmpty()) {
      throw new IllegalArgumentException("batchFetchData requires InternalFetchRequest with populated dataKeys.");
    }

    Filter filter = buildFilter(request);
    TableId tableId = TableId.of(request.cloudBigtableSpec.tableId);

    Query query = Query.create(tableId);
    // Assuming request.dataKeys contains unique keys
    Set<String> uniqueDataKeys = new HashSet<>(request.dataKeys);
    for (String dataKey : uniqueDataKeys) {
      query = query.rowKey(dataKey);
    }
    query = query.filter(filter);

    ServerStream<Row> rowStream = bigtableDataClient.readRows(query);

    List<Row> rows = new ArrayList<>();
    Set<String> receivedKeys = new HashSet<>();
    int expectedKeyCount = uniqueDataKeys.size();

    try {
      for (Row row : rowStream) {
        rows.add(row);
        receivedKeys.add(row.getKey().toStringUtf8());

        // Optimization: Exit loop if all expected unique keys have been received.
        if (receivedKeys.size() >= expectedKeyCount) {
          break;
        }
      }
    } finally {
    }

    return rows;
  }
}
