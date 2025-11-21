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

import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey.CompositeKey;
import com.google.cloud.aiplatform.v1.FeatureViewName;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Internal fetch request class which bundles all the info needed to fetch feature values from
 * Bigtable. Supports both single and batch fetch.
 */
public class InternalFetchRequest {

  long projectNumber;
  String projectId;
  String location;
  String onlineStoreId;
  String featureViewId;

  // Enum field, either KEY_VALUE or PROTO_STRUCT.
  FeatureViewDataFormat format;

  // Time at which the request is received. This is used to compute
  // server-side latency for Cloud monitoring.
  Timestamp arrivalTime;

  // Entity id built from by single id or composite ids. Used for single fetch.
  // Will be null if dataKeys is used.
  String dataKey;

  // Will be empty if dataKey is used.
  ImmutableList<String> dataKeys;

  CloudBigtableSpec cloudBigtableSpec;
  FeatureViewSpec featureViewSpec;

  /**
   * Constructor for a single FetchFeatureValuesRequest.
   */
  public InternalFetchRequest(FetchFeatureValuesRequest request) {
    FeatureViewName featureViewName = FeatureViewName.parse(request.getFeatureView());
    // projectId could be project number in String.
    this.projectId = featureViewName.getProject();
    this.location = featureViewName.getLocation();
    this.onlineStoreId = featureViewName.getFeatureOnlineStore();
    this.featureViewId = featureViewName.getFeatureView();

    this.format = request.getDataFormat();
    Instant now = Instant.now();
    this.arrivalTime = Timestamp.newBuilder()
        .setSeconds(now.getEpochSecond())
        .setNanos(now.getNano())
        .build();
    this.dataKey = constructDataKey(request);
    this.dataKeys = ImmutableList.of(); // Empty for single fetch

    String onlineStoreResourcePath =
        String.format(
            "projects/%s/locations/%s/featureOnlineStores/%s",
            projectId, location, onlineStoreId);
    this.cloudBigtableSpec = CloudBigtableCache.getInstance(Optional.empty()).getCloudBigtableSpec(onlineStoreResourcePath);
    this.featureViewSpec = FeatureViewCache.getInstance(Optional.empty()).getFeatureViewSpec(request.getFeatureView());
  }

  /**
   * Constructor for a batch of FetchFeatureValuesRequests.
   * Assumes all requests in the list target the same FeatureView and have the same DataFormat.
   *
   * @param requests A non-empty list of {@link FetchFeatureValuesRequest}.
   * @throws IllegalArgumentException if the list is null or empty, or if requests
   *         have different FeatureViews or DataFormats.
   */
  public InternalFetchRequest(List<FetchFeatureValuesRequest> requests) {
    if (requests == null || requests.isEmpty()) {
      throw new IllegalArgumentException("Batch fetch requests list cannot be null or empty.");
    }

    FetchFeatureValuesRequest firstRequest = requests.get(0);
    FeatureViewName featureViewName = FeatureViewName.parse(firstRequest.getFeatureView());
    this.projectId = featureViewName.getProject();
    this.location = featureViewName.getLocation();
    this.onlineStoreId = featureViewName.getFeatureOnlineStore();
    this.featureViewId = featureViewName.getFeatureView();
    // Assuming all requests have the same data format.
    this.format = firstRequest.getDataFormat();
    this.arrivalTime = now();
    this.dataKey = null; // Null for batch fetch
    // Construct data keys for each request in the batch.
    this.dataKeys = requests.stream()
        .map(InternalFetchRequest::constructDataKey)
        .collect(ImmutableList.toImmutableList());

    String onlineStoreResourcePath =
        String.format(
            "projects/%s/locations/%s/featureOnlineStores/%s",
            projectId, location, onlineStoreId);
    this.cloudBigtableSpec = CloudBigtableCache.getInstance(Optional.empty()).getCloudBigtableSpec(onlineStoreResourcePath);
    // Assuming all requests have the same feature view spec.
    this.featureViewSpec = FeatureViewCache.getInstance(Optional.empty()).getFeatureViewSpec(firstRequest.getFeatureView());

    validateBatchRequests(requests, firstRequest);
  }

  private static Timestamp now() {
    Instant now = Instant.now();
    return Timestamp.newBuilder()
        .setSeconds(now.getEpochSecond())
        .setNanos(now.getNano())
        .build();
  }

  private void validateBatchRequests(List<FetchFeatureValuesRequest> requests, FetchFeatureValuesRequest firstRequest) {
    String firstFeatureView = firstRequest.getFeatureView();
    for (int i = 1; i < requests.size(); i++) {
      FetchFeatureValuesRequest currentRequest = requests.get(i);
      if (!currentRequest.getDataFormat().equals(this.format)) {
        throw new IllegalArgumentException(
            String.format("All requests in a batch must have the same data format. Found different formats: %s and %s", this.format, currentRequest.getDataFormat()));
      }
      if (!currentRequest.getFeatureView().equals(firstFeatureView)) {
        throw new IllegalArgumentException(
            String.format("All requests in a batch must target the same FeatureView. Found different views: %s and %s", firstFeatureView, currentRequest.getFeatureView()));
      }
    }
  }

  protected static String constructDataKey(FetchFeatureValuesRequest request) {
    if (!request.hasDataKey()) {
      throw new IllegalArgumentException(String.format("No data key provided in request: %s", request));
    }
    FeatureViewDataKey dataKey = request.getDataKey();
    if (!dataKey.hasCompositeKey() && !dataKey.hasKey()) {
      throw new IllegalArgumentException(String.format("No key or composite key provided in request: %s", request));
    }
    if (dataKey.hasKey()) {
      if (dataKey.getKey().isEmpty()) {
        throw new IllegalArgumentException(String.format("Key is empty in request: %s", request));
      }
      return dataKey.getKey();
    }
    // Build composite key ID.
    CompositeKey compositeKey = dataKey.getCompositeKey();
    if (compositeKey.getPartsList().isEmpty()) {
      throw new IllegalArgumentException(String.format("Composite key must have at least one part: %s", request));
    }
    return String.join(":", dataKey.getCompositeKey().getPartsList());
  }

  InternalFetchRequest(Builder builder) {
    this.projectNumber = builder.projectNumber;
    this.projectId = builder.projectId;
    this.location = builder.location;
    this.onlineStoreId = builder.onlineStoreId;
    this.featureViewId = builder.featureViewId;
    this.format = builder.format;
    this.arrivalTime = builder.arrivalTime;

    // Ensure only one of dataKey or dataKeys is set.
    boolean hasSingleKey = builder.dataKey != null;
    ImmutableList<String> builtDataKeys = builder.dataKeysBuilder.build();
    boolean hasBatchKeys = !builtDataKeys.isEmpty();

    if (hasSingleKey && hasBatchKeys) {
      throw new IllegalStateException("Cannot build InternalFetchRequest with both a single dataKey and a list of dataKeys. Use either dataKey() or dataKeys()/addAllDataKeys().");
    }

    this.dataKey = builder.dataKey;
    this.dataKeys = builtDataKeys;

    this.cloudBigtableSpec = builder.cloudBigtableSpec;
    this.featureViewSpec = builder.featureViewSpec;
  }

  public static Builder builder(){
    return new Builder();
  }

  public static class Builder {
    long projectNumber;
    String projectId;
    String location;
    String onlineStoreId;
    String featureViewId;
    FeatureViewDataFormat format;
    Timestamp arrivalTime;

    // Supports setting either a single key or multiple keys.
    String dataKey;
    private ImmutableList.Builder<String> dataKeysBuilder = ImmutableList.builder();

    CloudBigtableSpec cloudBigtableSpec;
    FeatureViewSpec featureViewSpec;

    public Builder projectNumber(long projectNumber) {
      this.projectNumber = projectNumber;
      return this;
    }

    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder location(String location) {
      this.location = location;
      return this;
    }

    public Builder onlineStoreId(String onlineStoreId) {
      this.onlineStoreId = onlineStoreId;
      return this;
    }

    public Builder featureViewId(String featureViewId) {
      this.featureViewId = featureViewId;
      return this;
    }

    public Builder format(FeatureViewDataFormat format) {
      this.format = format;
      return this;
    }

    public Builder arrivalTime(Timestamp arrivalTime) {
      this.arrivalTime = arrivalTime;
      return this;
    }

    /**
     * Sets a single data key for the request. This will clear any previously added batch data keys.
     */
    public Builder dataKey(String dataKey) {
      this.dataKey = dataKey;
      // Clear dataKeysBuilder to ensure only one key type is active.
      this.dataKeysBuilder = ImmutableList.builder();
      return this;
    }

    /**
     * NEW: Sets a list of data keys for a batch request. This will clear any previously set single data key.
     * The provided list is defensively copied.
     */
    public Builder dataKeys(List<String> dataKeys) {
      this.dataKeysBuilder = ImmutableList.<String>builder().addAll(dataKeys);
      // Clear dataKey to ensure only one key type is active.
      this.dataKey = null;
      return this;
    }

    /**
     * Adds a single data key to the batch. Implies a batch request.
     */
    public Builder addDataKey(String dataKey) {
      this.dataKeysBuilder.add(dataKey);
      this.dataKey = null; // Clear single key if batch keys are being added.
      return this;
    }

    /**
     * Adds multiple data keys from an Iterable to the batch. Implies a batch request.
     */
    public Builder addAllDataKeys(Iterable<String> dataKeys) {
      this.dataKeysBuilder.addAll(dataKeys);
      this.dataKey = null; // Clear single key if batch keys are being added.
      return this;
    }

    public Builder cloudBigtableSpec(CloudBigtableSpec cloudBigtableSpec) {
      this.cloudBigtableSpec = cloudBigtableSpec;
      return this;
    }

    public Builder featureViewSpec(FeatureViewSpec featureViewSpec) {
      this.featureViewSpec = featureViewSpec;
      return this;
    }

    public InternalFetchRequest build() {
      return new InternalFetchRequest(this);
    }
  }
}
