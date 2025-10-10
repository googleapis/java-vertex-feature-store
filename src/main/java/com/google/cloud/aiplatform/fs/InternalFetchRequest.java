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

import com.google.cloud.aiplatform.v1beta1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1beta1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1beta1.FeatureViewDataKey.CompositeKey;
import com.google.cloud.aiplatform.v1beta1.FeatureViewName;
import com.google.cloud.aiplatform.v1beta1.FetchFeatureValuesRequest;
import com.google.protobuf.Timestamp;
import java.time.Instant;

/**
 * Internal fetch request class which bundles all the info needed to fetch feature values from
 * Bigtable.
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

  // Entity id built from by single id or composite ids.
  String dataKey;

  CloudBigtableSpec cloudBigtableSpec;
  FeatureViewSpec featureViewSpec;

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
    String onlineStoreResourcePath =
        String.format(
            "projects/%s/locations/%s/featureOnlineStores/%s",
            projectId, location, onlineStoreId);
    this.cloudBigtableSpec = CloudBigtableCache.getInstance().getCloudBigtableSpec(onlineStoreResourcePath);
    this.featureViewSpec = FeatureViewCache.getInstance().getFeatureViewSpec(request.getFeatureView());
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
    this.dataKey = builder.dataKey;
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
    String dataKey;

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

    public Builder dataKey(String dataKey) {
      this.dataKey = dataKey;
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