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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.UnimplementedException;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreName;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewName;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.grpc.Status.Code;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/** The client to interact with FeatureOnlineStore. */
public class FeatureOnlineStoreDirectClient {

  Logger logger = Logger.getLogger(FeatureOnlineStoreDirectClient.class.getName());

  private final BigtableClientManager bigtableClientManager;

  // Client library generates access token, and applies the default settings to Bigtable connections.
  public static FeatureOnlineStoreDirectClient create(String featureViewResourceName)
      throws Exception {
    return new FeatureOnlineStoreDirectClient(featureViewResourceName, /* settings= */ Optional.empty());
  }

  // Temporary function to accept accessToken from users. This will be deprecated and the provided
  // access token is no longer used.
  public static FeatureOnlineStoreDirectClient create(
      String featureViewResourceName, String accessToken) throws Exception {
    return new FeatureOnlineStoreDirectClient(featureViewResourceName, /* settings= */ Optional.empty());
  }

  // Client library generates access token, and applies the DirectClientSettings to create
  // Bigtable connections.
  public static FeatureOnlineStoreDirectClient create(
      String featureViewResourceName, DirectClientSettings settings) throws Exception {
    return new FeatureOnlineStoreDirectClient(featureViewResourceName, Optional.of(settings));
  }


  FeatureOnlineStoreDirectClient(String featureViewResourceName, Optional<DirectClientSettings> settings) throws Exception {
    FeatureViewName featureViewName = FeatureViewName.parse(featureViewResourceName);
    String projectIdOrNumber = featureViewName.getProject();
    String locationId = featureViewName.getLocation();
    String onlineStoreId = featureViewName.getFeatureOnlineStore();
    String featureViewId = featureViewName.getFeatureView();
    logger.log(
        Level.INFO,
        String.format(
            "Initializing for: projectID %s, location %s, onlineStore %s, featureView %s",
            projectIdOrNumber, locationId, onlineStoreId, featureViewId));
    String fosName =
        FeatureOnlineStoreName.newBuilder()
            .setProject(projectIdOrNumber)
            .setLocation(locationId)
            .setFeatureOnlineStore(onlineStoreId)
            .build().toString();
    // Call GetFeatureOnlineStore and GetFeatureView APIs and save the metadata in cache.
    CloudBigtableSpec btSpec = CloudBigtableCache.getInstance(settings.map(DirectClientSettings::getCredentialsProvider)).getCloudBigtableSpec(fosName);
    FeatureViewSpec fvSpec = FeatureViewCache.getInstance(settings.map(DirectClientSettings::getCredentialsProvider)).getFeatureViewSpec(featureViewResourceName);
    this.bigtableClientManager = new BigtableClientManager(btSpec, fvSpec, featureViewResourceName, locationId, settings);
  }

  public FetchFeatureValuesResponse fetchFeatureValues(FetchFeatureValuesRequest request) throws Exception {
    if (request.getDataFormat().equals(FeatureViewDataFormat.PROTO_STRUCT)) {
      // PROTO_STRUCT is not supported yet.
      throw new UnimplementedException(
          new Throwable("PROTO_STRUCT is not supported yet"),
          /* statusCode= */ GrpcStatusCode.of(Code.UNIMPLEMENTED),
          /* retryable= */ false);
    }
    InternalFetchRequest internalRequest = new InternalFetchRequest(request);
    Row row = this.bigtableClientManager.getClient().fetchData(internalRequest);
    if (row == null) {
      throw new NotFoundException(
          new Throwable(String.format("Entity id %s is not found", internalRequest.dataKey)),
          /* statusCode= */ GrpcStatusCode.of(Code.NOT_FOUND),
          /* retryable= */ false);
    }
    return Converter.rowToResponse(row, internalRequest);
  }

  public List<FetchFeatureValuesResponse> batchFetchFeatureValues(List<FetchFeatureValuesRequest> requests) throws Exception {
    for (FetchFeatureValuesRequest request : requests) {
      if (request.getDataFormat().equals(FeatureViewDataFormat.PROTO_STRUCT)) {
        throw new UnimplementedException(
            new Throwable("PROTO_STRUCT is not supported yet for batch fetch"),
            /* statusCode= */ GrpcStatusCode.of(Code.UNIMPLEMENTED),
            /* retryable= */ false);
      }
    }

    InternalFetchRequest internalRequest = new InternalFetchRequest(requests);
    List<Row> rows = this.bigtableClientManager.getClient().batchFetchData(internalRequest);
    return Converter.rowsToResponses(rows, internalRequest);
  }

  public void close() {
    this.bigtableClientManager.shutdown();
  }
}
