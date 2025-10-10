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
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.UnimplementedException;
import com.google.cloud.aiplatform.fs.FeatureViewInternalStorage.FeatureData;
import com.google.cloud.aiplatform.v1.FeatureValue;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList.FeatureNameValuePair;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Converter util file for converting from Bigtable Row to FetchFeatureValuesRequest proto.
 * All methods are static.
 */
public class Converter {

  private static final Logger logger = Logger.getLogger(Converter.class.getName());

  // default column name.
  private static final String kDefaultColumn = "default";

  // Column name for direct write.
  private static final String  kDirectWriteColumn = "dw";

  // Converts a single RowCell to FeatureViewCell. This is called for direct write or batch sync.
  public static FeatureViewCell populateFeatureViewCellFromBTCell(RowCell cell) throws InvalidProtocolBufferException {
    FeatureViewInternalStorage internalStorage = FeatureViewInternalStorage.parseFrom(cell.getValue());
    return FeatureViewCell.newBuilder()
        .setInternalStorage(internalStorage)
        .setTimestampMs(cell.getTimestamp()/1000)
        .build();
  }

  public static FeatureViewCell rowToFeatureViewCell(Row row, InternalFetchRequest request) throws Exception {
    // When continuous sync is enabled.
    if (request.featureViewSpec.continuousSyncEnabled) {
      throw new UnimplementedException(
          new Throwable("Fetching features written by continuous sync is not supported"),
          /* statusCode= */ GrpcStatusCode.of(Code.UNIMPLEMENTED),
          /* retryable= */ false);
    }

    if (row.getCells().size() > 2) {
      throw new InternalException(
          new Throwable("Unexpected data is returned from bigtable."),
          /* statusCode= */ GrpcStatusCode.of(Code.INTERNAL),
          /* retryable= */ false);
    }

    // When both batch sync and direct write exist.
    if (row.getCells().size() == 2) {
      logger.log(Level.FINE, "Converting Row for both batch and direct write sync");
      int defaultCellIndex = row.getCells().get(0).getQualifier().toString().contains(kDefaultColumn)? 0 : 1;
      RowCell defaultCell = row.getCells().get(defaultCellIndex);
      RowCell directWriteCell = row.getCells().get(1-defaultCellIndex);
      if (!defaultCell.getFamily().equals(request.featureViewId)
          || !defaultCell.getQualifier().toString().contains(kDefaultColumn)){
        throw new InternalException(
            new Throwable("Unexpected data is returned from bigtable."),
            /* statusCode= */ GrpcStatusCode.of(Status.Code.INTERNAL),
            /* retryable= */ false);
      }
      if (!directWriteCell.getFamily().equals(request.featureViewId) || !directWriteCell.getQualifier().toString().contains(kDirectWriteColumn)){
        throw new InternalException(
            new Throwable("Unexpected data is returned from bigtable."),
            /* statusCode= */ GrpcStatusCode.of(Status.Code.INTERNAL),
            /* retryable= */ false);
      }
      RowCell resultCell = defaultCell.getTimestamp() >= directWriteCell.getTimestamp() ? defaultCell : directWriteCell;
      return populateFeatureViewCellFromBTCell(resultCell);
    }

    // Only Direct write column exists.
    if (row.getCells().size() == 1 &&
        row.getCells().get(0).getFamily().equals(request.featureViewId) &&
        row.getCells().get(0).getQualifier().toString().contains(kDirectWriteColumn)) {
      logger.log(Level.FINE, "Converting Row for direct write only");
      return populateFeatureViewCellFromBTCell(row.getCells().get(0));
    }

    // Only Batch Sync column exists.
    if (row.getCells().size() == 1 &&
        row.getCells().get(0).getFamily().equals(request.featureViewId) &&
        row.getCells().get(0).getQualifier().toString().contains(kDefaultColumn)) {
      logger.log(Level.FINE, "Converting Row for batch sync only");
      return populateFeatureViewCellFromBTCell(row.getCells().get(0));
    }

    // Conversion did not happen. Log the request for further investigation.
    throw new InternalException(
        new Throwable(String.format(
            "Unexpected data is returned from bigtable and issue in conversion. Internal request:",
            request)),
        /* statusCode= */ GrpcStatusCode.of(Status.Code.INTERNAL),
        /* retryable= */ false);
  }

  // Converts FeatureViewCell to FeatureNameValuePairList for the keyValue type.
  public static FeatureNameValuePairList internalStorageToKeyValuesList(
      FeatureViewCell cell,
      InternalFetchRequest request) {
    FeatureNameValuePairList.Builder keyValueList = FeatureNameValuePairList.newBuilder();
    for (FeatureData featureData : cell.getInternalStorage().getFeatureDataList()) {
      if (featureData.getValuesList().size() > 1) {
        logger.log(Level.FINE,
            String.format("OnlineStoreId:%s, featureViewId:%s returned multiple feature values",
                request.onlineStoreId, request.featureViewId));
        throw new InternalException(
            new Throwable("Multiple feature values is not yet supported."),
            /* statusCode= */ GrpcStatusCode.of(Status.Code.INTERNAL),
            /* retryable= */ false);
      }
      FeatureNameValuePair.Builder feature = FeatureNameValuePair.newBuilder()
          .setName(featureData.getName());
      if (featureData.getValuesList().isEmpty())
        continue;
      FeatureValue value = featureData.getValuesList().get(0);
      feature.setValue(value);
      keyValueList.addFeatures(feature);
    }
    return keyValueList.build();
  }

  public static FetchFeatureValuesResponse rowToResponse(Row row, InternalFetchRequest request) throws Exception {
    if (request.format == FeatureViewDataFormat.PROTO_STRUCT) {
      // Should not reach here. This is checked at fetchFeatureValues() and returned already.
      throw new UnimplementedException(
          new Throwable("PROTO_STRUCT is not supported"),
          /* statusCode= */ GrpcStatusCode.of(Code.UNIMPLEMENTED),
          /* retryable= */ false);
    }

    // Step 1. Convert from Bigtable Row to FeatureViewCell.
    FeatureViewCell cell = rowToFeatureViewCell(row, request);

    // Step 2. Convert from FeatureViewCell to FetchFeatureValuesResponse.
    FetchFeatureValuesResponse.Builder responseBuilder = FetchFeatureValuesResponse.newBuilder();
    // Processing KEY_VALUE request.
    responseBuilder.setKeyValues(internalStorageToKeyValuesList(cell, request));
    return responseBuilder.build();
  }
}
