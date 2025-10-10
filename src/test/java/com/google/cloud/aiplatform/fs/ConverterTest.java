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

import com.google.api.gax.rpc.InternalException;
import com.google.cloud.aiplatform.fs.FeatureViewInternalStorage.FeatureData;
import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStore.Bigtable.BigtableMetadata;
import com.google.cloud.aiplatform.v1beta1.FeatureValue;
import com.google.cloud.aiplatform.v1beta1.FeatureView;
import com.google.cloud.aiplatform.v1beta1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1beta1.FetchFeatureValuesResponse;
import com.google.cloud.aiplatform.v1beta1.FetchFeatureValuesResponse.FeatureNameValuePairList;
import com.google.cloud.aiplatform.v1beta1.FetchFeatureValuesResponse.FeatureNameValuePairList.FeatureNameValuePair;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class ConverterTest {
  private static final long PROJECT_NUMBER = 12345L;
  private static final String LOCATION = "us-central1";
  private static final String FOS_ID = "my_online_store";
  private static final String FV_ID = "my_feature_view";
  private static final String TENANT_PROJECT_ID = "tenant-project";
  private static final String BIGTABLE_INSTANCE_ID = "instance-id";
  private static final String BIGTABLE_TABLE_ID = "default";
  private static final String READ_PROFILE_ID = "read-3115404624652140544";
  private static final String FOS_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s", PROJECT_NUMBER, LOCATION, FOS_ID);
  private static final String FV_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s/featureViews/%s",
          PROJECT_NUMBER, LOCATION, FOS_ID, FV_ID);

  private static final AutoScaling VALID_AUTO_SCALING =
      AutoScaling.newBuilder()
          .setCpuUtilizationTarget(50)
          .setMaxNodeCount(30)
          .setMinNodeCount(1)
          .build();

  private static final FeatureOnlineStore FEATURE_ONLINE_STORE =
      FeatureOnlineStore.newBuilder()
          .setName(FOS_NAME)
          .setBigtable(Bigtable.newBuilder()
              .setBigtableMetadata(
                  BigtableMetadata.newBuilder()
                      .setTenantProjectId(TENANT_PROJECT_ID)
                      .setInstanceId(BIGTABLE_INSTANCE_ID)
                      .setTableId(BIGTABLE_TABLE_ID)
                      .build())
              .setAutoScaling(VALID_AUTO_SCALING).build())
          .build();

  private static final FeatureView FEATURE_VIEW = FeatureView.newBuilder()
      .setName(FV_NAME)
      .setBigtableMetadata(FeatureView.BigtableMetadata.newBuilder().setReadAppProfile(READ_PROFILE_ID).build()).build();

  private static final InternalFetchRequest DEFAULT_REQUEST =
      InternalFetchRequest.builder()
          .projectNumber(PROJECT_NUMBER)
          .projectId(TENANT_PROJECT_ID)
          .location(LOCATION)
          .onlineStoreId(FOS_ID)
          .featureViewId(FV_ID)
          .format(FeatureViewDataFormat.KEY_VALUE)
          .arrivalTime(Timestamp.newBuilder().setSeconds(1735684800L).build())
          .dataKey("key1:key2:key3")
          .cloudBigtableSpec(new CloudBigtableSpec(FEATURE_ONLINE_STORE))
          .featureViewSpec(new FeatureViewSpec(FEATURE_VIEW))
          .build();


  private static final Timestamp NOW = Timestamps.now();
  private static final FeatureViewInternalStorage SAMPLE_INTERNALSTORAGE =
      FeatureViewInternalStorage.newBuilder()
      .setFeatureTimestamp(NOW)
      .addFeatureData(FeatureData.newBuilder()
          .setName("test")
          .addValues(FeatureValue.newBuilder().setStringValue("sample feature value").build())
          .build())
      .build();

  private static final FeatureViewInternalStorage SAMPLE_INTERNALSTORAGE2 =
      FeatureViewInternalStorage.newBuilder()
          .setFeatureTimestamp(NOW)
          .addFeatureData(FeatureData.newBuilder()
              .setName("test")
              .addValues(FeatureValue.newBuilder().setStringValue("updated feature value").build())
              .build())
          .build();

  private static final RowCell SAMPLE_CELL_DEFAULT = RowCell.create(
      FV_ID,
      ByteString.copyFromUtf8("default"),
      1000000,
      ImmutableList.of(),
      SAMPLE_INTERNALSTORAGE.toByteString());

  private static final RowCell SAMPLE_CELL_DR = RowCell.create(
      FV_ID,
      ByteString.copyFromUtf8("dw"),
      1000000,
      ImmutableList.of(),
      SAMPLE_INTERNALSTORAGE.toByteString());

  @Test
  public void singleRowCellToFeatureView_success() throws Exception {
    // Sample test
    // https://source.corp.google.com/piper///depot/google3/cloud/ml/featurestore/thunder/storage/cloud_bigtable_online_storage_single_row_test.cc;l=60
    FeatureViewCell returnedCell = Converter.populateFeatureViewCellFromBTCell(SAMPLE_CELL_DEFAULT);
    assertThat(returnedCell.getInternalStorage()).isEqualTo(SAMPLE_INTERNALSTORAGE);
    assertThat(returnedCell.getTimestampMs()).isEqualTo(1000);
  }

  @Test
  public void singleRowToFeatureView_batchSync_success() throws Exception {
    Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of(SAMPLE_CELL_DEFAULT));
    FeatureViewCell returnedCell = Converter.rowToFeatureViewCell(row, DEFAULT_REQUEST);
    assertThat(returnedCell.getInternalStorage()).isEqualTo(SAMPLE_INTERNALSTORAGE);
    assertThat(returnedCell.getTimestampMs()).isEqualTo(1000);
  }

  @Test
  public void singleRowToFeatureView_directwrite_success() throws Exception {
    Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of(SAMPLE_CELL_DR));
    FeatureViewCell returnedCell = Converter.rowToFeatureViewCell(row, DEFAULT_REQUEST);
    assertThat(returnedCell.getInternalStorage()).isEqualTo(SAMPLE_INTERNALSTORAGE);
    assertThat(returnedCell.getTimestampMs()).isEqualTo(1000);
  }

  @Test
  public void twoRowsToFeatureView_directCellNewer_success() throws Exception {
    RowCell directWriteCell = RowCell.create(
        FV_ID,
        ByteString.copyFromUtf8("dw"),
        5000000,
        ImmutableList.of(),
        SAMPLE_INTERNALSTORAGE2.toByteString());
    Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of(SAMPLE_CELL_DEFAULT, directWriteCell));
    FeatureViewCell returnedCell = Converter.rowToFeatureViewCell(row, DEFAULT_REQUEST);
    assertThat(returnedCell.getInternalStorage()).isEqualTo(SAMPLE_INTERNALSTORAGE2);
    assertThat(returnedCell.getTimestampMs()).isEqualTo(5000);
  }

  @Test
  public void threeRowsToFeatureViewCell_error() throws Exception {
    Row row = Row.create(
        ByteString.copyFromUtf8("key"),
        ImmutableList.of(SAMPLE_CELL_DEFAULT, SAMPLE_CELL_DEFAULT, SAMPLE_CELL_DR));
    try {
      Converter.rowToFeatureViewCell(row, DEFAULT_REQUEST);
      Assert.fail("Exception should be thrown");
    } catch (InternalException e) {
      // pass
    }
  }

  @Test
  public void wrongColumnFamily_error() throws Exception {
    RowCell directWriteCell = RowCell.create(
        "wrong-column-family",
        ByteString.copyFromUtf8("dw"),
        5000000,
        ImmutableList.of(),
        SAMPLE_INTERNALSTORAGE2.toByteString());
    Row row = Row.create(
        ByteString.copyFromUtf8("key"),
        ImmutableList.of(directWriteCell));
    try {
      FeatureViewCell returnedCell = Converter.rowToFeatureViewCell(row, DEFAULT_REQUEST);
      Assert.fail("Exception should be thrown");
    } catch (InternalException e) {
      // pass
    }
  }

  @Test
  public void internalStorageToKeyValuesList_success() {
    FeatureViewInternalStorage storage = FeatureViewInternalStorage.newBuilder()
        .addFeatureData(FeatureData.newBuilder()
            .setName("test")
            .addValues(FeatureValue.newBuilder()
                .setStringValue("sample return value")
                .build())
            .build())
        .build();
    FeatureViewCell cell = FeatureViewCell.newBuilder()
        .setInternalStorage(storage)
        .setTimestampMs(1000000)
        .build();
    FeatureNameValuePairList actual = Converter.internalStorageToKeyValuesList(cell, DEFAULT_REQUEST);
    assertThat(actual).isEqualTo(
        FeatureNameValuePairList.newBuilder()
        .addFeatures(FeatureNameValuePair.newBuilder()
            .setName("test")
            .setValue(FeatureValue.newBuilder().setStringValue("sample return value").build())
            .build())
        .build());
  }

  @Test
  public void internalStorageToKeyValuesList_multipleFeatureValues_fail() {
    FeatureViewInternalStorage storage = FeatureViewInternalStorage.newBuilder()
        .addFeatureData(FeatureData.newBuilder()
            .setName("test")
            .addValues(FeatureValue.newBuilder()
                .setStringValue("sample return value")
                .build())
            .addValues(FeatureValue.newBuilder()
                .setStringValue("second  value")
                .build())
            .build())
        .build();
    FeatureViewCell cell = FeatureViewCell.newBuilder()
        .setInternalStorage(storage)
        .setTimestampMs(1000000)
        .build();
    try {
      Converter.internalStorageToKeyValuesList(cell, DEFAULT_REQUEST);
      Assert.fail("Exception should not be thrown");
    }  catch (Exception e) {
      // pass
    }
  }

  @Test
  public void rowToKeyValueResponse_keyValueE2e_success() throws Exception {
    Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of(SAMPLE_CELL_DEFAULT));
    FeatureNameValuePairList list = FeatureNameValuePairList.newBuilder()
        .addFeatures(FeatureNameValuePair.newBuilder()
            .setName("test")
            .setValue(FeatureValue.newBuilder().setStringValue("sample feature value").build())
            .build())
        .build();
    try {
      FetchFeatureValuesResponse response = Converter.rowToResponse(row, DEFAULT_REQUEST);
      assertThat(response).isEqualTo(FetchFeatureValuesResponse.newBuilder()
          .setKeyValues(list).build());
    } catch (InternalException e) {
      Assert.fail(String.format("Exception should not be thrown but did: %s", e));
    }
  }

  @Test
    public void rowToKeyValueResponse_protoStructE2e_fail() throws Exception {
      InternalFetchRequest protoRequest = InternalFetchRequest.builder()
          .projectNumber(PROJECT_NUMBER)
          .projectId(TENANT_PROJECT_ID)
          .location(LOCATION)
          .onlineStoreId(FOS_ID)
          .featureViewId(FV_ID)
          .format(FeatureViewDataFormat.PROTO_STRUCT)
          .arrivalTime(Timestamp.newBuilder().setSeconds(1735684800L).build())
          .dataKey("key1:key2:key3")
          .cloudBigtableSpec(new CloudBigtableSpec(FEATURE_ONLINE_STORE))
          .featureViewSpec(new FeatureViewSpec(FEATURE_VIEW))
          .build();;

      Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of(SAMPLE_CELL_DEFAULT));
      FeatureNameValuePairList list  =FeatureNameValuePairList.newBuilder()
          .addFeatures(FeatureNameValuePair.newBuilder()
              .setName("test")
              .setValue(FeatureValue.newBuilder().setStringValue("sample feature value").build())
              .build())
          .build();
      try {
        Converter.rowToResponse(row, protoRequest);
        Assert.fail("Exception should not be thrown");
      } catch (Exception e) {
        // pass
      }
    }
}
