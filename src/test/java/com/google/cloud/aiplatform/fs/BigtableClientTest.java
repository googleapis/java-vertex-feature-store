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
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.BigtableMetadata;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.FeatureView.SyncConfig;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BigtableDataClient.class)
public final class BigtableClientTest {
  private static final long PROJECT_NUMBER = 12345L;
  private static final String PROJECT_ID = "1234567";
  private static final String LOCATION = "us-central1";
  private static final String FOS_ID = "my_online_store";
  private static final String FV_ID = "my_feature_view";
  private static final String TENANT_PROJECT_ID = "tenant-project";
  private static final String BIGTABLE_INSTANCE_ID = "instance-id";
  private static final String BIGTABLE_TABLE_ID = "default";

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
              .setAutoScaling(VALID_AUTO_SCALING)
              .setBigtableMetadata(
                BigtableMetadata.newBuilder()
                    .setTenantProjectId(TENANT_PROJECT_ID)
                    .setInstanceId(BIGTABLE_INSTANCE_ID)
                    .setTableId(BIGTABLE_TABLE_ID)
                    .build())
                .build())
          .build();

  private static final FeatureView FEATURE_VIEW = FeatureView.newBuilder().setName(FV_NAME).build();

  private static final Row SAMPLE_ROW =
      Row.create(
          ByteString.copyFromUtf8("key1"),
          ImmutableList.of(
              RowCell.create(
                  "family",
                  ByteString.EMPTY,
                  1000,
                  ImmutableList.<String>of(),
                  ByteString.copyFromUtf8("value"))));

  // Default fetch request for FeatureView which does not use continuous sync.
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

  @Mock private BigtableDataClient mockBigtableClient;

  @Before
  public void setup() throws Exception {
    PowerMockito.mockStatic(BigtableDataClient.class);
    mockBigtableClient = mock(BigtableDataClient.class);
    when(BigtableDataClient.create(any())).thenReturn(mockBigtableClient);
  }

  @Test
  public void fetchData_noContinuousSync_success() throws Exception {
    // Prepare to capture arguments passed to BT.readRow()
    ArgumentCaptor<TableId> tableIdCaptor = forClass(TableId.class);
    ArgumentCaptor<String> rowKeyCaptor = forClass(String.class);
    ArgumentCaptor<Filter> filterCapture = forClass(Filter.class);
    when(mockBigtableClient.readRow(
        tableIdCaptor.capture(), rowKeyCaptor.capture(), filterCapture.capture()))
        .thenReturn(SAMPLE_ROW);
    BigtableClient bigtableClient =
        BigtableClient.create(
            String.format("s", PROJECT_NUMBER), BIGTABLE_INSTANCE_ID, "", "");
    // Execute.
    try {
      Row returnedRow = bigtableClient.fetchData(DEFAULT_REQUEST);
      assertThat(returnedRow).isEqualTo(SAMPLE_ROW);
      // Assert.
      assertThat(TableId.of(BIGTABLE_TABLE_ID)).isEqualTo(tableIdCaptor.getValue());
      assertThat("key1:key2:key3").isEqualTo(rowKeyCaptor.getValue());
      Filter expectedFilter =
          FILTERS
              .chain()
              .filter(
                  FILTERS
                      .qualifier()
                      .rangeWithinFamily(FV_ID)
                      .startClosed("default")
                      .endClosed("dw"))
              .filter(FILTERS.limit().cellsPerColumn(1));
      assertThat(expectedFilter.toProto()).isEqualTo(filterCapture.getValue().toProto());
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown e" + e);
    }
  }

  @Test
  public void fetchData_continuousSync_success() throws Exception {
    // Prepare to capture arguments passed to BT.readRow()
    ArgumentCaptor<TableId> tableIdCaptor = forClass(TableId.class);
    ArgumentCaptor<String> rowKeyCaptor = forClass(String.class);
    ArgumentCaptor<Filter> filterCapture = forClass(Filter.class);
    when(mockBigtableClient.readRow(
        tableIdCaptor.capture(), rowKeyCaptor.capture(), filterCapture.capture()))
        .thenReturn(SAMPLE_ROW);
    InternalFetchRequest request =
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
            .featureViewSpec(
                new FeatureViewSpec(
                    FEATURE_VIEW.toBuilder()
                        .setSyncConfig(SyncConfig.newBuilder().setContinuous(true).build())
                        .build()))
            .build();
    BigtableClient bigtableClient =
        BigtableClient.create(String.format("s", PROJECT_NUMBER), BIGTABLE_INSTANCE_ID, "", "");

    try {
      Row returnedRow = bigtableClient.fetchData(request);
      assertThat(returnedRow).isEqualTo(SAMPLE_ROW);
      // Assert.
      assertThat(TableId.of(BIGTABLE_TABLE_ID)).isEqualTo(tableIdCaptor.getValue());
      assertThat("key1:key2:key3").isEqualTo(rowKeyCaptor.getValue());
      Filter expectedFilter =
          FILTERS
              .chain()
              .filter(FILTERS.family().regex(FV_ID))
              .filter(FILTERS.limit().cellsPerColumn(1));
      assertThat(expectedFilter.toProto()).isEqualTo(filterCapture.getValue().toProto());
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown e" + e);
    }
  }

  @Test
  public void fetchData_rowsNotFound() throws Exception {
    // Prepare
    ArgumentCaptor<TableId> tableIdCaptor = forClass(TableId.class);
    ArgumentCaptor<String> rowKeyCaptor = forClass(String.class);
    ArgumentCaptor<Filter> filterCapture = forClass(Filter.class);
    // When there is no rows to return, BT readRow() returns null
    // https://cloud.google.com/java/docs/reference/google-cloud-bigtable/latest/com.google.cloud.bigtable.data.v2.BigtableDataClient#com_google_cloud_bigtable_data_v2_BigtableDataClient_readRow_com_google_cloud_bigtable_data_v2_models_TargetId_java_lang_String_com_google_cloud_bigtable_data_v2_models_Filters_Filter_
    when(mockBigtableClient.readRow(
        tableIdCaptor.capture(), rowKeyCaptor.capture(), filterCapture.capture()))
        .thenReturn(null);
    BigtableClient bigtableClient =
        BigtableClient.create(String.format("s", PROJECT_NUMBER), BIGTABLE_INSTANCE_ID, "", "");

    try {
      Row returnedRow = bigtableClient.fetchData(DEFAULT_REQUEST);
      assertThat(returnedRow).isEqualTo(null);
      // Assertions
      assertThat(TableId.of(BIGTABLE_TABLE_ID)).isEqualTo(tableIdCaptor.getValue());
      assertThat("key1:key2:key3").isEqualTo(rowKeyCaptor.getValue());
      // Filter for non-continuous sync FeatureView.
      Filter expectedFilter =
          FILTERS
              .chain()
              .filter(
                  FILTERS
                      .qualifier()
                      .rangeWithinFamily(FV_ID)
                      .startClosed("default")
                      .endClosed("dw"))
              .filter(FILTERS.limit().cellsPerColumn(1));
      assertThat(expectedFilter.toProto()).isEqualTo(filterCapture.getValue().toProto());
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown e" + e);
    }
  }

  @Test
  public void fetchData_unexpected_error_resource_exhusted() throws Exception {
    // Prepare
    ArgumentCaptor<TableId> tableIdCaptor = forClass(TableId.class);
    ArgumentCaptor<String> rowKeyCaptor = forClass(String.class);
    ArgumentCaptor<Filter> filterCapture = forClass(Filter.class);
    when(mockBigtableClient.readRow(
        tableIdCaptor.capture(), rowKeyCaptor.capture(), filterCapture.capture()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    BigtableClient bigtableClient =
        BigtableClient.create(String.format("s", PROJECT_NUMBER), "", "", "");
    try {
      bigtableClient.fetchData(DEFAULT_REQUEST);
      Assert.fail("Exception should be thrown but did not");
    } catch (Exception e) {
      // pass
    }
  }
}
