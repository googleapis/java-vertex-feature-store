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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.aiplatform.fs.FeatureViewInternalStorage.FeatureData;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.BigtableMetadata;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceSettings;
import com.google.cloud.aiplatform.v1.FeatureValue;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse.FeatureNameValuePairList.FeatureNameValuePair;
import com.google.cloud.aiplatform.v1.GenerateFetchAccessTokenResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Unit tests for {@link FeatureOnlineStoreDirectClient}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudBigtableCache.class,FeatureViewCache.class,BigtableDataClient.class, FeatureOnlineStoreServiceClient.class})
public final class FeatureOnlineStoreDirectClientTest {

  private static final Timestamp NOW = Timestamps.now();
  private static final String PROJECT_ID = "test-project";
  private static final String LOCATION = "us-central1";
  private static final String FOS_ID = "my_online_store";
  private static final String FV_ID = "my_feature_view";
  private static final String TENANT_PROJECT_ID = "tenant-project";
  private static final String BIGTABLE_INSTANCE_ID = "instance-id";
  private static final String BIGTABLE_TABLE_ID = "default";
  private static final String READ_PROFILE_ID = "read-3115404624652140544";
  private static final String ACCESS_TOKEN = "ABCDEFG";

  private static final String FOS_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s",
          PROJECT_ID, LOCATION, FOS_ID);

  private static final String FV_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s/featureViews/%s",
          PROJECT_ID, LOCATION, FOS_ID, FV_ID);

  private static final AutoScaling VALID_AUTO_SCALING =
      AutoScaling.newBuilder()
          .setCpuUtilizationTarget(50)
          .setMaxNodeCount(30)
          .setMinNodeCount(1)
          .build();

  private static final FeatureOnlineStore BIGTABLE_FEATURE_ONLINE_STORE =
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

  private static final FeatureView DEFAULT_BIGTABLE_FEATURE_VIEW =
      FeatureView.newBuilder()
          .setName(FV_NAME)
          .setBigtableMetadata(FeatureView.BigtableMetadata.newBuilder().setReadAppProfile(READ_PROFILE_ID).build())
          .build();

  private static final FeatureViewInternalStorage SAMPLE_INTERNALSTORAGE =
      FeatureViewInternalStorage.newBuilder()
          .setFeatureTimestamp(NOW)
          .addFeatureData(FeatureData.newBuilder()
              .setName("test")
              .addValues(FeatureValue.newBuilder().setStringValue("sample feature value").build())
              .build())
          .build();

  private static final RowCell SAMPLE_CELL_DEFAULT = RowCell.create(
      FV_ID,
      ByteString.copyFromUtf8("default"),
      1000000,
      ImmutableList.of(),
      SAMPLE_INTERNALSTORAGE.toByteString());

  private static final Row SAMPLE_ROW =
      Row.create(
          ByteString.copyFromUtf8("key"),
          ImmutableList.of(SAMPLE_CELL_DEFAULT));

  private static final FeatureViewInternalStorage SAMPLE_INTERNALSTORAGE_2 =
      FeatureViewInternalStorage.newBuilder()
          .setFeatureTimestamp(NOW)
          .addFeatureData(FeatureData.newBuilder()
              .setName("test")
              .addValues(FeatureValue.newBuilder().setStringValue("sample feature value 2").build())
              .build())
          .build();

  private static final RowCell SAMPLE_CELL_DEFAULT_2 = RowCell.create(
      FV_ID,
      ByteString.copyFromUtf8("default"),
      1000000,
      ImmutableList.of(),
      SAMPLE_INTERNALSTORAGE_2.toByteString());

  private static final Row SAMPLE_ROW_2 =
      Row.create(
          ByteString.copyFromUtf8("key_2"),
          ImmutableList.of(SAMPLE_CELL_DEFAULT_2));

  @Mock
  private CloudBigtableCache mockBigtableCache;

  @Mock
  private FeatureViewCache mockFeatureViewCache;

  @Mock
  private BigtableDataClient mockBigtableClient;

  @Mock private FeatureOnlineStoreServiceClient mockOnlineStoreServiceClient;

  @Before
  public void setup() throws Exception {
    PowerMockito.mockStatic(CloudBigtableCache.class);
    mockBigtableCache = mock(CloudBigtableCache.class);
    when(CloudBigtableCache.getInstance()).thenReturn(mockBigtableCache);

    PowerMockito.mockStatic(FeatureViewCache.class);
    mockFeatureViewCache = mock(FeatureViewCache.class);
    when(FeatureViewCache.getInstance()).thenReturn(mockFeatureViewCache);

    PowerMockito.mockStatic(BigtableDataClient.class);
    mockBigtableClient = mock(BigtableDataClient.class);
    when(BigtableDataClient.create(any())).thenReturn(mockBigtableClient);

    // Set up default response for GetFOS and GetFV APIs.
    CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
    when(mockBigtableCache.getCloudBigtableSpec(anyString())).thenReturn(bigtableSpec);
    FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
    when(mockFeatureViewCache.getFeatureViewSpec(anyString())).thenReturn(featureViewSpec);

    // Mock for online store server and generating access token
    PowerMockito.mockStatic(FeatureOnlineStoreServiceClient.class);
    mockOnlineStoreServiceClient = mock(FeatureOnlineStoreServiceClient.class);
    when(FeatureOnlineStoreServiceClient.create(any(FeatureOnlineStoreServiceSettings.class)))
        .thenReturn(mockOnlineStoreServiceClient);
    GenerateFetchAccessTokenResponse response = GenerateFetchAccessTokenResponse.newBuilder()
        .setAccessToken(ACCESS_TOKEN)
        .build();
    when(mockOnlineStoreServiceClient.generateFetchAccessToken(any())).thenReturn(response);
  }

  @Test
  public void fetchFeatureValues_keyValue_success() {
    // Set the response for Bigtable.readRow API.
    when(mockBigtableClient.readRow(any(TableId.class), anyString(), any(Filter.class)))
        .thenReturn(SAMPLE_ROW);
    FetchFeatureValuesResponse expected = FetchFeatureValuesResponse.newBuilder()
        .setKeyValues(
            FeatureNameValuePairList.newBuilder()
                .addFeatures(
                    FeatureNameValuePair.newBuilder()
                        .setName("test")
                        .setValue(FeatureValue.newBuilder().setStringValue("sample feature value").build())
                        .build())
                .build())
        .build();

    try {
      FeatureOnlineStoreDirectClient client =
          FeatureOnlineStoreDirectClient.create(FV_NAME, "");

      FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
          .setFeatureView(FV_NAME)
          .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
          .setDataKey(FeatureViewDataKey.newBuilder().setKey("entityId").build())
          .build();
      // This code triggers loading cache from CloudBigtableCache and FeatureViewCache.
      FetchFeatureValuesResponse response = client.fetchFeatureValues(request);
      assertThat(response).isEqualTo(expected);
    } catch (Exception e) {
      Assert.fail(String.format("Exception should not be thrown but did. %s", e));
    }
  }

  @Test
  public void fetchFeatureValues_protoStruct_fail() {
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.PROTO_STRUCT)
        .setDataKey(FeatureViewDataKey.newBuilder().setKey("entityId").build())
        .build();
    try {
      FeatureOnlineStoreDirectClient client =
          FeatureOnlineStoreDirectClient.create(FV_NAME, "");
      client.fetchFeatureValues(request);
      Assert.fail("Exception should be thrown");
    } catch (Exception e) {
      // pass
    }
  }

  @Test
  public void batchFetchFeatureValues_keyValue_success() {
    // Set the response for Bigtable.readRows API.
    List<Row> expectedRows = Arrays.asList(SAMPLE_ROW, SAMPLE_ROW_2);
    ServerStream<Row> mockRowStream = org.mockito.Mockito.mock(ServerStream.class);
    org.mockito.Mockito.when(mockRowStream.iterator()).thenReturn(expectedRows.iterator());

    // Stub the readRows method to return the mocked stream
    when(mockBigtableClient.readRows(any(Query.class))).thenReturn(mockRowStream);

    List<FetchFeatureValuesResponse> expectedResponses = new ArrayList<>();
    expectedResponses.add(FetchFeatureValuesResponse.newBuilder()
        .setKeyValues(
            FeatureNameValuePairList.newBuilder()
                .addFeatures(
                    FeatureNameValuePair.newBuilder()
                        .setName("test")
                        .setValue(FeatureValue.newBuilder().setStringValue("sample feature value").build())
                        .build())
                .build())
        .build());
    expectedResponses.add(FetchFeatureValuesResponse.newBuilder()
        .setKeyValues(
            FeatureNameValuePairList.newBuilder()
                .addFeatures(
                    FeatureNameValuePair.newBuilder()
                        .setName("test")
                        .setValue(FeatureValue.newBuilder().setStringValue("sample feature value 2").build())
                        .build())
                .build())
        .build());

    try {
      FeatureOnlineStoreDirectClient client =
          FeatureOnlineStoreDirectClient.create(FV_NAME, "");

      List<FetchFeatureValuesRequest> requests = new ArrayList<>();
      requests.add(FetchFeatureValuesRequest.newBuilder()
          .setFeatureView(FV_NAME)
          .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
          .setDataKey(FeatureViewDataKey.newBuilder().setKey("key").build())
          .build());
      requests.add(FetchFeatureValuesRequest.newBuilder()
          .setFeatureView(FV_NAME)
          .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
          .setDataKey(FeatureViewDataKey.newBuilder().setKey("key_2").build())
          .build());
      // This code triggers loading cache from CloudBigtableCache and FeatureViewCache.
      List<FetchFeatureValuesResponse> actualResponses = client.batchFetchFeatureValues(requests);
      assertThat(actualResponses).containsExactlyElementsIn(expectedResponses).inOrder();
    } catch (Exception e) {
      Assert.fail(String.format("Exception should not be thrown but did. %s", e));
    }
  }

  @Test
  public void testBigtableConnectionWithSettings() {
    // Prepare
    // Set the response for Bigtable.readRow API.
    when(mockBigtableClient.readRow(any(TableId.class), anyString(), any(Filter.class)))
        .thenReturn(SAMPLE_ROW);

    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelayDuration(Duration.ofSeconds(1))
            .setMaxAttempts(10)
            .setMaxRetryDelayDuration(Duration.ofSeconds(5))
            .build();
    FetchFeatureValuesRequest request =
        FetchFeatureValuesRequest.newBuilder()
            .setFeatureView(FV_NAME)
            .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
            .setDataKey(FeatureViewDataKey.newBuilder().setKey("entityId").build())
            .build();
    try {
      // Execute
      DirectClientSettings settings =
          new DirectClientSettings.Builder().setRetrySettings(retrySettings).build();
      FeatureOnlineStoreDirectClient client =
          FeatureOnlineStoreDirectClient.create(FV_NAME, settings);
      client.fetchFeatureValues(request);
      // pass
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown");
    }
  }
}
