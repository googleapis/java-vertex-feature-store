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
import static org.mockito.Mockito.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.api.gax.rpc.UnimplementedException;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.FeatureView.BigtableMetadata;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey.CompositeKey;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudBigtableCache.class,FeatureViewCache.class})
public final class InternalFetchRequestTest {
  private static final String PROJECT_ID = "test-project";
  private static final String LOCATION = "us-central1";
  private static final String FOS_ID = "my_online_store";
  private static final String FV_ID = "my_feature_view";

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
          .setBigtable(Bigtable.newBuilder().setAutoScaling(VALID_AUTO_SCALING).build())
          .build();

  private static final Timestamp NOW = Timestamps.now();

  private static final FeatureView DEFAULT_BIGTABLE_FEATURE_VIEW =
      FeatureView.newBuilder()
          .setName(FV_NAME)
          .setBigtableMetadata(BigtableMetadata.getDefaultInstance())
          .build();

  @Mock
  private CloudBigtableCache mockBigtableCache;

  @Mock
  private FeatureViewCache mockFeatureViewCache;

  @Before
  public void setup() {
    PowerMockito.mockStatic(CloudBigtableCache.class);
    mockBigtableCache = mock(CloudBigtableCache.class);
    when(CloudBigtableCache.getInstance()).thenReturn(mockBigtableCache);

    PowerMockito.mockStatic(FeatureViewCache.class);
    mockFeatureViewCache = mock(FeatureViewCache.class);
    when(FeatureViewCache.getInstance()).thenReturn(mockFeatureViewCache);
  }

  @Test
  public void internalRequest_withKey_success() throws Exception {
    // Mock the response for CloudBigtableCache and FeatureViewCache.
    CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
    when(mockBigtableCache.getCloudBigtableSpec(anyString())).thenReturn(bigtableSpec);
    FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
    when(mockFeatureViewCache.getFeatureViewSpec(anyString())).thenReturn(featureViewSpec);
    // Execute.
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.PROTO_STRUCT)
        .setDataKey(FeatureViewDataKey.newBuilder().setKey("entityId").build())
        .build();
    InternalFetchRequest r = new InternalFetchRequest(request);

    // Assert.
    assertThat(r.format).isEqualTo(FeatureViewDataFormat.PROTO_STRUCT);
    assertThat(r.dataKey).isEqualTo("entityId");
    assertThat(r.cloudBigtableSpec).isEqualTo(bigtableSpec);
    assertThat(r.featureViewSpec).isEqualTo(featureViewSpec);
  }

  @Test
  public void internalRequest_compositeKey_success() throws Exception {
    // Mock the response for CloudBigtableCache and FeatureViewCache.
    CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
    when(mockBigtableCache.getCloudBigtableSpec(anyString())).thenReturn(bigtableSpec);
    FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
    when(mockFeatureViewCache.getFeatureViewSpec(anyString())).thenReturn(featureViewSpec);

    // Execute.
    CompositeKey compositeKey = CompositeKey.newBuilder()
        .addParts("key1")
        .addParts("key2")
        .addParts("key3")
        .build();
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
        .setDataKey(FeatureViewDataKey.newBuilder().setCompositeKey(compositeKey).build())
        .build();
    InternalFetchRequest r = new InternalFetchRequest(request);

    // Assert.
    assertThat(r.format).isEqualTo(FeatureViewDataFormat.KEY_VALUE);
    assertThat(r.dataKey).isEqualTo("key1:key2:key3");
    assertThat(r.cloudBigtableSpec).isEqualTo(bigtableSpec);
    assertThat(r.featureViewSpec).isEqualTo(featureViewSpec);
  }

  @Test
  public void internalRequest_missingDataKey_invalid() throws Exception {
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
        .build();
    try {
      InternalFetchRequest r = new InternalFetchRequest(request);
      Assert.fail("Exception should be thrown but did not");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("No data key provided in request");
    }
  }

  public void internalRequest_emptyKey_invalid() throws Exception {
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
        .setDataKey(FeatureViewDataKey.newBuilder().setKey("").build())
        .build();
    try {
      InternalFetchRequest r = new InternalFetchRequest(request);
      Assert.fail("Exception should be thrown but did not");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("Key is empty in request");
    }
  }

  public void internalRequest_emptyDataKey_invalid() throws Exception {
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
        .setDataKey(FeatureViewDataKey.getDefaultInstance())
        .build();
    try {
      InternalFetchRequest r = new InternalFetchRequest(request);
      Assert.fail("Exception should be thrown but did not");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("No key or composite key provided in request");
    }
  }

  @Test
  public void internalRequest_emptyCompositeKey_invalid() throws Exception {
    CompositeKey compositeKey = CompositeKey.getDefaultInstance();
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
        .setDataKey(FeatureViewDataKey.newBuilder().setCompositeKey(compositeKey).build())
        .build();
    try {
      InternalFetchRequest r = new InternalFetchRequest(request);
      Assert.fail("Exception should be thrown but did not");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("Composite key must have at least one part");
    }
  }

  @Test
  public void internalRequest_protoStruct_unimplemented() throws Exception {
    FetchFeatureValuesRequest request = FetchFeatureValuesRequest.newBuilder()
        .setFeatureView(FV_NAME)
        .setDataFormat(FeatureViewDataFormat.PROTO_STRUCT)
        .setDataKey(FeatureViewDataKey.newBuilder().setKey("entityId").build())
        .build();
    InternalFetchRequest internalRequest = new InternalFetchRequest(request);
    Row row = Row.create(ByteString.copyFromUtf8("key"), ImmutableList.of());
    try {
      Converter.rowToResponse(row, internalRequest);
      Assert.fail("Exception should be thrown but did not");
    } catch (UnimplementedException e) {
      assertThat(e.getMessage()).contains("PROTO_STRUCT is not supported");
    }
  }
}
