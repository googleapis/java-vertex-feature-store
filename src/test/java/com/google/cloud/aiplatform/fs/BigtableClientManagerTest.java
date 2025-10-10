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
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.BigtableMetadata;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreServiceSettings;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.GenerateFetchAccessTokenResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.util.Optional;
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
@PrepareForTest({BigtableDataClient.class, FeatureOnlineStoreServiceClient.class})
public class BigtableClientManagerTest {

  private static final long PROJECT_NUMBER = 12345L;
  private static final String LOCATION = "us-central1";
  private static final String FOS_ID = "my_online_store";
  private static final String FV_ID = "my_feature_view";
  private static final String TENANT_PROJECT_ID = "tenant-project";
  private static final String BIGTABLE_INSTANCE_ID = "instance-id";
  private static final String BIGTABLE_TABLE_ID = "default";
  private static final String READ_APP_PROFILE = "read-app-profile";
  private static final String ACCESS_TOKEN = "ABCDEFG";
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

  private static final FeatureOnlineStore BIGTABLE_FEATURE_ONLINE_STORE =
      FeatureOnlineStore.newBuilder()
          .setName(FOS_NAME)
          .setBigtable(
              Bigtable.newBuilder()
              .setAutoScaling(VALID_AUTO_SCALING)
              .setBigtableMetadata(BigtableMetadata.newBuilder()
                  .setTenantProjectId(TENANT_PROJECT_ID)
                  .setInstanceId(BIGTABLE_INSTANCE_ID)
                  .setTableId(BIGTABLE_TABLE_ID)
                  .build())
              .build())
          .build();

  private static final FeatureView DEFAULT_BIGTABLE_FEATURE_VIEW =
      FeatureView.newBuilder()
          .setName(FV_NAME)
          .setBigtableMetadata(FeatureView.BigtableMetadata
              .newBuilder()
              .setReadAppProfile(READ_APP_PROFILE)
              .build())
          .build();

  private static final GenerateFetchAccessTokenResponse ACCESS_TOKEN_RESPONSE =
      GenerateFetchAccessTokenResponse.newBuilder()
          .setAccessToken(ACCESS_TOKEN)
          .build();

  @Mock private BigtableDataClient mockBigtableClient;
  @Mock private FeatureOnlineStoreServiceClient mockOnlineStoreServiceClient;

  @Before
  public void setup() throws Exception {
    PowerMockito.mockStatic(BigtableDataClient.class);
    mockBigtableClient = mock(BigtableDataClient.class);

    PowerMockito.mockStatic(FeatureOnlineStoreServiceClient.class);
    mockOnlineStoreServiceClient = mock(FeatureOnlineStoreServiceClient.class);
    when(FeatureOnlineStoreServiceClient.create(any(FeatureOnlineStoreServiceSettings.class)))
        .thenReturn(mockOnlineStoreServiceClient);
  }

  @Test
  public void createBigtableClient_success() throws Exception {
    ArgumentCaptor<BigtableDataSettings> clientSetting = forClass(BigtableDataSettings.class);
    CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
    FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
    when(BigtableDataClient.create(clientSetting.capture())).thenReturn(mockBigtableClient);
    when(mockOnlineStoreServiceClient.generateFetchAccessToken(any()))
        .thenReturn(ACCESS_TOKEN_RESPONSE);
    new BigtableClientManager(bigtableSpec, featureViewSpec, FV_NAME, LOCATION, Optional.empty());
    // check project, instance, app-profile are properly passed to create Bigtable instance.
    BigtableDataSettings actualSetting = clientSetting.getValue();
    assertThat(actualSetting.getProjectId()).isEqualTo(TENANT_PROJECT_ID);
    assertThat(actualSetting.getInstanceId()).isEqualTo(BIGTABLE_INSTANCE_ID);
    assertThat(actualSetting.getAppProfileId()).isEqualTo(READ_APP_PROFILE);
  }

  @Test
  public void createBigtableClient_notFoundError() throws Exception {
    ArgumentCaptor<BigtableDataSettings> clientSetting = forClass(BigtableDataSettings.class);
    CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
    FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
    when(BigtableDataClient.create(clientSetting.capture())).thenReturn(mockBigtableClient);
    when(mockOnlineStoreServiceClient.generateFetchAccessToken(any()))
        .thenThrow(new NotFoundException(
            new Throwable("Online Store not found"), getStatusCode(Code.NOT_FOUND), false));
    try {
      new BigtableClientManager(bigtableSpec, featureViewSpec, FV_NAME, LOCATION, Optional.empty());
      Assert.fail("Exception should be thrown but did not");
    } catch (NotFoundException e) {
      // pass
    }
  }

    @Test
    public void createBigtableClient_permissionDeniedError() throws Exception {
      ArgumentCaptor<BigtableDataSettings> clientSetting = forClass(BigtableDataSettings.class);
      CloudBigtableSpec bigtableSpec = new CloudBigtableSpec(BIGTABLE_FEATURE_ONLINE_STORE);
      FeatureViewSpec featureViewSpec = new FeatureViewSpec(DEFAULT_BIGTABLE_FEATURE_VIEW);
      when(BigtableDataClient.create(clientSetting.capture())).thenReturn(mockBigtableClient);
      when(mockOnlineStoreServiceClient.generateFetchAccessToken(any()))
          .thenThrow(new PermissionDeniedException(
              new Throwable("permission Denied"), getStatusCode(Code.PERMISSION_DENIED), false));
      try {
        new BigtableClientManager(bigtableSpec, featureViewSpec, FV_NAME, LOCATION, Optional.empty());
        Assert.fail("Exception should be thrown but did not");
      } catch (PermissionDeniedException e) {
        // pass
      }
  }

  private StatusCode getStatusCode(StatusCode.Code code){
    return new StatusCode() {
      @Override
      public Code getCode() {
        return code;
      }

      @Override
      public Object getTransportCode() {
        return null;
      }
    };
  }
}
