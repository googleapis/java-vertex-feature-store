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

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.AutoScaling;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Bigtable.BigtableMetadata;
import com.google.cloud.aiplatform.v1.FeatureOnlineStore.Optimized;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CloudBigtableCacheTest {

  private static final String FOS_ID = "my_online_store";
  private static final String TENANT_PROJECT_ID = "tenant-project";
  private static final String BIGTABLE_INSTANCE_ID = "instance-id";
  private static final String BIGTABLE_TABLE_ID = "default";
  private static final String FOS_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s",
          "test", "us-central1", FOS_ID);

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

  @Test
  public void loadingCache_bigtable_success() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    // Set the response to return Bigtable proto.
    mockService.addResponse(BIGTABLE_FEATURE_ONLINE_STORE);

    // Execute
    CloudBigtableSpec spec = cache.getCloudBigtableSpec(FOS_NAME);

    // Assert
    assertThat(spec.tenantProjectId).isEqualTo(TENANT_PROJECT_ID);
    assertThat(spec.instanceId).isEqualTo(BIGTABLE_INSTANCE_ID);
    assertThat(spec.tableId).isEqualTo(BIGTABLE_TABLE_ID);
  }

  @Test
  public void loadingCache_optimized_fos() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    FeatureOnlineStore optimized = BIGTABLE_FEATURE_ONLINE_STORE.toBuilder()
        .clearBigtable()
        .setOptimized(Optimized.getDefaultInstance())
        .build();
    mockService.addResponse(optimized);

    // Execute
    try {
      cache.getCloudBigtableSpec(FOS_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (InvalidArgumentException e) {
      // pass
    }
  }

  @Test
  public void loadingCache_fos_notFound() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.NOT_FOUND);
    mockService.addException(exception);

    // Execute
    try {
      cache.getCloudBigtableSpec(FOS_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (NotFoundException e) {
     // pass
    }
  }

  @Test
  public void loadingCache_permission_denied() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.PERMISSION_DENIED);
    mockService.addException(exception);

    // Execute
    try {
      cache.getCloudBigtableSpec(FOS_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (PermissionDeniedException e) {
      // pass
    }
  }

  @Test
  public void loadingCache_unexpected_error() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.UNAVAILABLE);
    mockService.addException(exception);

    // Execute
    try {
      cache.getCloudBigtableSpec(FOS_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (RuntimeException e) {
      // pass
    }
  }

  @Test
  public void loadingCache_directRead_notEnabled() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    CloudBigtableCache cache = new CloudBigtableCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    // Set the response to return Bigtable proto.
    FeatureOnlineStore fos = FeatureOnlineStore.newBuilder()
        .setBigtable(Bigtable.getDefaultInstance())
        .build();
    mockService.addResponse(fos);

    // Execute
    try {
      cache.getCloudBigtableSpec(FOS_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (InvalidArgumentException e) {
      // pass
    }
  }
}
