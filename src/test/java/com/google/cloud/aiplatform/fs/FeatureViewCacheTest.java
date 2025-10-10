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
import com.google.cloud.aiplatform.v1.FeatureView;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FeatureViewCacheTest {

  private static final String READ_PROFILE_ID = "read-3115404624652140544";

  private static final String FV_NAME =
      String.format(
          "projects/%s/locations/%s/featureOnlineStores/%s/featureViews/%s",
          "test", "us-central1", "my_online_store", "my_feature_view");

  private static final FeatureView DEFAULT_BIGTABLE_FEATURE_VIEW =
      FeatureView.newBuilder()
          .setName(FV_NAME)
          .setBigtableMetadata(
              FeatureView.BigtableMetadata.newBuilder().setReadAppProfile(READ_PROFILE_ID).build())
          .build();

  @Test
  public void loadingCache_bigtable_success() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    FeatureViewCache cache = new FeatureViewCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    // Set the response to return Bigtable proto.
    mockService.addResponse(DEFAULT_BIGTABLE_FEATURE_VIEW);

    // Execute
    FeatureViewSpec spec = cache.getFeatureViewSpec(FV_NAME);

    // Assert
    assertThat(spec.continuousSyncEnabled).isEqualTo(false);
    assertThat(spec.readAppProfileId).isEqualTo(READ_PROFILE_ID);
  }


  @Test
  public void loadingCache_fv_notFound() throws Exception {
    // Prepare
    MockFeatureOnlineStoreAdminServiceClientFactory factory =
        new MockFeatureOnlineStoreAdminServiceClientFactory();
    FeatureViewCache cache = new FeatureViewCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.NOT_FOUND);
    mockService.addException(exception);

    // Execute
    try {
      cache.getFeatureViewSpec(FV_NAME);
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
    FeatureViewCache cache = new FeatureViewCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.PERMISSION_DENIED);
    mockService.addException(exception);

    // Execute
    try {
      cache.getFeatureViewSpec(FV_NAME);
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
    FeatureViewCache cache = new FeatureViewCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    StatusRuntimeException exception = new StatusRuntimeException(io.grpc.Status.UNAVAILABLE);
    mockService.addException(exception);

    // Execute
    try {
      cache.getFeatureViewSpec(FV_NAME);
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
    FeatureViewCache cache = new FeatureViewCache(factory);
    MockFeatureOnlineStoreAdminService mockService = factory.getMockService();
    // Set the response to return Bigtable proto.
    mockService.addResponse(FeatureView.getDefaultInstance());

    // Execute
    try {
      cache.getFeatureViewSpec(FV_NAME);
      // Assert
      Assert.fail("Exception should be thrown but did not");
    } catch (InvalidArgumentException e) {
      // pass
    }
  }
}
