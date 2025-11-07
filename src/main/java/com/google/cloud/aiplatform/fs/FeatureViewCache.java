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
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceClient;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.FeatureViewName;
import com.google.cloud.aiplatform.v1.GetFeatureViewRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.Status;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache class for FeatureViewSpec metadata
 */
public final class FeatureViewCache {

  // Singleton cache, shared across all threads in Java.
  private static final FeatureViewCache FEATURE_VIEW_CACHE = new FeatureViewCache();

  private final LoadingCache<String, FeatureViewSpec> cache;

  private final FeatureOnlineStoreAdminServiceClientFactory clientFactory;


  private FeatureViewCache() {
    this(new DefaultFeatureOnlineStoreAdminServiceClientFactory());
  }

  FeatureViewCache(FeatureOnlineStoreAdminServiceClientFactory clientFactory) {
    this.clientFactory = clientFactory;
    cache = CacheBuilder.newBuilder()
        .maximumSize(100) // Limit cache size
        .build(new CacheLoader<String, FeatureViewSpec>() {

          @Override
          public FeatureViewSpec load(String fvName) throws Exception {
            FeatureViewName name = FeatureViewName.parse(fvName);
            FeatureOnlineStoreAdminServiceClient adminClient =
                clientFactory.createClient(name.getLocation());
            GetFeatureViewRequest fvRequest =
                GetFeatureViewRequest.newBuilder().setName(fvName).build();
            FeatureView fv = adminClient.getFeatureView(fvRequest);
            adminClient.close();
            if (!fv.hasBigtableMetadata()) {
              throw new InvalidArgumentException(
                  new Throwable("Direct access to Bigtable is not enabled"),
                  /* statusCode= */ GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                  /* retryable= */false);
            }
            return new FeatureViewSpec(fv);
          }});
  }

  public FeatureViewSpec getFeatureViewSpec(String key) {
    try {
      return cache.get(key);
    } catch (Exception e) {
      // CacheLoader's load() automatically wraps exception in UncheckedExecutionException.
      // Get the underlying actual exception and re-throw for better debuggability.
      Throwable cause = e.getCause();
      if (cause instanceof InvalidArgumentException) {
        throw (InvalidArgumentException) cause;
      } else if (cause instanceof NotFoundException) {
        throw (NotFoundException) cause;
      } else if (cause instanceof PermissionDeniedException) {
        throw (PermissionDeniedException) cause;
      } else {
        Logger.getLogger(FeatureViewCache.class.getName()).log(Level.SEVERE, String.format("Unexpected exception: %s", e));
        throw new RuntimeException(cause);
      }
    }
  }

  public static FeatureViewCache getInstance() {
    return FEATURE_VIEW_CACHE;
  }

  public void clear() {
    cache.invalidateAll();
  }
}

class FeatureViewSpec {
  Boolean continuousSyncEnabled;

  String readAppProfileId;

  Boolean timestampsEnabled;

  FeatureViewSpec(boolean continuousSyncEnabled, String readAppProfileId, boolean timestampsEnabled) {
    this.continuousSyncEnabled = continuousSyncEnabled;
    this.readAppProfileId = readAppProfileId;
    this.timestampsEnabled = timestampsEnabled;
  }

  FeatureViewSpec(FeatureView fv) {
    this.continuousSyncEnabled = fv.getSyncConfig().getContinuous();
    this.readAppProfileId = fv.getBigtableMetadata().getReadAppProfile();
    this.timestampsEnabled = false;
  }
}
