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
import com.google.cloud.aiplatform.v1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreAdminServiceClient;
import com.google.cloud.aiplatform.v1.FeatureOnlineStoreName;
import com.google.cloud.aiplatform.v1.GetFeatureOnlineStoreRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.Status;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache class for CloudBigtable metadata
 */
class CloudBigtableCache {
  // Singleton cache, shared across all threads in Java.
  private static final CloudBigtableCache BIGTABLE_CACHE = new CloudBigtableCache();

  private final LoadingCache<String, CloudBigtableSpec> cache;
  private final FeatureOnlineStoreAdminServiceClientFactory clientFactory;

  // Default constructor to uses the real admin service client.
  private CloudBigtableCache() {
    this(new DefaultFeatureOnlineStoreAdminServiceClientFactory());
  }

  CloudBigtableCache(FeatureOnlineStoreAdminServiceClientFactory clientFactory) {
    this.clientFactory = clientFactory;
    cache = CacheBuilder.newBuilder()
        .maximumSize(100) // Limit cache size
        .build(new CacheLoader<String, CloudBigtableSpec>() {
          @Override
          public CloudBigtableSpec load(String fosName) throws Exception {
            FeatureOnlineStoreName name = FeatureOnlineStoreName.parse(fosName);
            FeatureOnlineStoreAdminServiceClient adminClient =
                clientFactory.createClient(name.getLocation());
            GetFeatureOnlineStoreRequest fosRequest =
                GetFeatureOnlineStoreRequest.newBuilder().setName(fosName).build();
            FeatureOnlineStore fos = adminClient.getFeatureOnlineStore(fosRequest);
            adminClient.close();
            if (!fos.hasBigtable()) {
              throw new InvalidArgumentException(
                  new Throwable("Online store storage is not Bigtable"),
                  /* statusCode= */ GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                  /* retryable= */false);
            }
            if (!fos.getBigtable().hasBigtableMetadata()) {
              throw new InvalidArgumentException(
                  new Throwable("Direct access to Bigtable is not enabled"),
                  /* statusCode= */ GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                  /* retryable= */false);
            }
            return new CloudBigtableSpec(fos);
          }
        });
  }

  // Method to be called by FeatureOnlineStoreDirectClient to load Bigtable metadata.
  // If Bigtable metadata for the given fosName is not in the cache, it triggers load().
  public CloudBigtableSpec getCloudBigtableSpec(String fosName) {
    try {
      return cache.get(fosName);
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
        Logger.getLogger(CloudBigtableCache.class.getName()).log(Level.SEVERE, String.format("Unexpected exception: %s", e));
        throw new RuntimeException(cause);
      }
    }
  }

  public static CloudBigtableCache getInstance() {
    return BIGTABLE_CACHE;
  }

  public void clear() {
    cache.invalidateAll();
  }

}

class CloudBigtableSpec {

  String tenantProjectId;
  String instanceId;
  String tableId;

  CloudBigtableSpec(FeatureOnlineStore fos) {
    this.tenantProjectId = fos.getBigtable().getBigtableMetadata().getTenantProjectId();
    this.instanceId = fos.getBigtable().getBigtableMetadata().getInstanceId();
    this.tableId = fos.getBigtable().getBigtableMetadata().getTableId();
  }
}