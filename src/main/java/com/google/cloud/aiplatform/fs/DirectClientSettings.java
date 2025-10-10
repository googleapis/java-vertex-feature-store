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

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;

// Settings class for FeatureOnlineStoreDirectClient. This class can be used by customers to
// configure Bigtable connections.
public class DirectClientSettings {

 private ChannelPoolSettings channelPoolSettings;
  private RetrySettings retrySettings;
  private Code[] retryables;

  public DirectClientSettings(Builder builder) {
    this.channelPoolSettings = builder.channelPoolSettings;
    this.retrySettings = builder.retrySettings;
    this.retryables = builder.retryables;
  }

  // Build a BigtableDataSettings.Builder by applying all the provided configs.
  public BigtableDataSettings.Builder toBigtableSettingsBuilder() {
    BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();

    if (channelPoolSettings != null) {
      builder
          .stubSettings()
          .setTransportChannelProvider(
              EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
                  .setChannelPoolSettings(channelPoolSettings)
                  .build());
    }

    UnaryCallSettings.Builder<Query, Row> readRowSettings =
        builder.stubSettings().readRowSettings();
    if (retrySettings != null) {
      readRowSettings.setRetrySettings(retrySettings);
    }
    if (retryables != null && retryables.length > 0) {
      readRowSettings.setRetryableCodes(retryables);
      // All read operations need to have the same retryable codes.
      builder.stubSettings().readRowsSettings().setRetryableCodes(retryables);
      builder.stubSettings().bulkReadRowsSettings().setRetryableCodes(retryables);
    }
    return builder;
  }

  public static class Builder {
   private ChannelPoolSettings channelPoolSettings;
    private RetrySettings retrySettings;
    private Code[] retryables;

    public Builder setChannelPoolSettings(ChannelPoolSettings channelPoolSettings) {
      this.channelPoolSettings = channelPoolSettings;
      return this;
    }

    public Builder setRetrySettings(RetrySettings retrySettings) {
      this.retrySettings = retrySettings;
      return this;
    }

    public Builder setRetryableCodes(Code... codes) {
      this.retryables = codes;
      return this;
    }

    public DirectClientSettings build() {
      return new DirectClientSettings(this);
    }
  }
}