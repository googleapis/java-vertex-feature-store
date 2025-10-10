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

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import java.time.Duration;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DirectClientSettingsTest {

  @Test
  public void directClientSettings_emptyChannelSettings() throws Exception {
    DirectClientSettings settings = new DirectClientSettings.Builder().build();

    BigtableDataSettings btSettings =
        settings
            .toBigtableSettingsBuilder()
            .setProjectId("test")
            .setInstanceId("bigtable-instance")
            .build();
    TransportChannelProvider actualProvider =
        btSettings.getStubSettings().getTransportChannelProvider();
    assertThat(actualProvider).isInstanceOf(InstantiatingGrpcChannelProvider.class);

    InstantiatingGrpcChannelProvider expectedGrpcProvider =
        EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder().build();
    assertThat(actualProvider).isInstanceOf(InstantiatingGrpcChannelProvider.class);
    InstantiatingGrpcChannelProvider actualGrpcProvider =
        (InstantiatingGrpcChannelProvider) actualProvider;
    assertThat(actualGrpcProvider.getChannelPoolSettings())
        .isEqualTo(expectedGrpcProvider.getChannelPoolSettings());
  }

  @Test
  public void directClientSettings_channelSettings() throws Exception {
    // prepare
    ChannelPoolSettings channelPoolSettings =
        ChannelPoolSettings.builder()
            .setInitialChannelCount(3)
            .setMaxChannelCount(10)
            .setMinChannelCount(3)
            .build();
    // execute
    DirectClientSettings settings =
        new DirectClientSettings.Builder().setChannelPoolSettings(channelPoolSettings).build();
    BigtableDataSettings btSettings =
        settings
            .toBigtableSettingsBuilder()
            .setProjectId("test")
            .setInstanceId("bigtable-instance")
            .build();
    TransportChannelProvider actualProvider =
        btSettings.getStubSettings().getTransportChannelProvider();

    assertThat(actualProvider).isInstanceOf(InstantiatingGrpcChannelProvider.class);

    InstantiatingGrpcChannelProvider expectedGrpcProvider =
        EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder().build();
    assertThat(actualProvider).isInstanceOf(InstantiatingGrpcChannelProvider.class);
    InstantiatingGrpcChannelProvider actualGrpcProvider =
        (InstantiatingGrpcChannelProvider) actualProvider;
    assertThat(actualGrpcProvider.getChannelPoolSettings()).isEqualTo(channelPoolSettings);
  }

  @Test
  public void directClientSettings_emptyRetrySettings() throws Exception {
    // execute
    DirectClientSettings settings = new DirectClientSettings.Builder().build();
    BigtableDataSettings btSettings =
        settings
            .toBigtableSettingsBuilder()
            .setProjectId("test")
            .setInstanceId("bigtable-instance")
            .build();
    RetrySettings actualRetrySetting =
        btSettings.getStubSettings().readRowSettings().getRetrySettings();
    // Confirm default values are applied.
    assertThat(actualRetrySetting.getInitialRetryDelayDuration()).isEqualTo(Duration.ofMillis(10));
    assertThat(actualRetrySetting.getMaxRetryDelayDuration()).isEqualTo(Duration.ofMinutes(1));
    assertThat(actualRetrySetting.getMaxAttempts()).isEqualTo(0);
    assertThat(actualRetrySetting.getTotalTimeoutDuration()).isEqualTo(Duration.ofMinutes(10));
  }

  @Test
  public void directClientSettings_retrySettings() throws Exception {
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelayDuration(Duration.ofSeconds(1))
            .setMaxAttempts(10)
            .setMaxRetryDelayDuration(Duration.ofSeconds(5))
            .build();
    // Execute
    DirectClientSettings settings =
        new DirectClientSettings.Builder().setRetrySettings(retrySettings).build();
    BigtableDataSettings btSettings =
        settings
            .toBigtableSettingsBuilder()
            .setProjectId("test")
            .setInstanceId("bigtable-instance")
            .build();
    RetrySettings actualRetrySetting =
        btSettings.getStubSettings().readRowSettings().getRetrySettings();

    assertThat(actualRetrySetting).isEqualTo(retrySettings);
  }

  @Test
  public void directClientSettings_retriableCodes() throws Exception {
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelayDuration(Duration.ofSeconds(1))
            .setMaxAttempts(10)
            .setMaxRetryDelayDuration(Duration.ofSeconds(5))
            .build();
    // Execute
    DirectClientSettings settings =
        new DirectClientSettings.Builder()
            .setRetryableCodes(Code.ABORTED, Code.INTERNAL, Code.CANCELLED)
            .build();
    BigtableDataSettings btSettings =
        settings
            .toBigtableSettingsBuilder()
            .setProjectId("test")
            .setInstanceId("bigtable-instance")
            .build();
    Set<Code> actualCodes = btSettings.getStubSettings().readRowSettings().getRetryableCodes();
    assertThat(actualCodes)
        .containsExactly(
            StatusCode.Code.ABORTED, StatusCode.Code.INTERNAL, StatusCode.Code.CANCELLED);
  }
}
