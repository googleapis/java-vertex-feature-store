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

import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.protobuf.AbstractMessage;
import io.grpc.ServerServiceDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

// Mock class for the APIs in FeatureOnlineStoreAdminClient.
public class MockFeatureOnlineStoreAdminService implements MockGrpcService {

  private final MockFeatureOnlineStoreAdminServiceImpl serviceImpl;

  // Requests sent from test cases.
  private List<AbstractMessage> requests;

  // Response to return to test execution.
  private Queue<Object> responses;

  protected MockFeatureOnlineStoreAdminService() throws IOException {
    serviceImpl = new MockFeatureOnlineStoreAdminServiceImpl();
    requests = new ArrayList<>();
    responses = new LinkedList<>();
  }

  @Override
  public List<AbstractMessage> getRequests() {
    return serviceImpl.getRequests();
  }

  @Override
  public void addResponse(AbstractMessage response) {
    serviceImpl.addResponse(response);
  }

  @Override
  public void addException(Exception exception) {
    serviceImpl.addException(exception);
  }

  @Override
  public ServerServiceDefinition getServiceDefinition() {
    return serviceImpl.bindService();
  }

  @Override
  public void reset() {
    serviceImpl.reset();
  }
}
