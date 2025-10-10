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

import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStore;
import com.google.cloud.aiplatform.v1beta1.FeatureOnlineStoreAdminServiceGrpc.FeatureOnlineStoreAdminServiceImplBase;
import com.google.cloud.aiplatform.v1beta1.FeatureView;
import com.google.cloud.aiplatform.v1beta1.GetFeatureOnlineStoreRequest;
import com.google.cloud.aiplatform.v1beta1.GetFeatureViewRequest;
import com.google.protobuf.AbstractMessage;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

// Mock implementation for FeatureOnlineStoreAdminService.
public class MockFeatureOnlineStoreAdminServiceImpl extends FeatureOnlineStoreAdminServiceImplBase {

  private List<AbstractMessage> requests;
  private Queue<Object> responses;

  public MockFeatureOnlineStoreAdminServiceImpl() {
    requests = new ArrayList<>();
    responses = new LinkedList<>();
  }

  public List<AbstractMessage> getRequests() {
    return requests;
  }

  public void addResponse(AbstractMessage response) {
    responses.add(response);
  }

  public void setResponses(List<AbstractMessage> responses) {
    this.responses = new LinkedList<Object>(responses);
  }

  public void addException(Exception exception) {
    responses.add(exception);
  }

  public void reset() {
    requests = new ArrayList<>();
    responses = new LinkedList<>();
  }

  @Override
  public void getFeatureOnlineStore(
      GetFeatureOnlineStoreRequest request, StreamObserver<FeatureOnlineStore> responseObserver) {
    Object response = responses.poll();
    if (response instanceof FeatureOnlineStore) {
      requests.add(request);
      responseObserver.onNext(((FeatureOnlineStore) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method GetFeatureOnlineStore, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  FeatureOnlineStore.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void getFeatureView(
      GetFeatureViewRequest request, StreamObserver<FeatureView> responseObserver) {
    Object response = responses.poll();
    if (response instanceof FeatureView) {
      requests.add(request);
      responseObserver.onNext(((FeatureView) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method GetFeatureView, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  FeatureView.class.getName(),
                  Exception.class.getName())));
    }
  }
}
