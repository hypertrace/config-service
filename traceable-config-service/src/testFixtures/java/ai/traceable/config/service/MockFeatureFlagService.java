package ai.traceable.config.service;

import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceImplBase;
import ai.traceable.featureflag.v1.FeatureFlagValue;
import ai.traceable.featureflag.v1.FeatureFlagValueChange;
import ai.traceable.featureflag.v1.SubscribeFlagValuesRequest;
import ai.traceable.featureflag.v1.SubscribeFlagValuesResponse;
import io.grpc.stub.StreamObserver;

public class MockFeatureFlagService extends FeatureFlagServiceImplBase {
  @Override
  public void subscribeFlagValues(
      SubscribeFlagValuesRequest request,
      StreamObserver<SubscribeFlagValuesResponse> responseObserver) {
    responseObserver.onNext(
        SubscribeFlagValuesResponse.newBuilder()
            .putChanges(
                "data-classification.mvp",
                FeatureFlagValueChange.newBuilder()
                    .setCurrent(FeatureFlagValue.newBuilder().setBoolean(true).build())
                    .build())
            .build());
    responseObserver.onCompleted();
  }
}
