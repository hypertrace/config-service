package ai.traceable.config.service;

import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceImplBase;
import ai.traceable.featureflag.v1.FeatureFlagValue;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesRequest;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesResponse;
import io.grpc.stub.StreamObserver;

public class MockFeatureFlagService extends FeatureFlagServiceImplBase {
  @Override
  public void getCurrentFlagValues(
      GetCurrentFlagValuesRequest request,
      StreamObserver<GetCurrentFlagValuesResponse> responseObserver) {
    responseObserver.onNext(
        GetCurrentFlagValuesResponse.newBuilder()
            .putValues(
                "data-classification.mvp", FeatureFlagValue.newBuilder().setBoolean(true).build())
            .putValues(
                "enricher.ipqs-ip-intelligence",
                FeatureFlagValue.newBuilder().setBoolean(true).build())
            .putValues(
                "tpa.modsec-processing-disabled",
                FeatureFlagValue.newBuilder().setBoolean(false).getDefaultInstanceForType())
            .build());
    responseObserver.onCompleted();
  }
}
