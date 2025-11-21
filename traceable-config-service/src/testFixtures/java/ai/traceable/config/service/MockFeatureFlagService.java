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
            .putValues(
                "enricher.genai-detection-v2",
                FeatureFlagValue.newBuilder().setBoolean(false).getDefaultInstanceForType())
            .putValues(
                "traceable-edge.edge-decision",
                FeatureFlagValue.newBuilder().setBoolean(true).build())
            .putValues(
                "ui.detection-exclusions-v2",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "config-service.waap-rules-versioning",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "tpa.crs-msg-hide-match-value",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "protection-engine.webapp-protection",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "protection-engine.api-protection",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "api-protect.policies.revamp",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .putValues(
                "api-protect.policies.migration",
                FeatureFlagValue.newBuilder().setBoolean(false).build())
            .build());
    responseObserver.onCompleted();
  }
}
