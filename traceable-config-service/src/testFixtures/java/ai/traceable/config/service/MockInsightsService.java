package ai.traceable.config.service;

import ai.traceable.platform.insights.api.v1.AttributeValue;
import ai.traceable.platform.insights.api.v1.Insight;
import ai.traceable.platform.insights.api.v1.InsightType;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.platform.insights.api.v1.QueryInsightsRequest;
import ai.traceable.platform.insights.api.v1.QueryInsightsResponse;
import ai.traceable.platform.insights.api.v1.Value;
import io.grpc.stub.StreamObserver;

public class MockInsightsService extends InsightsServiceGrpc.InsightsServiceImplBase {

  @Override
  public void queryInsights(
      QueryInsightsRequest request, StreamObserver<QueryInsightsResponse> responseObserver) {
    responseObserver.onNext(
        QueryInsightsResponse.newBuilder()
            .addInsight(getHeaderInsight("http.request.header.h1"))
            .addInsight(getHeaderInsight("http.request.header.h2"))
            .build());
    responseObserver.onCompleted();
  }

  private Insight getHeaderInsight(String headerNamespacedName) {
    return Insight.newBuilder()
        .setType(InsightType.INSIGHT_TYPE_HEADER)
        .putAttributes(
            "headerNamespacedName",
            AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString(headerNamespacedName).build())
                .build())
        .build();
  }
}
