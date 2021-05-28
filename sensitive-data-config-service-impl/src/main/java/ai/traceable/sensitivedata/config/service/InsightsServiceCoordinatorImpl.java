package ai.traceable.sensitivedata.config.service;

import ai.traceable.platform.insights.api.v1.AttributeFilter;
import ai.traceable.platform.insights.api.v1.AttributeValue;
import ai.traceable.platform.insights.api.v1.InsightType;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc.InsightsServiceBlockingStub;
import ai.traceable.platform.insights.api.v1.Operator;
import ai.traceable.platform.insights.api.v1.QueryInsightsRequest;
import ai.traceable.platform.insights.api.v1.QueryInsightsResponse;
import ai.traceable.platform.insights.api.v1.Value;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;

class InsightsServiceCoordinatorImpl implements InsightsServiceCoordinator {

  private static final String HEADER_NAMESPACED_NAME = "headerNamespacedName";
  private static final String IS_HEADER_PII = "isHeaderPii";

  private final InsightsServiceBlockingStub insightsServiceBlockingStub;

  @Inject
  InsightsServiceCoordinatorImpl(InsightsServiceBlockingStub insightsServiceBlockingStub) {
    this.insightsServiceBlockingStub = insightsServiceBlockingStub;
  }

  @Override
  public List<Parameter> getSensitiveHeaderParameters(RequestContext requestContext) {
    AttributeFilter isPiiAttributeFilter =
        AttributeFilter.newBuilder()
            .setOperator(Operator.OPERATOR_EQ)
            .setName(IS_HEADER_PII)
            .setValue(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setBoolean(true).build())
                    .build())
            .build();
    QueryInsightsRequest request =
        QueryInsightsRequest.newBuilder()
            .setType(InsightType.INSIGHT_TYPE_HEADER)
            .setFilter(isPiiAttributeFilter)
            .build();
    QueryInsightsResponse response =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            requestContext.getRequestHeaders(),
            () -> insightsServiceBlockingStub.queryInsights(request));
    return response.getInsightList().stream()
        .filter(i -> i.getAttributesMap().containsKey(HEADER_NAMESPACED_NAME))
        .map(
            i ->
                Parameter.newBuilder()
                    .setName(
                        i.getAttributesMap().get(HEADER_NAMESPACED_NAME).getValue().getString())
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
