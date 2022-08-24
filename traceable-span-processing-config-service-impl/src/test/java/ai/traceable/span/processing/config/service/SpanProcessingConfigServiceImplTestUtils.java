package ai.traceable.span.processing.config.service;

import static ai.traceable.span.processing.config.service.v1.Field.FIELD_ENVIRONMENT_NAME;
import static ai.traceable.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EQUALS;

import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import com.google.protobuf.Duration;

class SpanProcessingConfigServiceImplTestUtils {

  static ProtectionSpanRuleInfo buildProtectionSpanRuleInfo() {
    return ProtectionSpanRuleInfo.newBuilder()
        .setName("name")
        .setDisabled(false)
        .setFilter(buildFilter())
        .build();
  }

  static SamplingConfig buildSamplingConfig(String id, long quantityAllowed) {
    return SamplingConfig.newBuilder()
        .setId(id)
        .setSamplingConfigInfo(buildSamplingConfigInfo(quantityAllowed))
        .build();
  }

  static SamplingConfigInfo buildSamplingConfigInfo(long quantityAllowed) {
    return SamplingConfigInfo.newBuilder()
        .setRateLimitConfig(buildRateLimitConfig(quantityAllowed))
        .build();
  }

  private static RateLimitConfig buildRateLimitConfig(long quantityAllowed) {
    return RateLimitConfig.newBuilder()
        .setTraceLimitGlobal(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(quantityAllowed)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setTraceLimitPerEndpoint(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(quantityAllowed)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setApiEndpointCacheDuration(Duration.newBuilder().setSeconds(604800).build())
        .build();
  }

  private static SpanFilter buildFilter() {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(FIELD_ENVIRONMENT_NAME)
                .setOperator(RELATIONAL_OPERATOR_EQUALS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue("env").build())
                .build())
        .build();
  }
}
