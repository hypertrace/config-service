package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.RateLimitConfigManager;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRules;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceRequest;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceResponse;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;

@Slf4j
public class DefaultSpanProcessingRulesManager implements SpanProcessingRulesManager {

  private final ExcludeSpanRulesManager excludeSpanRulesManager;
  private final RateLimitConfigManager rateLimitConfigManager;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultSpanProcessingRulesManager(
      ExcludeSpanRulesManager excludeSpanRulesManager,
      RateLimitConfigManager rateLimitConfigManager,
      UuidGenerator uuidGenerator) {
    this.excludeSpanRulesManager = excludeSpanRulesManager;
    this.rateLimitConfigManager = rateLimitConfigManager;
    this.uuidGenerator = uuidGenerator;
  }

  public GetSpanProcessingRulesResponse getSpanProcessingRulesResponse(
      RequestContext requestContext, GetSpanProcessingRulesRequest request) {
    Optional<String> environment =
        request.hasEnvironment() ? Optional.of(request.getEnvironment()) : Optional.empty();
    final List<ExcludeSpanRule> excludeSpanRules =
        excludeSpanRulesManager.getAllExcludeSpanRules(requestContext);
    final List<SamplingConfig> samplingConfigs =
        rateLimitConfigManager.getAllSamplingConfigs(requestContext);
    List<SpanProcessingRulesServiceResponse> serviceResponses = new ArrayList<>();
    for (SpanProcessingRulesServiceRequest serviceRequest : request.getServiceRequestsList()) {
      serviceResponses.add(
          getSpanProcessingRules(
              requestContext, excludeSpanRules, samplingConfigs, serviceRequest, environment));
    }
    return GetSpanProcessingRulesResponse.newBuilder()
        .addAllSpanProcessingRulesServiceResponses(serviceResponses)
        .build();
  }

  private SpanProcessingRulesServiceResponse getSpanProcessingRules(
      RequestContext requestContext,
      List<ExcludeSpanRule> excludeSpanRules,
      List<SamplingConfig> samplingConfigs,
      SpanProcessingRulesServiceRequest serviceRequest,
      Optional<String> environment) {
    String serviceName = serviceRequest.getServiceName();
    SpanProcessingRules spanProcessingRules =
        getSpanProcessingRules(
            requestContext, excludeSpanRules, samplingConfigs, serviceName, environment);
    String hash = uuidGenerator.generateId(spanProcessingRules);
    if (hash.equals(serviceRequest.getHash())) {
      return SpanProcessingRulesServiceResponse.newBuilder()
          .setServiceName(serviceName)
          .setHash(hash)
          .build();
    }
    return SpanProcessingRulesServiceResponse.newBuilder()
        .setServiceName(serviceRequest.getServiceName())
        .setSpanProcessingRules(spanProcessingRules)
        .setHash(hash)
        .build();
  }

  private SpanProcessingRules getSpanProcessingRules(
      RequestContext requestContext,
      List<ExcludeSpanRule> excludeSpanRules,
      List<SamplingConfig> samplingConfigs,
      String serviceName,
      Optional<String> environment) {
    SpanProcessingRules.Builder spanProcessingRulesBuilder =
        SpanProcessingRules.newBuilder()
            .addAllExcludeSpanRules(
                excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
                    requestContext, excludeSpanRules, serviceName, environment));

    Optional<RateLimitConfig> rateLimitConfigOptional =
        rateLimitConfigManager.getFirstMatchingRateLimitConfig(
            requestContext, samplingConfigs, serviceName, environment);
    rateLimitConfigOptional.ifPresent(spanProcessingRulesBuilder::setRateLimitConfig);

    return spanProcessingRulesBuilder.build();
  }
}
