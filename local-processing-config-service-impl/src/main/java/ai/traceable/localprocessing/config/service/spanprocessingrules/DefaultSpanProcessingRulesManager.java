package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.RateLimitConfigManager;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRules;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceRequest;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceResponse;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultSpanProcessingRulesManager implements SpanProcessingRulesManager {

  private final ExcludeSpanRulesManager excludeSpanRulesManager;
  private final RateLimitConfigManager rateLimitConfigManager;
  private final ProtectionSpanRulesManager protectionSpanRulesManager;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultSpanProcessingRulesManager(
      ExcludeSpanRulesManager excludeSpanRulesManager,
      RateLimitConfigManager rateLimitConfigManager,
      ProtectionSpanRulesManager protectionSpanRulesManager,
      UuidGenerator uuidGenerator) {
    this.excludeSpanRulesManager = excludeSpanRulesManager;
    this.rateLimitConfigManager = rateLimitConfigManager;
    this.protectionSpanRulesManager = protectionSpanRulesManager;
    this.uuidGenerator = uuidGenerator;
  }

  public GetSpanProcessingRulesResponse getSpanProcessingRulesResponse(
      RequestContext requestContext, GetSpanProcessingRulesRequest request) {
    Optional<String> environment =
        request.hasEnvironment() ? Optional.of(request.getEnvironment()) : Optional.empty();
    List<SpanProcessingRulesServiceResponse> serviceResponses = new ArrayList<>();
    for (SpanProcessingRulesServiceRequest serviceRequest : request.getServiceRequestsList()) {
      serviceResponses.add(getSpanProcessingRules(requestContext, serviceRequest, environment));
    }
    return GetSpanProcessingRulesResponse.newBuilder()
        .addAllSpanProcessingRulesServiceResponses(serviceResponses)
        .build();
  }

  private SpanProcessingRulesServiceResponse getSpanProcessingRules(
      RequestContext requestContext,
      SpanProcessingRulesServiceRequest serviceRequest,
      Optional<String> environment) {
    String serviceName = serviceRequest.getServiceName();
    SpanProcessingRules spanProcessingRules =
        getSpanProcessingRules(requestContext, serviceName, environment);
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
      RequestContext requestContext, String serviceName, Optional<String> environment) {
    SpanProcessingRules.Builder spanProcessingRulesBuilder =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(
                rateLimitConfigManager.getRateLimitConfig(requestContext, serviceName, environment))
            .addAllExcludeSpanRules(
                excludeSpanRulesManager.getAllExcludeSpanProcessingRules(
                    requestContext, serviceName, environment))
            .addAllProtectionSpanRules(
                protectionSpanRulesManager.getAllProtectionSpanProcessingRules(
                    requestContext, serviceName, environment));
    return spanProcessingRulesBuilder.build();
  }
}
