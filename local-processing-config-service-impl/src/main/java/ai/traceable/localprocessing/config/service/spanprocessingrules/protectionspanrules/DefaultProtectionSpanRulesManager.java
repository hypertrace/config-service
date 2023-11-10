package ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules;

import static java.util.Collections.unmodifiableList;

import ai.traceable.config.utils.SpanFilterMatcher;
import ai.traceable.localprocessing.config.service.utils.FilterConverter;
import ai.traceable.localprocessing.config.service.v1.ProtectionSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectionSpanProcessingRuleInfo;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultProtectionSpanRulesManager implements ProtectionSpanRulesManager {

  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;
  private final FilterConverter filterConverter;

  @Override
  public List<ProtectionSpanRule> getAllProtectionSpanRules(RequestContext requestContext) {
    return unmodifiableList(
        requestContext
            .call(
                () ->
                    configServiceBlockingStub.getAllResolvedProtectionSpanRules(
                        GetAllResolvedProtectionSpanRulesRequest.getDefaultInstance()))
            .getRulesList());
  }

  @Override
  public List<ProtectionSpanProcessingRule> getAllMatchingProtectionSpanProcessingRules(
      RequestContext requestContext,
      List<ProtectionSpanRule> protectionSpanRules,
      String serviceName,
      Optional<String> environment) {
    return protectionSpanRules.stream()
        .map(
            protectionSpanRule ->
                convertProtectionSpanRule(protectionSpanRule, serviceName, environment))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  // assumption: first class field conditions are ANDed and appear in the first level of the filter
  // tree structure
  private Optional<ProtectionSpanProcessingRule> convertProtectionSpanRule(
      ProtectionSpanRule protectionSpanRule, String serviceName, Optional<String> environment) {
    // check if the rule is disabled
    if (!protectionSpanRule.getRuleInfo().hasFilter()
        || protectionSpanRule.getRuleInfo().getDisabled()) {
      return Optional.empty();
    }

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(
        protectionSpanRule.getRuleInfo().getFilter(), environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(
        protectionSpanRule.getRuleInfo().getFilter(), serviceName)) {
      return Optional.empty();
    }

    Optional<SpanFilter> spanFilter =
        filterConverter.convert(protectionSpanRule.getRuleInfo().getFilter());
    if (spanFilter.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        ProtectionSpanProcessingRule.newBuilder()
            .setProtectionSpanProcessingRuleInfo(
                ProtectionSpanProcessingRuleInfo.newBuilder()
                    .setId(protectionSpanRule.getId())
                    .setFilter(spanFilter.get())
                    .build())
            .build());
  }
}
