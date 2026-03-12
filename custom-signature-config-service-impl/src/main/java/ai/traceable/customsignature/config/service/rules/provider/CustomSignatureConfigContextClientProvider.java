package ai.traceable.customsignature.config.service.rules.provider;

import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureConfigContextConverter;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureConfigContextClientProvider
    implements CustomSignatureConfigContextProvider {

  private final CustomSignatureRulesStore rulesStore;
  private final CustomSignatureConfigContextConverter configContextConverter;

  private static final GetRulesFilter CUSTOM_SIGNATURE_RULES_FILTER =
      GetRulesFilter.newBuilder()
          .setDisabled(false)
          .addCategories(Category.CATEGORY_CUSTOM_SIGNATURE)
          .build();

  @Inject
  public CustomSignatureConfigContextClientProvider(
      CustomSignatureRulesStore rulesStore,
      CustomSignatureConfigContextConverter configContextConverter) {
    this.rulesStore = rulesStore;
    this.configContextConverter = configContextConverter;
  }

  @Override
  public CustomSignatureConfigContext getCustomSignatureConfigContext(
      RequestContext requestContext, GetCustomSignatureEvaluationConfigContextRequest request) {
    GetRulesFilter filter = createEventTypeFilter(request);
    return loadCustomSignatureConfigContext(requestContext, request, filter);
  }

  protected CustomSignatureConfigContext loadCustomSignatureConfigContext(
      RequestContext requestContext,
      GetCustomSignatureEvaluationConfigContextRequest request,
      GetRulesFilter filter) {

    try {
      List<CustomSignatureRuleRecord> ruleRecords =
          getCustomSignatureRuleRecords(requestContext, filter);

      log.debug(
          "Retrieved {} custom signature rule records for tenant: {} with event type filter",
          ruleRecords.size(),
          requestContext.getTenantId());

      return configContextConverter.convert(
          ruleRecords.stream().map(CustomSignatureRuleRecord::getRule).collect(Collectors.toList()),
          request);

    } catch (Exception e) {
      log.error(
          "Error loading custom signature config context from records for tenant: {}",
          requestContext.getTenantId(),
          e);
      return CustomSignatureConfigContext.getDefaultInstance();
    }
  }

  protected List<CustomSignatureRuleRecord> getCustomSignatureRuleRecords(
      RequestContext requestContext, GetRulesFilter filter) {
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllRuleRecords(requestContext);
    } else {
      return rulesStore.getAllRuleRecords(requestContext, filter);
    }
  }

  protected GetRulesFilter createEventTypeFilter(
      GetCustomSignatureEvaluationConfigContextRequest request) {
    GetRulesFilter.Builder filterBuilder = GetRulesFilter.newBuilder(CUSTOM_SIGNATURE_RULES_FILTER);
    if (request == null) {
      return filterBuilder.build();
    }

    if (request.getRuleEvaluationPoint() != RuleEvaluationPoint.RULE_EVALUATION_POINT_UNSPECIFIED) {
      filterBuilder.addRuleEvaluationPoints(request.getRuleEvaluationPoint());
    }
    if (request.getEventType() != EventType.EVENT_TYPE_UNSPECIFIED) {
      filterBuilder.addEventTypes(request.getEventType());
    }

    return filterBuilder.build();
  }
}
