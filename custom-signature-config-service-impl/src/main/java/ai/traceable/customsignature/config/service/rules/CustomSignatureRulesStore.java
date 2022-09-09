package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class CustomSignatureRulesStore
    extends IdentifiedObjectStoreWithFilter<CustomSignatureRule, GetRulesFilter> {

  private final CustomSignatureRuleConverter customSignatureRuleConverter;
  public static final String CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE = "customSignatureRule";
  public static final String CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME =
      "customSignatureRuleConfig";

  @Inject
  public CustomSignatureRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      CustomSignatureRuleConverter customSignatureRuleConverter,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE,
        CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.customSignatureRuleConverter = customSignatureRuleConverter;
  }

  @Override
  protected Optional<CustomSignatureRule> buildDataFromValue(Value value) {
    try {
      return Optional.of(customSignatureRuleConverter.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to custom signature rule for rule: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(CustomSignatureRule data) {
    return customSignatureRuleConverter.convert(data);
  }

  @Override
  protected String getContextFromData(CustomSignatureRule data) {
    return data.getId();
  }

  @Override
  protected Optional<CustomSignatureRule> filterConfigData(
      CustomSignatureRule ruleData, GetRulesFilter filter) {
    return Optional.of(ruleData)
        .filter(
            rule -> filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId()))
        .filter(
            rule ->
                filter.getEventTypesList().isEmpty()
                    || filter.getEventTypesList().contains(rule.getEffect().getEventType()))
        .filter(rule -> !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled()))
        .filter(rule -> !(filter.hasInternal() && rule.getInternal() != filter.getInternal()))
        .filter(rule -> filterRuleOnScope(rule.getRuleScope(), filter.getRuleScope()));
  }

  private boolean filterRuleOnScope(RuleScope ruleScope, RuleScope filterScope) {
    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    List<String> ruleEnvironmentIds = ruleScope.getEnvironmentScope().getEnvironmentIdsList();
    if (filterEnvironmentIds.isEmpty() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }
}
