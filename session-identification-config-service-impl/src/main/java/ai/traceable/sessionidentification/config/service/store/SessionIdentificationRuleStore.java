package ai.traceable.sessionidentification.config.service.store;

import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class SessionIdentificationRuleStore
    extends IdentifiedObjectStoreWithFilter<
        SessionIdentificationRule,
        GetSessionIdentificationRulesRequest.GetSessionIdentificationRulesFilter> {
  private static final String SESSION_IDENTIFICATION_RULE_RESOURCE_NAME =
      "session-identification-rule";
  private static final String SESSION_IDENTIFICATION_RESOURCE_NAMESPACE = "session-identification";

  @Inject
  public SessionIdentificationRuleStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SESSION_IDENTIFICATION_RESOURCE_NAMESPACE,
        SESSION_IDENTIFICATION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<SessionIdentificationRule> buildDataFromValue(Value value) {
    SessionIdentificationRule.Builder builder = SessionIdentificationRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to SessionIdentificationRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SessionIdentificationRule data) {
    SessionIdentificationRule.Builder builder = data.toBuilder();
    return ConfigProtoConverter.convertToValue(builder);
  }

  @Override
  protected String getContextFromData(SessionIdentificationRule data) {
    return data.getId();
  }

  @Override
  protected Optional<SessionIdentificationRule> filterConfigData(
      SessionIdentificationRule data,
      GetSessionIdentificationRulesRequest.GetSessionIdentificationRulesFilter filter) {
    return Optional.of(data)
        .filter(
            rule ->
                filter.getRuleIdsList().isEmpty() || filter.getRuleIdsList().contains(rule.getId()))
        .filter(
            rule -> !filter.hasDisabled() || rule.getStatus().getDisabled() == filter.getDisabled())
        .filter(rule -> filterRuleOnScope(data, filter));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      SessionIdentificationRule rule,
      GetSessionIdentificationRulesRequest.GetSessionIdentificationRulesFilter filter) {
    List<String> ruleEnvironmentNamesList = rule.getScope().getEnvironmentNamesList();
    if (!filter.hasEnvironmentFilter() || ruleEnvironmentNamesList.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentNamesList =
        filter.getEnvironmentFilter().getEnvironmentNamesList();
    return ruleEnvironmentNamesList.stream().anyMatch(filterEnvironmentNamesList::contains);
  }
}
