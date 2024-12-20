package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.span.processing.config.service.SpanProcessingConfigConstants;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleFilter;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class ServiceNamingRuleStore
    extends IdentifiedObjectStoreWithFilter<ServiceNamingRule, ServiceNamingRuleFilter> {
  private static final String SERVICE_NAMING_RULE_RESOURCE_NAME = "service-naming-rule";
  private final RankCalculator<ServiceNamingRule, String> rankCalculator;

  @Inject
  ServiceNamingRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RankCalculator<ServiceNamingRule, String> rankCalculator) {
    super(
        configServiceBlockingStub,
        SpanProcessingConfigConstants.RESOURCE_NAMESPACE,
        SERVICE_NAMING_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
    this.rankCalculator = rankCalculator;
  }

  @Override
  protected Optional<ServiceNamingRule> buildDataFromValue(Value ruleValue) {
    try {
      ServiceNamingRule.Builder builder = ServiceNamingRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ServiceNamingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected List<ContextualConfigObject<ServiceNamingRule>> orderFetchedObjects(
      List<ContextualConfigObject<ServiceNamingRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }

  @Override
  protected String getContextFromData(ServiceNamingRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<ServiceNamingRule> filterConfigData(
      ServiceNamingRule data, ServiceNamingRuleFilter filter) {

    if (!filter.getScope().hasEnvironmentScope() || !data.getScope().hasEnvironmentScope()) {
      return Optional.of(data);
    }

    if (!Collections.disjoint(
        filter.getScope().getEnvironmentScope().getEnvironmentNamesList(),
        data.getScope().getEnvironmentScope().getEnvironmentNamesList())) {
      return Optional.of(data);
    }

    return Optional.empty();
  }
}
