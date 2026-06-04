package ai.traceable.application.grouping.config.service.store;

import static ai.traceable.application.grouping.config.service.constants.ApplicationGroupingServiceConfigConstants.APPLICATION_GROUPING_CONFIG_NAMESPACE;
import static ai.traceable.application.grouping.config.service.constants.ApplicationGroupingServiceConfigConstants.APPLICATION_GROUPING_RULE_CONFIG_RESOURCE_NAME;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRulesFilter;
import com.google.inject.Inject;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedFilterPushedDownObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.LogicalOperator;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.config.service.v1.RelationalOperator;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingRuleConfigStore
    extends IdentifiedFilterPushedDownObjectStore<
        ApplicationGroupingRuleConfig, ApplicationGroupingRulesFilter, SortBy> {

  private static final String ID = "id";

  @Inject
  public ApplicationGroupingRuleConfigStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator,
      final ClientConfig clientConfig) {
    super(
        configServiceBlockingStub,
        APPLICATION_GROUPING_CONFIG_NAMESPACE,
        APPLICATION_GROUPING_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator,
        clientConfig);
  }

  @Override
  @SneakyThrows
  protected Optional<ApplicationGroupingRuleConfig> buildDataFromValue(final Value value) {
    final ApplicationGroupingRuleConfig.Builder configBuilder =
        ApplicationGroupingRuleConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(
      final ApplicationGroupingRuleConfig applicationGroupingRuleConfig) {
    return ConfigProtoConverter.convertToValue(applicationGroupingRuleConfig);
  }

  @Override
  @SneakyThrows
  protected String getContextFromData(
      final ApplicationGroupingRuleConfig applicationGroupingRuleConfig) {
    return applicationGroupingRuleConfig.getId();
  }

  @Override
  protected Filter buildFilter(final ApplicationGroupingRulesFilter filterInput) {
    final List<Filter> filters = new ArrayList<>();

    if (!filterInput.getIdsList().isEmpty()) {
      filters.add(buildInFilter(ID, filterInput.getIdsList()));
    }

    return buildAndFilter(filters);
  }

  @Override
  protected SortBy buildSort(final SortBy sortInput) {
    return sortInput;
  }

  public List<ApplicationGroupingRuleConfig> getMatchingApplicationGroupingRuleConfigs(
      final RequestContext requestContext, final ApplicationGroupingRulesFilter filterInput) {
    return getMatchingObjects(requestContext, filterInput, emptyList()).stream()
        .map(ContextualConfigObject::getData)
        .collect(toUnmodifiableList());
  }

  public Optional<ApplicationGroupingRuleConfig> findByRuleName(
      final RequestContext requestContext, final String ruleName) {
    return getAllConfigData(requestContext).stream()
        .filter(
            config -> config.getApplicationGroupingRuleConfigInfo().getRuleName().equals(ruleName))
        .findFirst();
  }

  private Filter buildInFilter(final String path, final List<?> values) {
    final ListValue.Builder listValue = ListValue.newBuilder();
    values.forEach(
        value -> listValue.addValues(Value.newBuilder().setStringValue(value.toString())));

    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                .setValue(Value.newBuilder().setListValue(listValue)))
        .build();
  }

  private Filter buildAndFilter(final List<Filter> filters) {
    if (filters.isEmpty()) {
      return Filter.getDefaultInstance();
    }
    if (filters.size() == 1) {
      return filters.get(0);
    }
    return Filter.newBuilder()
        .setLogicalFilter(
            LogicalFilter.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                .addAllOperands(filters))
        .build();
  }
}
