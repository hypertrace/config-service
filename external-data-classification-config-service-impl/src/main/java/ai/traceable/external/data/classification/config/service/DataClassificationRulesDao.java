package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataClassificationOverrideScope;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.data.classification.config.service.v1.ScopeFilter;
import com.google.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationRulesDao {
  private final DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public DataClassificationRulesDao(
      DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub,
      ClientConfig clientConfig) {
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  public List<DataType> getResolvedDataTypesInEvaluationOrder(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getDataTypes(
                        GetDataTypesRequest.newBuilder()
                            .setResolveInheritedDetails(true)
                            .setOrdering(DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                            .setFilter(
                                DataTypeFilter.newBuilder().setEnabled(true).setLegacyTypes(false))
                            .build()))
        .getDataTypesList();
  }

  public Set<String> getEnabledLegacyDataTypeIds(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getDataTypes(
                        GetDataTypesRequest.newBuilder()
                            .setFilter(
                                DataTypeFilter.newBuilder().setEnabled(true).setLegacyTypes(true))
                            .setResolveInheritedDetails(true)
                            .build()))
        .getDataTypesList()
        .stream()
        .map(DataType::getId)
        .collect(Collectors.toUnmodifiableSet());
  }

  public List<DataClassificationOverride> getUnscopedDataSuppressionOverrideRules(
      RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getDataClassificationOverrides(
                        GetDataClassificationOverridesRequest.getDefaultInstance()))
        .getDataClassificationOverridesList()
        .stream()
        .filter(override -> !override.getDataClassificationOverrideRule().hasScope())
        .collect(Collectors.toUnmodifiableList());
  }

  public List<DataClassificationOverride> getDataSuppressionOverrideRulesForEnvironment(
      RequestContext requestContext, String environment) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getDataClassificationOverrides(
                        GetDataClassificationOverridesRequest.newBuilder()
                            .setFilter(
                                DataClassificationOverrideFilter.newBuilder()
                                    .setScopeFilter(
                                        ScopeFilter.newBuilder()
                                            .setIncludePartialMatches(true)
                                            .addScopes(
                                                DataClassificationOverrideScope.newBuilder()
                                                    .setEnvironmentScope(
                                                        EnvironmentScope.newBuilder()
                                                            .setEnvironmentId(environment)))))
                            .build()))
        .getDataClassificationOverridesList();
  }
}
