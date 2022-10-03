package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataClassificationOverrideScope;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.ScopeFilter;
import com.google.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationRulesDao {
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
  private final DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;

  @Inject
  public DataClassificationRulesDao(
      DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub) {
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
  }

  public List<DataType> getAllDataTypes(RequestContext requestContext) {
    return requestContext.call(
        () ->
            dataClassificationConfigServiceBlockingStub
                .withDeadlineAfter(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .getDataTypes(GetDataTypesRequest.getDefaultInstance())
                .getDataTypesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }

  public List<DataSet> getAllDataSets(RequestContext requestContext) {
    return requestContext.call(
        () ->
            dataClassificationConfigServiceBlockingStub
                .withDeadlineAfter(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .getDataSets(GetDataSetsRequest.getDefaultInstance())
                .getDataSetsList());
  }

  public Optional<DataSuppression> getDataSuppressionOverride(
      RequestContext requestContext, String environmentId) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .getDataClassificationOverrides(
                        GetDataClassificationOverridesRequest.newBuilder()
                            .setFilter(
                                DataClassificationOverrideFilter.newBuilder()
                                    .setScopeFilter(
                                        ScopeFilter.newBuilder()
                                            .addScopes(
                                                DataClassificationOverrideScope.newBuilder()
                                                    .setEnvironmentScope(
                                                        EnvironmentScope.newBuilder()
                                                            .setEnvironmentId(environmentId)))))
                            .build())
                    .getDataClassificationOverridesList())
        .stream()
        .findFirst()
        .map(
            dataClassificationOverride ->
                dataClassificationOverride
                    .getDataClassificationOverrideRule()
                    .getDataSuppressionOverride()
                    .getDataSuppression());
  }
}
