package ai.traceable.data.classification.config.service;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
class DataTypeResolver {
  private static final int THREAD_POOL_SIZE = 1;
  private static final int MAX_CACHE_SIZE = 100;
  private static final Duration REFRESH_DURATION = Duration.ofMinutes(2);
  private static final Duration EXPIRATION_DURATION = Duration.ofMinutes(10);

  private static final Comparator<DataSet> DATA_SET_PRECEDENCE_COMPARATOR =
      Comparator.comparing((DataSet dataSet) -> dataSet.getInfo().getEnabled())
          .reversed() // true values then false
          .thenComparing(DataTypeResolver::calculateDataSetPrecedence);

  private final DataClassificationConfigServiceBlockingStub stub;

  private final LoadingCache<ContextualKey<Void>, Map<String, DataSet>>
      primaryDataSetByDataTypeIdCache =
          CacheBuilder.newBuilder()
              .refreshAfterWrite(REFRESH_DURATION)
              .expireAfterAccess(EXPIRATION_DURATION)
              .maximumSize(MAX_CACHE_SIZE)
              .recordStats()
              .build(
                  CacheLoader.asyncReloading(
                      CacheLoader.from(this::resolvePrimaryDataSetForEachDataTypeId),
                      this.buildExecutor()));

  List<DataType> resolveInheritedDataTypeFields(
      RequestContext requestContext, List<DataType> dataTypeList) {
    Map<String, DataSet> primaryDataSetByDataTypeId =
        this.primaryDataSetByDataTypeIdCache.getUnchecked(
            requestContext.buildInternalContextualKey());

    return dataTypeList.stream()
        .map(
            dataType ->
                Optional.ofNullable(primaryDataSetByDataTypeId.get(dataType.getId()))
                    .map(dataSet -> this.resolveDataTypeWithDataSet(dataType, dataSet))
                    .orElse(dataType))
        .collect(toUnmodifiableList());
  }

  private Map<String, DataSet> resolvePrimaryDataSetForEachDataTypeId(ContextualKey<Void> key) {
    return key
        .callInContext(() -> this.stub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
        .getDataSetsList()
        .stream()
        .sorted(DATA_SET_PRECEDENCE_COMPARATOR)
        .map(this::flattenToDataTypeRelationships)
        .flatMap(Collection::stream)
        .collect(
            Collectors.toUnmodifiableMap(
                DataTypeDataSetRelationship::getDataTypeId,
                DataTypeDataSetRelationship::getDataSet,
                (first, second) -> first));
  }

  private DataType resolveDataTypeWithDataSet(DataType dataType, DataSet dataSet) {
    DataType.Builder builder = dataType.toBuilder();
    if (!dataType.getRule().hasEnabled()) {
      builder.getRuleBuilder().setEnabled(dataSet.getInfo().getEnabled());
    }
    if (dataType.getRule().getDataSuppression() == DataSuppression.DATA_SUPPRESSION_UNSPECIFIED) {
      builder.getRuleBuilder().setDataSuppression(dataSet.getInfo().getDataSuppression());
    }
    if (dataType.getRule().getSensitivity() == Sensitivity.SENSITIVITY_UNSPECIFIED) {
      builder.getRuleBuilder().setSensitivity(dataSet.getInfo().getSensitivity());
    }
    if (!dataType.getRule().hasColor() && dataSet.getInfo().hasColor()) {
      builder.getRuleBuilder().setColor(dataSet.getInfo().getColor());
    }
    return builder.build();
  }

  private List<DataTypeDataSetRelationship> flattenToDataTypeRelationships(DataSet dataSet) {
    return dataSet.getInfo().getDataTypeIdsList().stream()
        .map(id -> new DataTypeDataSetRelationship(id, dataSet))
        .collect(toUnmodifiableList());
  }

  private ExecutorService buildExecutor() {
    return Executors.newFixedThreadPool(
        THREAD_POOL_SIZE,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("data-type-resolver-data-set-cache-%d")
            .build());
  }

  private static int calculateDataSetPrecedence(DataSet dataSet) {
    switch (dataSet.getInfo().getDataSuppression()) {
      case DATA_SUPPRESSION_REDACT:
        return 0;
      case DATA_SUPPRESSION_OBFUSCATE:
        return 1;
      case DATA_SUPPRESSION_RAW:
        return 2;
      case UNRECOGNIZED:
      case DATA_SUPPRESSION_UNSPECIFIED:
      default:
        return Integer.MAX_VALUE;
    }
  }

  @Value
  private static class DataTypeDataSetRelationship {
    String dataTypeId;
    DataSet dataSet;
  }
}
