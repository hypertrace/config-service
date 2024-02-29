package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.FROM_LEGACY_REDACTION_RULE;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.ORPHAN_DATA_TYPE;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.RESOLVED_FROM_DATA_SET;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.STANDALONE_DATA_TYPE;
import static ai.traceable.data.classification.config.service.DataTypeResolver.DATA_SET_RESOLUTION_PRECEDENCE_COMPARATOR;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
class DataClassificationResolutionCache {
  private static final int THREAD_POOL_SIZE = 1;
  private static final int MAX_CACHE_SIZE = 100;
  private static final Duration REFRESH_DURATION = Duration.ofMinutes(1);
  private static final Duration EXPIRATION_DURATION = Duration.ofMinutes(30);
  private final DataClassificationConfigServiceBlockingStub stub;
  private final DataTypeResolver dataTypeResolver;
  private final LoadingCache<ContextualKey<List<DataType>>, List<DataTypeResolutionContext>>
      dataTypeResolutionContextCache;

  @Inject
  public DataClassificationResolutionCache(
      DataClassificationConfigServiceBlockingStub stub, DataTypeResolver dataTypeResolver) {
    this.stub = stub;
    this.dataTypeResolver = dataTypeResolver;

    this.dataTypeResolutionContextCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(REFRESH_DURATION)
            .expireAfterAccess(EXPIRATION_DURATION)
            .maximumSize(MAX_CACHE_SIZE)
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::buildResolutionContexts), this.buildExecutor()));

    PlatformMetricsRegistry.registerCache(
        "DataClassificationResolutionCache",
        this.dataTypeResolutionContextCache,
        Collections.emptyMap());
  }

  List<DataTypeResolutionContext> getDataTypeResolutions(
      RequestContext requestContext, List<DataType> dataTypes) {
    return this.dataTypeResolutionContextCache.getUnchecked(
        requestContext.buildInternalContextualKey(dataTypes));
  }

  private ExecutorService buildExecutor() {
    return Executors.newFixedThreadPool(
        THREAD_POOL_SIZE,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("data-classification-resolution-cache-%d")
            .build());
  }

  private List<DataTypeResolutionContext> buildResolutionContexts(
      ContextualKey<List<DataType>> key) {
    List<DataSet> orderedDataSets =
        key
            .callInContext(() -> this.stub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
            .getDataSetsList()
            .stream()
            .sorted(DATA_SET_RESOLUTION_PRECEDENCE_COMPARATOR)
            .collect(toUnmodifiableList());
    Map<String, DataType> dataTypeMap = Maps.uniqueIndex(key.getData(), DataType::getId);
    Map<String, DataSet> dataSetMap = Maps.uniqueIndex(orderedDataSets, DataSet::getId);
    return Stream.of(
            this.buildContextFragmentsFromDataTypes(key.getData(), dataSetMap),
            this.buildContextFragmentsFromDataSets(orderedDataSets, dataTypeMap))
        .flatMap(Collection::stream)
        .collect(
            Collectors.groupingBy(
                DataTypeResolutionContextFragment::getDataType,
                LinkedHashMap::new, // maintain original iteration order
                Collectors.toList()))
        .values()
        .stream()
        .map(this::mergeContextFragments)
        .collect(toUnmodifiableList());
  }

  private List<DataTypeResolutionContextFragment> buildContextFragmentsFromDataSets(
      Collection<DataSet> dataSets, Map<String, DataType> dataTypeMap) {
    AtomicInteger encounterOrder = new AtomicInteger(0);
    return dataSets.stream()
        .map(dataSet -> this.buildContextFragmentsFromDataSet(dataSet, encounterOrder, dataTypeMap))
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DataTypeResolutionContextFragment> buildContextFragmentsFromDataSet(
      DataSet dataSet, AtomicInteger encounterOrder, Map<String, DataType> dataTypeMap) {
    return dataSet.getInfo().getDataTypeIdsList().stream()
        .filter(dataTypeMap::containsKey)
        .map(dataTypeMap::get)
        .map(
            dataType ->
                new DataTypeResolutionContextFragment(
                    dataType,
                    List.of(dataSet),
                    DataTypeProvenance.RESOLVED_FROM_DATA_SET,
                    encounterOrder.getAndIncrement()))
        .collect(toUnmodifiableList());
  }

  private List<DataTypeResolutionContextFragment> buildContextFragmentsFromDataTypes(
      List<DataType> dataTypes, Map<String, DataSet> dataSetMap) {
    AtomicInteger encounterOrder = new AtomicInteger(0);
    return dataTypes.stream()
        .map(
            dataType ->
                this.buildContextFragmentsFromDataType(
                    dataType,
                    this.determineDataTypeProvenance(dataType),
                    encounterOrder,
                    dataSetMap))
        .collect(toUnmodifiableList());
  }

  private DataTypeResolutionContextFragment buildContextFragmentsFromDataType(
      DataType originalDataType,
      DataTypeProvenance provenance,
      AtomicInteger encounterOrder,
      Map<String, DataSet> dataSetMap) {
    List<DataSet> referencedDataSets =
        originalDataType.getRule().getDataSetIdList().stream()
            .filter(dataSetMap::containsKey)
            .map(dataSetMap::get)
            .collect(toUnmodifiableList());
    return new DataTypeResolutionContextFragment(
        originalDataType, referencedDataSets, provenance, encounterOrder.getAndIncrement());
  }

  private DataTypeProvenance determineDataTypeProvenance(DataType dataType) {
    if (this.dataTypeResolver.isLegacyDataType(dataType)) {
      return FROM_LEGACY_REDACTION_RULE;
    }
    if (this.dataTypeResolver.isStandAloneDataType(dataType)) {
      return STANDALONE_DATA_TYPE;
    } else {
      return ORPHAN_DATA_TYPE;
    }
  }

  private DataTypeResolutionContext mergeContextFragments(
      Collection<DataTypeResolutionContextFragment> contextFragments) {
    if (contextFragments.isEmpty()) {
      // This indicates a code bug,
      throw Status.INTERNAL.asRuntimeException();
    }
    List<DataSet> uniqueDataSetReferences =
        contextFragments.stream()
            .map(DataTypeResolutionContextFragment::getDataSets)
            .flatMap(Collection::stream)
            .distinct()
            .collect(toUnmodifiableList());

    // Links may be bidirectional - so determine the authoritative provenance to use in merged type
    return contextFragments.stream()
        .min(
            Ordering.explicit(
                    STANDALONE_DATA_TYPE,
                    FROM_LEGACY_REDACTION_RULE,
                    RESOLVED_FROM_DATA_SET,
                    ORPHAN_DATA_TYPE)
                .onResultOf(DataTypeResolutionContextFragment::getProvenance))
        .map(
            context ->
                new DataTypeResolutionContext(
                    context.getDataType(),
                    this.dataTypeResolver.resolve(context.getDataType(), uniqueDataSetReferences),
                    uniqueDataSetReferences,
                    context.getProvenance(),
                    context.getEncounterOrder()))
        .orElseThrow();
  }

  @Value
  private static class DataTypeResolutionContextFragment {
    DataType dataType;
    List<DataSet> dataSets;
    DataTypeProvenance provenance;
    int encounterOrder;
  }

  @Value
  static class DataTypeResolutionContext {
    DataType originalDataType;
    DataType resolvedDataType;
    List<DataSet> dataSets;
    DataTypeProvenance provenance;
    int encounterOrder;
  }

  enum DataTypeProvenance {

    // A standalone data type that fully defines its own behavior. Data sets are managed as labels
    // on the data type.
    STANDALONE_DATA_TYPE,
    // These are data types whose behavior is derived from a data set that references it.
    RESOLVED_FROM_DATA_SET,
    // Synthetic data types created as a reference to manage a legacy redaction rule
    FROM_LEGACY_REDACTION_RULE,
    // A data type that is neither referenced by a data set nor defines its own behavior, these
    // are not typically shown or used and eligible for cleanup.
    ORPHAN_DATA_TYPE
  }
}
