package ai.traceable.ratelimiting.service.v2.rules.modsec.datatype;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataClassificationInfoProvider {
  private final DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;

  @Inject
  public DataClassificationInfoProvider(
      DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub) {
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
  }

  public DataClassificationInfo fetchDataClassificationInfo(RequestContext requestContext) {
    List<DataType> dataTypes = this.fetchDataTypes(requestContext);
    Map<String, DataTypeRule> dataTypeRuleMap =
        dataTypes.stream()
            .collect(Collectors.toUnmodifiableMap(DataType::getId, DataType::getRule));
    ListMultimap<String, String> dataTypeIdsByDataSetId =
        dataTypes.stream()
            .collect(
                ImmutableListMultimap.flatteningToImmutableListMultimap(
                    DataType::getId, dataType -> dataType.getRule().getDataSetIdList().stream()))
            .inverse(); // collector is going data type id -> data set ids

    return new DataClassificationInfo(dataTypeRuleMap, dataTypeIdsByDataSetId);
  }

  private List<DataType> fetchDataTypes(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub.getDataTypes(
                    GetDataTypesRequest.newBuilder().setResolveInheritedDetails(true).build()))
        .getDataTypesList();
  }

  @Value
  @Slf4j
  @Getter(AccessLevel.NONE)
  public static class DataClassificationInfo {
    Map<String, DataTypeRule> dataTypeRuleMap;
    ListMultimap<String, String> dataTypeIdsByDataSetId;

    public List<String> getDataTypeIdsForDataSet(String datasetId) {
      return dataTypeIdsByDataSetId.get(datasetId);
    }

    public DataTypeRule getDataTypeRule(String dataTypeId) {
      try {
        return dataTypeRuleMap.get(dataTypeId);
      } catch (Exception e) {
        log.warn(
            "Error in finding the finding the data type rule corresponding to id: {}. Skipping",
            dataTypeId);
      }
      return null;
    }
  }
}
