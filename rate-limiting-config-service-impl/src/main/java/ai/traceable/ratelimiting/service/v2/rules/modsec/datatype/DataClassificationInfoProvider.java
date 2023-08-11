package ai.traceable.ratelimiting.service.v2.rules.modsec.datatype;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
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
    return new DataClassificationInfo(
        fetchDataTypes(requestContext), fetchDatasets(requestContext));
  }

  @Value
  @Slf4j
  @Getter(AccessLevel.NONE)
  public static class DataClassificationInfo {
    Map<String, DataTypeRule> dataTypeRuleMap;
    Map<String, List<String>> dataSetToDataTypeMap;

    public List<String> getDataTypeIdsForDataSet(String datasetId) {
      try {
        return dataSetToDataTypeMap.get(datasetId);
      } catch (Exception e) {
        log.warn(
            "Error in finding the resolving the data types for data set id: {}. Skipping",
            datasetId);
      }
      return List.of();
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

  private Map<String, List<String>> fetchDatasets(RequestContext requestContext) {
    GetDataSetsResponse response =
        requestContext.call(
            () ->
                dataClassificationConfigServiceBlockingStub.getDataSets(
                    GetDataSetsRequest.getDefaultInstance()));
    return response.getDataSetsList().stream()
        .collect(
            Collectors.toMap(DataSet::getId, dataSet -> dataSet.getInfo().getDataTypeIdsList()));
  }

  private Map<String, DataTypeRule> fetchDataTypes(RequestContext requestContext) {
    GetDataTypesResponse response =
        requestContext.call(
            () ->
                dataClassificationConfigServiceBlockingStub.getDataTypes(
                    GetDataTypesRequest.getDefaultInstance()));
    return response.getDataTypesList().stream()
        .collect(Collectors.toMap(DataType::getId, DataType::getRule));
  }
}
