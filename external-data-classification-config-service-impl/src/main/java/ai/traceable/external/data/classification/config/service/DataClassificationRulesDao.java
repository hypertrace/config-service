package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationRulesDao {
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
                .getDataTypes(GetDataTypesRequest.getDefaultInstance())
                .getDataTypesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }

  public List<DataSet> getAllDataSets(RequestContext requestContext) {
    return requestContext.call(
        () ->
            dataClassificationConfigServiceBlockingStub
                .getDataSets(GetDataSetsRequest.getDefaultInstance())
                .getDataSetsList());
  }
}
