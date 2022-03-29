package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import com.google.inject.Inject;
import java.util.List;

class ExternalDataClassificationRuleResponseBuilder {
  private final UuidGenerator uuidGenerator;

  @Inject
  ExternalDataClassificationRuleResponseBuilder(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  GetDataClassificationConfigResponse buildResponse(
      GetDataClassificationConfigRequest request,
      List<DataType> dataTypes,
      List<DataParsingRule> dataParsingRules) {
    GetDataClassificationConfigResponse.Builder responseBuilder =
        GetDataClassificationConfigResponse.newBuilder()
            .addAllDataTypes(dataTypes)
            .addAllDataParsingRules(dataParsingRules);

    String responseHash = uuidGenerator.generateId(responseBuilder.build());
    responseBuilder.setHash(responseHash);

    if (responseHash.equals(request.getChangeFilter().getPreviousHash())) {
      return GetDataClassificationConfigResponse.newBuilder().setHash(responseHash).build();
    }
    return responseBuilder.build();
  }
}
