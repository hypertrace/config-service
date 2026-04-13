package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.data.classification.config.service.obfuscation.DataObfuscationRulesManager;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.FullValueRegexDataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
class ExternalDataClassificationRuleResponseBuilder {
  private final UuidGenerator uuidGenerator;
  private final FeatureCachingClient featureClient;
  private final DataObfuscationRulesManager dataObfuscationRulesManager;

  GetDataClassificationConfigResponse buildDisabledResponse() {
    return GetDataClassificationConfigResponse.newBuilder().setEnabled(false).build();
  }

  GetDataClassificationConfigResponse buildEnabledResponse(
      GetDataClassificationConfigRequest request,
      RequestContext requestContext,
      List<DataType> dataTypes,
      List<DataParsingRule> dataParsingRules,
      List<FullValueRegexDataType> fullValueRegexDataTypes) {
    GetDataClassificationConfigResponse.Builder responseBuilder =
        GetDataClassificationConfigResponse.newBuilder()
            .setEnabled(true)
            .addAllDataTypes(dataTypes)
            .addAllDataParsingRules(dataParsingRules)
            .addAllFullValueRegexDataTypes(fullValueRegexDataTypes);
    if (this.featureClient.isDataClassificationEnhancedObfuscationEnabled(requestContext)) {
      responseBuilder.setObfuscationStrategy(
          this.dataObfuscationRulesManager.getObfuscationStrategy(requestContext));
    }

    String responseHash = uuidGenerator.generateId(responseBuilder.build());
    responseBuilder.setHash(responseHash);

    if (responseHash.equals(request.getChangeFilter().getPreviousHash())) {
      return this.buildNoChangeResponseForHash(responseHash);
    }
    return responseBuilder.build();
  }

  GetDataClassificationConfigResponse buildNoChangeResponseForHash(String hash) {
    return GetDataClassificationConfigResponse.newBuilder().setHash(hash).build();
  }
}
