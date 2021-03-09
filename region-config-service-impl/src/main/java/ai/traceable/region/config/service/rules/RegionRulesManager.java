package ai.traceable.region.config.service.rules;

import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.region.config.service.v1.RegionRule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;

@Slf4j
class RegionRulesManager implements RulesManager {

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final RegionRuleConverter regionRuleConverter;

  @Inject
  RegionRulesManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      RegionRuleConverter regionRuleConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.regionRuleConverter = regionRuleConverter;
  }

  @Override
  public List<RegionRule> getRegionRules() {
    GetAllConfigsRequest getAllRuleConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
            .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
            .build();

    List<ContextSpecificConfig> contextSpecificConfigs =
        configServiceBlockingStub
            .getAllConfigs(getAllRuleConfigsRequest)
            .getContextSpecificConfigsList();

    List<RegionRule> regionRules = new ArrayList<>();
    for (ContextSpecificConfig contextSpecificConfig : contextSpecificConfigs) {
      String ruleId = contextSpecificConfig.getContext();
      try {
        RegionRule regionRule = regionRuleConverter.convert(contextSpecificConfig.getConfig());
        regionRules.add(regionRule);
      } catch (InvalidProtocolBufferException e) {
        log.error("Unable to convert config to region rule for rule id: {}", ruleId);
      }
    }

    return ImmutableList.<RegionRule>builder().addAll(regionRules).build();
  }
}
