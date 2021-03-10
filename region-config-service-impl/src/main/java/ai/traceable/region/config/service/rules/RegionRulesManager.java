package ai.traceable.region.config.service.rules;

import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.region.config.service.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@Slf4j
class RegionRulesManager implements RulesManager {

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final RegionRuleConverter regionRuleConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  RegionRulesManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      RegionRuleConverter regionRuleConverter,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.regionRuleConverter = regionRuleConverter;
    this.uuidGenerator = uuidGenerator;
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
        log.error("Unable to convert config to region rule for rule id: {}", ruleId, e);
      }
    }

    return ImmutableList.<RegionRule>builder().addAll(regionRules).build();
  }

  @Override
  public Optional<RegionRule> createRegionRule(CreateRegionRuleRequest createRuleRequest) {
    String ruleId = this.uuidGenerator.generateId();
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId(ruleId)
            .addAllRegionId(createRuleRequest.getRegionIdList())
            .setName(createRuleRequest.getName())
            .setActionType(createRuleRequest.getActionType())
            .setExpirationMillis(createRuleRequest.getExpirationMillis())
            .build();

    UpsertConfigRequest upsertConfigRequest;
    try {
      upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
              .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
              .setConfig(regionRuleConverter.convert(regionRule))
              .setContext(ruleId)
              .build();
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert region rule {} to config object", regionRule);
      return Optional.empty();
    }

    UpsertConfigResponse response;
    try {
      response = configServiceBlockingStub.upsertConfig(upsertConfigRequest);
    } catch (RuntimeException e) {
      log.error("Unable to create region rule for request {}", createRuleRequest);
      return Optional.empty();
    }

    try {
      return Optional.ofNullable(regionRuleConverter.convert(response.getConfig()));
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert config response {} to region rule", response);
      return Optional.empty();
    }
  }

  @Override
  public Optional<RegionRule> updateRegionRule(RegionRule regionRule) {
    String ruleId = regionRule.getId();
    if (!doesRegionRuleExist(ruleId)) {
      return Optional.empty();
    }

    UpsertConfigRequest upsertConfigRequest;

    try {
      upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setContext(ruleId)
              .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
              .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
              .setConfig(regionRuleConverter.convert(regionRule))
              .build();
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert region rule {} to config object", regionRule);
      return Optional.empty();
    }

    UpsertConfigResponse response;
    try {
      response = configServiceBlockingStub.upsertConfig(upsertConfigRequest);
    } catch (RuntimeException e) {
      log.error("Unable to update region rule {}", regionRule);
      return Optional.empty();
    }

    try {
      return Optional.ofNullable(regionRuleConverter.convert(response.getConfig()));
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert config response {} to region rule", response);
      return Optional.empty();
    }
  }

  @Override
  public boolean deleteRegionRule(String id) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
            .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
            .setContext(id)
            .build();

    try {
      configServiceBlockingStub.deleteConfig(deleteConfigRequest);
      return true;
    } catch (RuntimeException e) {
      log.error("Unable to delete region rule {}", id, e);
      return false;
    }
  }

  private boolean doesRegionRuleExist(String ruleId) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addContexts(ruleId)
              .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
              .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
              .build();
      configServiceBlockingStub.getConfig(getConfigRequest);
      return true;
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return false;
      }
    }
    return false;
  }
}
