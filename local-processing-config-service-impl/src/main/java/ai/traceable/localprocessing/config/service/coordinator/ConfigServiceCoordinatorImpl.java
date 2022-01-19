package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.MODSEC_REDACT_MESSAGES;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.SAMPLING_POLICIES;

import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceConfig;
import ai.traceable.localprocessing.config.service.v1.*;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.ConfigObject;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ProtectionMode defaultProtectionMode;
  private final SamplingPolicies defaultSamplingPolicies;
  private final DefaultProtectionModeConfigStore defaultProtectionModeConfigStore;
  private final LocalProcessingRulesConfigStore localProcessingRulesConfigStore;
  private final ModsecConfig defaultModsecConfig;

  @Inject
  public ConfigServiceCoordinatorImpl(
      LocalProcessingConfigServiceConfig localProcessingConfigServiceConfig,
      DefaultProtectionModeConfigStore defaultProtectionModeConfigStore,
      LocalProcessingRulesConfigStore localProcessingRulesConfigStore) {
    this.defaultProtectionMode =
        ProtectionMode.valueOf(
            localProcessingConfigServiceConfig
                .getConfig()
                .getConfig(LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG)
                .getString(DEFAULT_PROTECTION_MODE));
    this.defaultSamplingPolicies =
        getSamplingPoliciesFromConfig(localProcessingConfigServiceConfig);
    this.defaultProtectionModeConfigStore = defaultProtectionModeConfigStore;
    this.localProcessingRulesConfigStore = localProcessingRulesConfigStore;
    this.defaultModsecConfig =
        ModsecConfig.newBuilder()
            .setRedactMessages(
                localProcessingConfigServiceConfig
                    .getConfig()
                    .getConfig(LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG)
                    .getBoolean(MODSEC_REDACT_MESSAGES))
            .build();
  }

  @Override
  public LocalProcessingRuleDetails createLocalProcessingRule(
      RequestContext requestContext, NewLocalProcessingRule newLocalProcessingRule) {
    LocalProcessingRule localProcessingRule = buildLocalProcessingRule(newLocalProcessingRule);
    validateRule(localProcessingRule);
    ContextualConfigObject<LocalProcessingRule> upsertedLocalProcessingRule =
        this.localProcessingRulesConfigStore.upsertObject(requestContext, localProcessingRule);
    return buildLocalProcessingRuleDetails(
        upsertedLocalProcessingRule.getData(),
        upsertedLocalProcessingRule.getCreationTimestamp().toEpochMilli());
  }

  @Override
  public LocalProcessingRuleDetails updateLocalProcessingRule(
      RequestContext requestContext, LocalProcessingRule localProcessingRule) {
    validateRule(localProcessingRule);

    ContextualConfigObject<LocalProcessingRule> upsertedLocalProcessingRule =
        this.localProcessingRulesConfigStore.upsertObject(requestContext, localProcessingRule);

    return buildLocalProcessingRuleDetails(
        upsertedLocalProcessingRule.getData(),
        upsertedLocalProcessingRule.getCreationTimestamp().toEpochMilli());
  }

  @Override
  public List<LocalProcessingRuleDetails> getAllLocalProcessingRules(
      RequestContext requestContext) {
    return this.localProcessingRulesConfigStore.getAllObjects(requestContext).stream()
        .map(
            contextSpecificConfig ->
                buildLocalProcessingRuleDetails(
                    contextSpecificConfig.getData(),
                    contextSpecificConfig.getCreationTimestamp().toEpochMilli()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteLocalProcessingRule(
      RequestContext requestContext, String localProcessingRuleId) {
    this.localProcessingRulesConfigStore.deleteObject(requestContext, localProcessingRuleId);
  }

  @Override
  public ProtectionMode upsertDefaultProtectionModeConfig(
      RequestContext requestContext, ProtectionMode defaultProtectionMode) {
    return this.defaultProtectionModeConfigStore
        .upsertObject(
            requestContext,
            DefaultProtectionModeConfig.newBuilder()
                .setDefaultProtectionMode(defaultProtectionMode)
                .build())
        .getData()
        .getDefaultProtectionMode();
  }

  @Override
  public ProtectionMode getDefaultProtectionModeConfig(RequestContext requestContext) {
    return this.defaultProtectionModeConfigStore
        .getData(requestContext)
        .map(DefaultProtectionModeConfig::getDefaultProtectionMode)
        .orElse(defaultProtectionMode);
  }

  @Override
  public SamplingPolicies getSamplingPoliciesConfig() {
    return defaultSamplingPolicies;
  }

  @Override
  public ModsecConfig getModsecConfig() {
    return this.defaultModsecConfig;
  }

  private void validateRule(LocalProcessingRule localProcessingRule) {
    Preconditions.checkArgument(
        localProcessingRule.getProtectionMode() != ProtectionMode.PROTECTION_MODE_UNSPECIFIED,
        "Protection mode can't be unspecified");
  }

  private LocalProcessingRule buildLocalProcessingRule(
      NewLocalProcessingRule newLocalProcessingRule) {
    String ruleId = UUID.randomUUID().toString();
    return LocalProcessingRule.newBuilder()
        .setId(ruleId)
        .setUrlPattern(newLocalProcessingRule.getUrlPattern())
        .setHostHeader(newLocalProcessingRule.getHostHeader())
        .setProtectionMode(newLocalProcessingRule.getProtectionMode())
        .build();
  }

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      LocalProcessingRule localProcessingRule, long creationTimestamp) {
    return LocalProcessingRuleDetails.newBuilder()
        .setRule(localProcessingRule)
        .setMetadata(
            LocalProcessingRuleMetadata.newBuilder()
                .setCreationTimestamp(creationTimestamp)
                .build())
        .build();
  }

  private SamplingPolicy convertSamplingPolicyFromGeneric(Value config) {
    SamplingPolicy.Builder builder = SamplingPolicy.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(config, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }

  private SamplingPolicies getSamplingPoliciesFromConfig(
      LocalProcessingConfigServiceConfig localProcessingConfigServiceConfig) {
    List<? extends ConfigObject> samplingPolicies =
        localProcessingConfigServiceConfig
            .getConfig()
            .getConfig(LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG)
            .getObjectList(SAMPLING_POLICIES);
    List<SamplingPolicy> samplingPolicyList =
        samplingPolicies.stream()
            .map(this::buildSamplingPolicyFromConfig)
            .collect(Collectors.toUnmodifiableList());
    return SamplingPolicies.newBuilder().addAllPolicies(samplingPolicyList).build();
  }

  @SneakyThrows
  private SamplingPolicy buildSamplingPolicyFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    SamplingPolicy.Builder builder = SamplingPolicy.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
