package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule.Builder;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CustomSignatureRulesManager implements RulesManager {

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final CustomSignatureRuleConverter customSignatureRuleConverter;

  @Inject
  public CustomSignatureRulesManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      CustomSignatureRuleConverter customSignatureRuleConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.customSignatureRuleConverter = customSignatureRuleConverter;
  }

  @Override
  public List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetRulesFilter filter) {

    List<CustomSignatureRule> customSignatureRules = new ArrayList<>();
    GetAllConfigsRequest getAllRuleConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceNamespace(CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE)
            .setResourceName(CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME)
            .build();

    List<ContextSpecificConfig> contextSpecificConfigs =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            requestContext.getRequestHeaders(),
            () ->
                configServiceBlockingStub
                    .getAllConfigs(getAllRuleConfigsRequest)
                    .getContextSpecificConfigsList());

    for (ContextSpecificConfig contextSpecificConfig : contextSpecificConfigs) {
      try {
        CustomSignatureRule customSignatureRule =
            customSignatureRuleConverter.convert(contextSpecificConfig.getConfig());
        customSignatureRules.add(customSignatureRule);
      } catch (InvalidProtocolBufferException e) {
        log.error(
            "Unable to convert config to custom signature rule for rule id: {}",
            contextSpecificConfig.getContext(),
            e);
        throw new RuntimeException(e);
      }
    }

    if (filter != GetRulesFilter.getDefaultInstance()) {
      return customSignatureRules.stream()
          .filter(
              rule -> {
                boolean ruleIdAccept =
                    filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId());

                boolean eventTypeAccept =
                    filter.getEventTypesList().isEmpty()
                        || filter.getEventTypesList().contains(rule.getEffect().getEventType());

                boolean disabledAccept =
                    !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled());

                boolean internalAccept =
                    !(filter.hasInternal() && rule.getInternal() != filter.getInternal());

                return ruleIdAccept && eventTypeAccept && disabledAccept && internalAccept;
              })
          .collect(ImmutableList.toImmutableList());
    }

    return Collections.unmodifiableList(customSignatureRules);
  }

  @Override
  public Optional<CustomSignatureRule> createCustomSignatureRule(
      RequestContext requestContext, CreateCustomSignatureRuleRequest createRuleRequest) {
    String ruleId = generateRuleId();
    Builder customSignatureRuleBuilder =
        CustomSignatureRule.newBuilder()
            .setId(ruleId)
            .setName(createRuleRequest.getName())
            .setDescription(createRuleRequest.getDescription())
            .setDefinition(createRuleRequest.getDefinition())
            .setEffect(createRuleRequest.getEffect())
            .setDisabled(false)
            .setInternal(false);
    if (createRuleRequest.hasBlockingExpiryDetails()) {
      updateExpiryDetails(customSignatureRuleBuilder, createRuleRequest.getBlockingExpiryDetails());
    }
    return upsertConfig(requestContext, customSignatureRuleBuilder.build());
  }

  @Override
  public Optional<CustomSignatureRule> updateCustomSignatureRule(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    String ruleId = customSignatureRule.getId();
    if (getCustomSignatureRule(requestContext, ruleId).isEmpty()) {
      return Optional.empty();
    }
    Builder customSignatureRuleBuilder = CustomSignatureRule.newBuilder(customSignatureRule);
    if (customSignatureRule.hasBlockingExpiryDetails()) {
      updateExpiryDetails(
          customSignatureRuleBuilder, customSignatureRule.getBlockingExpiryDetails());
    }
    return upsertConfig(requestContext, customSignatureRuleBuilder.build());
  }

  @Override
  public boolean deleteCustomSignatureRule(RequestContext requestContext, String id) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceNamespace(CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE)
            .setResourceName(CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME)
            .setContext(id)
            .build();
    try {
      GrpcClientRequestContextUtil.executeWithHeadersContext(
          requestContext.getRequestHeaders(),
          () -> configServiceBlockingStub.deleteConfig(deleteConfigRequest));
      return true;
    } catch (RuntimeException e) {
      log.error("Unable to delete custom signature rule {}", id, e);
      return false;
    }
  }

  private Optional<Value> getCustomSignatureRule(RequestContext requestContext, String ruleId) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addContexts(ruleId)
              .setResourceNamespace(CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE)
              .setResourceName(CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME)
              .build();

      return Optional.ofNullable(
              GrpcClientRequestContextUtil.executeWithHeadersContext(
                  requestContext.getRequestHeaders(),
                  () -> configServiceBlockingStub.getConfig(getConfigRequest).getConfig()))
          .filter(value -> value.getKindCase() != Value.KindCase.KIND_NOT_SET);

    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<CustomSignatureRule> upsertConfig(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    UpsertConfigRequest upsertConfigRequest;

    try {
      upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceNamespace(CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE)
              .setResourceName(CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME)
              .setConfig(customSignatureRuleConverter.convert(customSignatureRule))
              .setContext(customSignatureRule.getId())
              .build();
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert custom signature rule {} to config object", customSignatureRule);
      return Optional.empty();
    }

    UpsertConfigResponse response;
    try {
      response =
          GrpcClientRequestContextUtil.executeWithHeadersContext(
              requestContext.getRequestHeaders(),
              () -> configServiceBlockingStub.upsertConfig(upsertConfigRequest));
    } catch (RuntimeException e) {
      log.error("Unable to update custom signature rule {}", customSignatureRule);
      return Optional.empty();
    }

    try {
      return Optional.ofNullable(customSignatureRuleConverter.convert(response.getConfig()));
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert config response {} to custom signature rule", response);
      return Optional.empty();
    }
  }

  private void updateExpiryDetails(
      Builder customSignatureRuleBuilder, ExpiryDetails expiryDetails) {
    ExpiryDetails.Builder expiryDetailsBuilder = ExpiryDetails.newBuilder(expiryDetails);
    if (expiryDetails.hasExpiryDuration() && !expiryDetails.hasExpiryTimestampMillis()) {
      expiryDetailsBuilder.setExpiryTimestampMillis(
          System.currentTimeMillis()
              + Duration.parse(expiryDetails.getExpiryDuration()).toMillis());
    } else if (expiryDetails.hasExpiryTimestampMillis() && !expiryDetails.hasExpiryDuration()) {
      expiryDetailsBuilder.setExpiryDuration(
          Duration.ofMillis(expiryDetails.getExpiryTimestampMillis() - System.currentTimeMillis())
              .toString());
    }
    customSignatureRuleBuilder.setBlockingExpiryDetails(expiryDetailsBuilder.build());
  }
}
