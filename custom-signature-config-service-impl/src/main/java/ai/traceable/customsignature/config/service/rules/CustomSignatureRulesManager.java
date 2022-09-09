package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule.Builder;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CustomSignatureRulesManager implements RulesManager {

  private final CustomSignatureRulesStore rulesStore;

  @Inject
  public CustomSignatureRulesManager(CustomSignatureRulesStore rulesStore) {
    this.rulesStore = rulesStore;
  }

  @Override
  public List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetRulesFilter filter) {
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllConfigData(requestContext);
    }
    return rulesStore.getAllConfigData(requestContext, filter);
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
            .setRuleScope(createRuleRequest.getRuleScope())
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
  public CustomSignatureRule deleteCustomSignatureRule(RequestContext requestContext, String id) {
    return rulesStore
        .deleteObject(requestContext, id)
        .map(ContextualConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private Optional<CustomSignatureRule> getCustomSignatureRule(
      RequestContext requestContext, String ruleId) {
    try {
      return rulesStore.getData(requestContext, ruleId);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<CustomSignatureRule> upsertConfig(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    try {
      return Optional.of(rulesStore.upsertObject(requestContext, customSignatureRule).getData());
    } catch (Exception exception) {
      log.error("Unable to update custom signature rule {}", customSignatureRule, exception);
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
