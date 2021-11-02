package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule.Builder;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CustomSignatureRulesManager extends IdentifiedObjectStore<CustomSignatureRule>
    implements RulesManager {

  private final CustomSignatureRuleConverter customSignatureRuleConverter;

  @Inject
  public CustomSignatureRulesManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      CustomSignatureRuleConverter customSignatureRuleConverter) {
    super(
        configServiceBlockingStub,
        CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE,
        CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME);
    this.customSignatureRuleConverter = customSignatureRuleConverter;
  }

  @Override
  protected Optional<CustomSignatureRule> buildDataFromValue(Value value) {
    try {
      return Optional.of(customSignatureRuleConverter.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to custom signature rule for rule: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(CustomSignatureRule data) {
    return customSignatureRuleConverter.convert(data);
  }

  @Override
  protected String getContextFromData(CustomSignatureRule data) {
    return data.getId();
  }

  @Override
  public List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetRulesFilter filter) {

    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .filter(rule -> this.ruleMatchesFilter(rule, filter))
        .collect(ImmutableList.toImmutableList());
  }

  private boolean ruleMatchesFilter(CustomSignatureRule rule, GetRulesFilter filter) {
    if (Objects.equals(filter, GetRulesFilter.getDefaultInstance())) {
      return true;
    }
    boolean ruleIdAccept =
        filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId());

    boolean eventTypeAccept =
        filter.getEventTypesList().isEmpty()
            || filter.getEventTypesList().contains(rule.getEffect().getEventType());

    boolean disabledAccept = !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled());

    boolean internalAccept = !(filter.hasInternal() && rule.getInternal() != filter.getInternal());

    return ruleIdAccept && eventTypeAccept && disabledAccept && internalAccept;
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
  public CustomSignatureRule deleteCustomSignatureRule(RequestContext requestContext, String id) {
    return this.deleteObject(requestContext, id)
        .map(ContextualConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private Optional<CustomSignatureRule> getCustomSignatureRule(
      RequestContext requestContext, String ruleId) {
    try {
      return this.getData(requestContext, ruleId);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<CustomSignatureRule> upsertConfig(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    try {
      return Optional.of(this.upsertObject(requestContext, customSignatureRule).getData());
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
