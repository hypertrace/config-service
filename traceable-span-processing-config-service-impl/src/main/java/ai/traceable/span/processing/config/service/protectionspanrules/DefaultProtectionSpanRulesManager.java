package ai.traceable.span.processing.config.service.protectionspanrules;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.span.processing.config.service.licensestatus.LicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.store.ProtectionSpanRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleDetails;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleMetadata;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultProtectionSpanRulesManager implements ProtectionSpanRulesManager {

  private final TimestampConverter timestampConverter;
  private final LicenseStatusConfigManager licenseStatusConfigManager;
  private final ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore;

  @Inject
  public DefaultProtectionSpanRulesManager(
      ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore,
      TimestampConverter timestampConverter,
      LicenseStatusConfigManager licenseStatusConfigManager) {
    this.timestampConverter = timestampConverter;
    this.protectionSpanRulesConfigStore = protectionSpanRulesConfigStore;
    this.licenseStatusConfigManager = licenseStatusConfigManager;
  }

  @Override
  public List<ProtectionSpanRuleDetails> getAllProtectionSpanRuleDetails(
      RequestContext requestContext) {
    return this.protectionSpanRulesConfigStore.getAllData(requestContext);
  }

  @Override
  public List<ProtectionSpanRule> getAllResolvedProtectionSpanRule(RequestContext requestContext) {
    LicenseStatus licenseStatus = licenseStatusConfigManager.getLicenseStatus(requestContext);
    if (!licenseStatus.hasProtectionLicense()
        || LICENSE_LIMIT_EXHAUSTED.equals(licenseStatus.getTracesLicenseLimit())) {
      return Collections.emptyList();
    }
    return getAllProtectionSpanRules(requestContext);
  }

  @Override
  public ProtectionSpanRuleDetails createProtectionSpanRule(
      RequestContext requestContext,
      CreateProtectionSpanRuleRequest createProtectionSpanRuleRequest) {
    // TODO: need to handle priorities
    ProtectionSpanRule newRule =
        ProtectionSpanRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setRuleInfo(createProtectionSpanRuleRequest.getRuleInfo())
            .build();

    return buildProtectionSpanRuleDetails(
        this.protectionSpanRulesConfigStore.upsertObject(requestContext, newRule));
  }

  @Override
  public ProtectionSpanRuleDetails updateProtectionSpanRule(
      RequestContext requestContext,
      UpdateProtectionSpanRuleRequest updateProtectionSpanRuleRequest)
      throws StatusException {
    // TODO: need to handle priorities
    UpdateProtectionSpanRule updateProtectionSpanRule = updateProtectionSpanRuleRequest.getRule();
    ProtectionSpanRule existingRule =
        this.protectionSpanRulesConfigStore
            .getData(requestContext, updateProtectionSpanRule.getId())
            .orElseThrow(Status.NOT_FOUND::asException);
    ProtectionSpanRule updatedRule = buildUpdatedRule(existingRule, updateProtectionSpanRule);
    return buildProtectionSpanRuleDetails(
        this.protectionSpanRulesConfigStore.upsertObject(requestContext, updatedRule));
  }

  @Override
  public void deleteProtectionSpanRule(
      RequestContext requestContext,
      DeleteProtectionSpanRuleRequest deleteProtectionSpanRuleRequest) {
    // TODO: need to handle priorities
    this.protectionSpanRulesConfigStore
        .deleteObject(requestContext, deleteProtectionSpanRuleRequest.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private List<ProtectionSpanRule> getAllProtectionSpanRules(RequestContext requestContext) {
    return getAllProtectionSpanRuleDetails(requestContext).stream()
        .map(ProtectionSpanRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private ProtectionSpanRuleDetails buildProtectionSpanRuleDetails(
      ContextualConfigObject<ProtectionSpanRule> configObject) {
    return ProtectionSpanRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ProtectionSpanRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private ProtectionSpanRule buildUpdatedRule(
      ProtectionSpanRule existingRule, UpdateProtectionSpanRule updateProtectionSpanRule) {
    return ProtectionSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            ProtectionSpanRuleInfo.newBuilder()
                .setName(updateProtectionSpanRule.getName())
                .setFilter(updateProtectionSpanRule.getFilter())
                .setDisabled(updateProtectionSpanRule.getDisabled())
                .build())
        .build();
  }
}
