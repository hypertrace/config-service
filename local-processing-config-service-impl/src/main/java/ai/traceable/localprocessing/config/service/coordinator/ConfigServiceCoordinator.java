package ai.traceable.localprocessing.config.service.coordinator;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.ModsecConfig;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicies;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigServiceCoordinator {

  LocalProcessingRuleDetails createLocalProcessingRule(
      RequestContext requestContext, NewLocalProcessingRule newLocalProcessingRule);

  LocalProcessingRuleDetails updateLocalProcessingRule(
      RequestContext requestContext, LocalProcessingRule localProcessingRule);

  List<LocalProcessingRuleDetails> getAllLocalProcessingRules(RequestContext requestContext);

  void deleteLocalProcessingRule(RequestContext requestContext, String localProcessingRuleId);

  ProtectionMode upsertDefaultProtectionModeConfig(
      RequestContext requestContext, ProtectionMode defaultProtectionMode);

  ProtectionMode getDefaultProtectionModeConfig(RequestContext requestContext);

  SamplingPolicies getSamplingPoliciesConfig();

  ModsecConfig getModsecConfig(boolean shouldUseCoraza);
}
