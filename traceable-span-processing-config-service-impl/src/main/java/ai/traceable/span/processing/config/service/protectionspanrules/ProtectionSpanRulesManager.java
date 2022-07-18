package ai.traceable.span.processing.config.service.protectionspanrules;

import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleDetails;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import io.grpc.StatusException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ProtectionSpanRulesManager {
  List<ProtectionSpanRuleDetails> getAllProtectionSpanRuleDetails(RequestContext requestContext);

  List<ProtectionSpanRule> getAllResolvedProtectionSpanRule(RequestContext requestContext);

  ProtectionSpanRuleDetails createProtectionSpanRule(
      RequestContext requestContext,
      CreateProtectionSpanRuleRequest createProtectionSpanRuleRequest);

  ProtectionSpanRuleDetails updateProtectionSpanRule(
      RequestContext requestContext,
      UpdateProtectionSpanRuleRequest updateProtectionSpanRuleRequest)
      throws StatusException;

  void deleteProtectionSpanRule(
      RequestContext requestContext,
      DeleteProtectionSpanRuleRequest deleteProtectionSpanRuleRequest);
}
