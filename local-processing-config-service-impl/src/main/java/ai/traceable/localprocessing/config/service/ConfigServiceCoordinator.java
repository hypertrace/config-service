package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigServiceCoordinator {

  LocalProcessingRuleDetails createLocalProcessingRule(
      RequestContext requestContext, NewLocalProcessingRule newLocalProcessingRule);

  LocalProcessingRuleDetails updateLocalProcessingRule(
      RequestContext requestContext, LocalProcessingRule localProcessingRule);

  List<LocalProcessingRuleDetails> getAllLocalProcessingRules(RequestContext requestContext);

  void deleteLocalProcessingRule(RequestContext requestContext, String localProcessingRuleId);
}
