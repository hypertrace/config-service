package ai.traceable.genai.system.discovery.config.service.v1.manager;

import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GenAiSystemDiscoveryRuleManager {

  List<GenAiSystemDiscoveryRule> getGenAiSystemDiscoveryRules(
      RequestContext requestContext, GetGenAiSystemDiscoveryRulesFilter filter);

  GenAiSystemDiscoveryRule createGenAiSystemDiscoveryRule(
      RequestContext requestContext, CreateGenAiSystemDiscoveryRuleRequest createRuleRequest);

  GenAiSystemDiscoveryRule updateGenAiSystemDiscoveryRule(
      RequestContext requestContext, UpdateGenAiSystemDiscoveryRuleRequest updateRuleRequest);

  void deleteGenAiSystemDiscoveryRule(RequestContext requestContext, String id);
}
