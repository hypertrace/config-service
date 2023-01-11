package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleFilter;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ServiceNamingRulesManager {

  List<ServiceNamingRule> getRules(RequestContext requestContext, ServiceNamingRuleFilter filter);

  void deleteRule(RequestContext requestContext, String id);

  ServiceNamingRule updateRule(
      RequestContext requestContext, UpdateServiceNamingRuleRequest updateRequest);

  ServiceNamingRule createRule(
      RequestContext requestContext, CreateServiceNamingRuleRequest createRequest);
}
