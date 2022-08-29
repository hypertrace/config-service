package org.hypertrace.span.processing.config.service.apinamingrules;

import io.grpc.StatusException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRulesRequest;

public interface ApiNamingRulesManager {
  List<ApiNamingRuleDetails> getAllApiNamingRuleDetails(RequestContext requestContext);

  ApiNamingRuleDetails createApiNamingRule(
      RequestContext requestContext, CreateApiNamingRuleRequest request);

  List<ApiNamingRuleDetails> createApiNamingRules(
      RequestContext requestContext, CreateApiNamingRulesRequest request);

  ApiNamingRuleDetails updateApiNamingRule(
      RequestContext requestContext, UpdateApiNamingRuleRequest request) throws StatusException;

  List<ApiNamingRuleDetails> updateApiNamingRules(
      RequestContext requestContext, UpdateApiNamingRulesRequest request);

  void deleteApiNamingRule(RequestContext requestContext, DeleteApiNamingRuleRequest request);

  void deleteApiNamingRules(RequestContext requestContext, DeleteApiNamingRulesRequest request);
}
