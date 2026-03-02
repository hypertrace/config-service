package ai.traceable.span.processing.config.service.apinamingrules;

import ai.traceable.span.processing.config.service.store.ApiNamingRulesResult;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRulesFilter;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import io.grpc.StatusException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiNamingRulesManager {
  List<ApiNamingRuleDetails> getAllApiNamingRuleDetails(RequestContext requestContext);

  List<ApiNamingRuleDetails> getApiNamingRuleDetails(
      RequestContext requestContext, ApiNamingRulesFilter apiNamingRulesFilter);

  ApiNamingRulesResult getApiNamingRuleDetailsWithPaginationAndOptionalTotal(
      RequestContext requestContext, GetApiNamingRulesRequest request);

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
