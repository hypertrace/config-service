package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigResponse;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SpanIngestionRulesManager {

  GetSpanIngestionConfigResponse getRuleSet(
      RequestContext requestContext, GetSpanIngestionConfigRequest request);

  KeyValueRetentionRule createRule(
      RequestContext requestContext, CreateSpanIngestionRuleRequest request);

  void deleteRule(RequestContext requestContext, DeleteSpanIngestionRuleRequest request);

  KeyValueRetentionRule updateRule(
      RequestContext requestContext, UpdateSpanIngestionRuleRequest request);

  void rankRules(RequestContext requestContext, RankSpanIngestionRuleRequest request);
}
