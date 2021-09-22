package org.hypertrace.label.application.rule.config.service;

import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;

public interface ConfigServiceCoordinator {
  LabelApplicationRule upsertLabelApplicationRule(
      RequestContext requestContext, LabelApplicationRule labelApplicationRule);

  LabelApplicationRule getLabelApplicationRule(RequestContext requestContext, String id);

  List<LabelApplicationRule> getLabelApplicationRules(RequestContext requestContext);

  void deleteLabelApplicationRule(RequestContext requestContext, String id);
}
