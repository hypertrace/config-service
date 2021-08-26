package org.hypertrace.label.config.service;

import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.Label;

public interface ConfigServiceCoordinator {

  Label upsertLabel(RequestContext requestContext, Label label);

  Label getLabel(RequestContext requestContext, String labelId);

  List<Label> getAllLabels(RequestContext requestContext);

  void deleteLabel(RequestContext requestContext, String labelId);
}
