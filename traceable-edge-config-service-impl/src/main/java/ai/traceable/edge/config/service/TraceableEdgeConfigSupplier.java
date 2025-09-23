package ai.traceable.edge.config.service;

import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface TraceableEdgeConfigSupplier {

  String getConfigType();

  ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities);
}
