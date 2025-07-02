package ai.traceable.genai.system.discovery.client;

import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.info.GenAiSystemDiscoveryConfig;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GenAiSystemDiscoveryConfigClient {
  GenAiSystemDiscoveryConfig getGenAiSystemDiscoveryConfig(RequestContext requestContext);

  GenAiSystemDiscoveryConfig getGenAiSystemDiscoveryConfig(
      RequestContext requestContext, @Nonnull GetGenAiSystemDiscoveryRulesFilter filter);
}
