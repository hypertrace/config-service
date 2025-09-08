package ai.traceable.attribute.resolution.client;

import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.info.AttributeResolutionConfigInfo;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AttributeResolutionConfigClient {

  AttributeResolutionConfigInfo getAttributeResolutionConfig(RequestContext requestContext);

  AttributeResolutionConfigInfo getAttributeResolutionConfig(
      RequestContext requestContext, @Nonnull GetAttributeResolutionConfigsFilter filter);
}
