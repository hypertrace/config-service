package ai.traceable.localprocessing.config.service.client;

import ai.traceable.localprocessing.config.service.config.EntityDataServiceConfig;
import com.google.inject.Inject;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;

public class EntityDataServiceClient {
  private final EntityDataServiceBlockingStub entityDataServiceBlockingStub;

  @Inject
  public EntityDataServiceClient(
      EntityDataServiceConfig config, GrpcChannelRegistry channelRegistry) {
    this.entityDataServiceBlockingStub =
        EntityDataServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(
                    config.getEntityServiceHost(), config.getEntityServicePort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public Entity getByTypeAndIdentifyingProperties(
      RequestContext requestContext,
      ByTypeAndIdentifyingAttributes byTypeAndIdentifyingAttributesRequest) {
    return requestContext.call(
        () ->
            entityDataServiceBlockingStub.getByTypeAndIdentifyingProperties(
                byTypeAndIdentifyingAttributesRequest));
  }
}
