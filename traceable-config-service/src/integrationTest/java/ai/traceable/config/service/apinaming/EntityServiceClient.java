package ai.traceable.config.service.apinaming;

import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;

public class EntityServiceClient {
  private final EntityDataServiceBlockingStub entityDataServiceBlockingStub;
  private final EntityTypeServiceGrpc.EntityTypeServiceBlockingStub entityTypeServiceBlockingStub;

  public EntityServiceClient(EntityServiceConfig config, GrpcChannelRegistry channelRegistry) {
    this.entityDataServiceBlockingStub =
        EntityDataServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(
                    config.getEntityServiceHost(), config.getEntityServicePort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.entityTypeServiceBlockingStub =
        EntityTypeServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(
                    config.getEntityServiceHost(), config.getEntityServicePort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public void upsertEntityType(RequestContext requestContext, EntityType entityType) {
    requestContext.call(() -> entityTypeServiceBlockingStub.upsertEntityType(entityType));
  }

  public Entity upsertEntity(RequestContext requestContext, Entity entity) {
    return requestContext.call(() -> entityDataServiceBlockingStub.upsert(entity));
  }
}
