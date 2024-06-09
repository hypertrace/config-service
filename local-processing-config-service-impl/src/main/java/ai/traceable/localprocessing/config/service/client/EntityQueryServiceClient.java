package ai.traceable.localprocessing.config.service.client;

import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import com.google.inject.Inject;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;

public class EntityQueryServiceClient {
  private final EntityQueryServiceBlockingStub entityQueryServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public EntityQueryServiceClient(
      EntityQueryServiceConfig config,
      GrpcChannelRegistry channelRegistry,
      ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.entityQueryServiceBlockingStub =
        EntityQueryServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(
                    config.getEntityServiceHost(), config.getEntityServicePort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public Iterator<ResultSetChunk> execute(
      RequestContext requestContext, EntityQueryRequest entityQueryRequest) {
    return requestContext.call(
        () ->
            entityQueryServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .execute(entityQueryRequest));
  }
}
