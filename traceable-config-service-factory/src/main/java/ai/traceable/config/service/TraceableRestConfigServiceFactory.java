package ai.traceable.config.service;

import static ai.traceable.config.service.SharedConfigServiceProvidersFactory.SERVICE_NAME;

import ai.traceable.config.service.rest.TraceableConfigServiceRestModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.core.serviceframework.http.HttpContainerEnvironment;
import org.hypertrace.core.serviceframework.http.HttpHandlerDefinition;
import org.hypertrace.core.serviceframework.http.HttpHandlerFactory;

@RequiredArgsConstructor
public class TraceableRestConfigServiceFactory implements HttpHandlerFactory {
  public static final String REST_PORT_CONFIG = "service.port.rest";

  @Nonnull SharedConfigServiceProvidersFactory providersFactory;

  @Override
  public List<HttpHandlerDefinition> buildHandlers(HttpContainerEnvironment containerEnvironment) {
    if (containerEnvironment instanceof GrpcServiceContainerEnvironment) {
      return List.of(
          this.buildRestHandler(
              containerEnvironment.getConfig("rest-" + SERVICE_NAME),
              containerEnvironment
                  .getChannelRegistry()
                  .forName(
                      ((GrpcServiceContainerEnvironment) containerEnvironment)
                          .getInProcessChannelName()),
              containerEnvironment.getChannelRegistry()));
    }
    throw new RuntimeException("Config REST handlers expect to use a hybrid GRPC/HTTP container");
  }

  private HttpHandlerDefinition buildRestHandler(
      Config config, Channel configChannel, GrpcChannelRegistry channelRegistry) {
    return HttpHandlerDefinition.builder()
        .name("rest-" + SERVICE_NAME)
        .port(config.getInt(REST_PORT_CONFIG))
        .contextPath("/")
        .injector(
            Guice.createInjector(
                Stage.PRODUCTION,
                new TraceableConfigServiceRestModule(config, configChannel, channelRegistry)))
        .maxHeaderSizeBytes(
            Math.toIntExact(config.getMemorySize("service.rest.maxHeaderSize").toBytes()))
        .useSessions(true)
        .build();
  }
}
