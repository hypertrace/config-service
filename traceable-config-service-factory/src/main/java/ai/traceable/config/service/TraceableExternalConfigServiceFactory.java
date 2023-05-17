package ai.traceable.config.service;

import ai.traceable.blocking.config.service.BlockingConfigServiceFactory;
import ai.traceable.external.agent.attribute.config.service.ExternalAgentAttributeConfigServiceFactory;
import ai.traceable.external.data.classification.config.service.ExternalDataClassificationConfigServiceFactory;
import ai.traceable.external.userattribution.config.service.ExternalUserAttributionConfigServiceFactory;
import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceFactory;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServicesProvider;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

@RequiredArgsConstructor
public class TraceableExternalConfigServiceFactory implements GrpcPlatformServiceFactory {
  @Nonnull SharedConfigServiceProvidersFactory providersFactory;

  @Override
  public List<GrpcPlatformService> buildServices(GrpcServiceContainerEnvironment environment) {
    SharedConfigServiceProviders providers =
        providersFactory.getProvidersForEnvironment(environment);
    return Stream.concat(
            Stream.of(
                new SensitiveDataConfigServicesProvider(
                        providers.getLocalChannel(),
                        providers.getConfig(),
                        environment.getChannelRegistry(),
                        providers.getChangeEventGenerator(),
                        providers.getFeatureCachingClient())
                    .getPiiFilterConfigService(),
                LocalProcessingConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient()),
                ExternalUserAttributionConfigServiceFactory.build(providers.getLocalChannel()),
                ExternalAgentAttributeConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getFeatureCachingClient()),
                ExternalDataClassificationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    environment.getChannelRegistry(),
                    providers.getFeatureCachingClient())),
            BlockingConfigServiceFactory.build(
                providers.getLocalChannel(),
                providers.getConfig(),
                environment.getChannelRegistry())
                .stream())
        .map(GrpcPlatformService::new)
        .collect(Collectors.toUnmodifiableList());
  }
}
