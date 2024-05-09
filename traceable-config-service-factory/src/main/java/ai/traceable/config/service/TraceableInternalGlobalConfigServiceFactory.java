package ai.traceable.config.service;

import ai.traceable.fraud.datamodel.config.service.FraudDataModelConfigServiceFactory;
import ai.traceable.fraud.datamodel.derivation.config.service.FraudDataModelDerivationConfigServiceFactory;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.partitioner.config.service.PartitionerConfigServiceFactory;

@RequiredArgsConstructor
public class TraceableInternalGlobalConfigServiceFactory implements GrpcPlatformServiceFactory {

  @Nonnull SharedConfigServiceProvidersFactory providersFactory;

  @Override
  public List<GrpcPlatformService> buildServices(
      GrpcServiceContainerEnvironment grpcServiceContainerEnvironment) {
    SharedConfigServiceProviders providers =
        providersFactory.getProvidersForEnvironment(grpcServiceContainerEnvironment);
    return List.of(
        new GrpcPlatformService(PartitionerConfigServiceFactory.build(providers.getConfig())),
        new GrpcPlatformService(FraudDataModelConfigServiceFactory.build(providers.getConfig())),
        new GrpcPlatformService(
            FraudDataModelDerivationConfigServiceFactory.build(
                providers.getLocalChannel(), providers.getChangeEventGenerator())));
  }
}
