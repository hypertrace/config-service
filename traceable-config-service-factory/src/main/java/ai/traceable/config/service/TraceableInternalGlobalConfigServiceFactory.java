package ai.traceable.config.service;

import ai.traceable.fraud.datamodel.config.service.FraudDataModelConfigServiceFactory;
import ai.traceable.fraud.datamodel.derivation.config.service.FraudDataModelDerivationConfigServiceFactory;
import ai.traceable.fraud.datamodel.derivation.config.service.entity.EntityDerivationConfigServiceFactory;
import com.typesafe.config.Config;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.Datastore;
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
    Config config = providers.getConfig();
    Datastore datastore =
        DataStoreUtils.initDataStore(config, grpcServiceContainerEnvironment.getLifecycle());
    return List.of(
        new GrpcPlatformService(PartitionerConfigServiceFactory.build(config, datastore)),
        new GrpcPlatformService(
            FraudDataModelConfigServiceFactory.build(
                providers.getChangeEventGenerator(),
                datastore,
                config,
                providers.getChannelRegistry())),
        new GrpcPlatformService(
            FraudDataModelDerivationConfigServiceFactory.build(
                providers.getConfig(),
                providers.getLocalChannel(),
                providers.getChangeEventGenerator())),
        new GrpcPlatformService(
            EntityDerivationConfigServiceFactory.build(
                providers.getConfig(),
                providers.getLocalChannel(),
                providers.getChangeEventGenerator())));
  }
}
