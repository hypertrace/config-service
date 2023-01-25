package org.hypertrace.config.service;

import java.util.List;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.partitioner.config.service.PartitionerConfigServiceFactory;

/**
 * This is meant to be used to construct global config services that are not tied to any tenant and
 * those that run on a separate port. They use the same underlying datastore used to manage config
 * data
 */
public class GlobalConfigServiceFactory implements GrpcPlatformServiceFactory {
  private static final String SERVICE_NAME = "config-service";
  public static final String GLOBAL_CONFIG_SERVICE_PORT_CONFIG = "global.config.service.port";

  @Override
  public List<GrpcPlatformService> buildServices(
      GrpcServiceContainerEnvironment grpcServiceContainerEnvironment) {
    return List.of(
        new GrpcPlatformService(
            PartitionerConfigServiceFactory.build(
                grpcServiceContainerEnvironment.getConfig(SERVICE_NAME))));
  }
}
