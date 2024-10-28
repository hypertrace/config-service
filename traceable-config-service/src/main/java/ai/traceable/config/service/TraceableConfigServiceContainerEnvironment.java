package ai.traceable.config.service;

import com.typesafe.config.Config;
import io.grpc.health.v1.HealthCheckResponse;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.core.serviceframework.http.HttpContainerEnvironment;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

class TraceableConfigServiceContainerEnvironment
    implements GrpcServiceContainerEnvironment, HttpContainerEnvironment {
  private final GrpcServiceContainerEnvironment delegate;

  TraceableConfigServiceContainerEnvironment(GrpcServiceContainerEnvironment delegate) {
    this.delegate = delegate;
  }

  @Override
  public InProcessGrpcChannelRegistry getChannelRegistry() {
    return delegate.getChannelRegistry();
  }

  @Override
  public void reportServiceStatus(String s, HealthCheckResponse.ServingStatus servingStatus) {
    delegate.reportServiceStatus(s, servingStatus);
  }

  @Override
  public Config getConfig(String serviceName) {
    return delegate.getConfig(serviceName);
  }

  @Override
  public String getInProcessChannelName() {
    return delegate.getInProcessChannelName();
  }

  @Override
  public PlatformServiceLifecycle getLifecycle() {
    return delegate.getLifecycle();
  }
}
