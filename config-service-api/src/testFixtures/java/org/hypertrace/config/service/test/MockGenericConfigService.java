package org.hypertrace.config.service.test;

import static java.util.function.Predicate.not;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.protobuf.Value;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceImplBase;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * A mock implementation of the generic config service.
 *
 * <p>Each method must be explicitly requested for mocking to allow strict stubbing.
 */
public class MockGenericConfigService {

  private Server grpcServer;
  private final InProcessServerBuilder serverBuilder;
  private final ManagedChannel configChannel;
  private final RequestContext context = RequestContext.forTenantId("default tenant");
  private final String DEFAULT_CONFIG_CONTEXT = "__default";
  private final Table<ResourceType, String, ContextSpecificConfig> currentValues =
      Tables.newCustomTable(new LinkedHashMap<>(), LinkedHashMap::new);

  private final ConfigServiceImplBase mockConfigService =
      Mockito.mock(
          ConfigServiceImplBase.class,
          invocation -> { // Error if unmocked called so we don't hang waiting for streamobserver
            if (invocation
                .getMethod()
                .equals(ConfigServiceImplBase.class.getMethod("bindService"))) {
              return invocation.callRealMethod();
            }
            throw new UnsupportedOperationException("Unmocked method invoked");
          });

  public MockGenericConfigService addService(BindableService service) {
    this.serverBuilder.addService(ServerInterceptors.intercept(service, new TestInterceptor()));
    return this;
  }

  public MockGenericConfigService() {
    String uniqueName = InProcessServerBuilder.generateName();
    this.configChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.serverBuilder =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor()
            .addService(this.mockConfigService);
  }

  public void start() {
    try {
      this.grpcServer = serverBuilder.build().start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Channel channel() {
    return this.configChannel;
  }

  public void shutdown() {
    this.currentValues.clear();
    this.grpcServer.shutdownNow();
    this.configChannel.shutdownNow();
  }

  public MockGenericConfigService mockUpsert() {
    Mockito.doAnswer(
            invocation -> {
              UpsertConfigRequest request = invocation.getArgument(0, UpsertConfigRequest.class);
              StreamObserver<UpsertConfigResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);
              ResourceType resourceType =
                  ResourceType.of(request.getResourceNamespace(), request.getResourceName());
              String configContext = configContextOrDefault(request.getContext());
              ContextSpecificConfig existingConfig = currentValues.get(resourceType, configContext);
              long updateTimestamp = System.currentTimeMillis();
              long creationTimestamp =
                  existingConfig == null ? updateTimestamp : existingConfig.getCreationTimestamp();
              currentValues.put(
                  resourceType,
                  configContext,
                  ContextSpecificConfig.newBuilder()
                      .setContext(configContext)
                      .setConfig(request.getConfig())
                      .setCreationTimestamp(creationTimestamp)
                      .setUpdateTimestamp(updateTimestamp)
                      .build());
              responseStreamObserver.onNext(
                  UpsertConfigResponse.newBuilder()
                      .setConfig(request.getConfig())
                      .setCreationTimestamp(creationTimestamp)
                      .setUpdateTimestamp(updateTimestamp)
                      .build());
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .upsertConfig(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  public MockGenericConfigService mockGetAll() {
    Mockito.doAnswer(
            invocation -> {
              StreamObserver<GetAllConfigsResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);
              GetAllConfigsRequest request = invocation.getArgument(0, GetAllConfigsRequest.class);
              GetAllConfigsResponse response =
                  currentValues
                      .row(
                          ResourceType.of(
                              request.getResourceNamespace(), request.getResourceName()))
                      .values()
                      .stream()
                      .collect(
                          Collectors.collectingAndThen(
                              Collectors.toList(),
                              list ->
                                  GetAllConfigsResponse.newBuilder()
                                      .addAllContextSpecificConfigs(Lists.reverse(list))
                                      .build()));

              responseStreamObserver.onNext(response);
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .getAllConfigs(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  public MockGenericConfigService mockDelete() {
    Mockito.doAnswer(
            invocation -> {
              DeleteConfigRequest request = invocation.getArgument(0, DeleteConfigRequest.class);
              StreamObserver<DeleteConfigResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);

              ContextSpecificConfig configToDelete =
                  currentValues.get(
                      ResourceType.of(request.getResourceNamespace(), request.getResourceName()),
                      request.getContext());
              currentValues.remove(
                  ResourceType.of(request.getResourceNamespace(), request.getResourceName()),
                  configContextOrDefault(request.getContext()));
              responseStreamObserver.onNext(
                  DeleteConfigResponse.newBuilder().setDeletedConfig(configToDelete).build());
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .deleteConfig(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  public MockGenericConfigService mockGet() {
    Mockito.doAnswer(
            invocation -> {
              GetConfigRequest request = invocation.getArgument(0, GetConfigRequest.class);
              StreamObserver<GetConfigResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);

              Optional<Value> mergedValue =
                  Stream.concat(
                          Stream.of(DEFAULT_CONFIG_CONTEXT), request.getContextsList().stream())
                      .map(
                          context ->
                              this.currentValues.get(
                                  ResourceType.of(
                                      request.getResourceNamespace(), request.getResourceName()),
                                  context))
                      .filter(Objects::nonNull)
                      .map(ContextSpecificConfig::getConfig)
                      .collect(
                          Collectors.collectingAndThen(Collectors.toList(), this::mergeValues));

              if (isValidValue(mergedValue)) {
                responseStreamObserver.onNext(
                    GetConfigResponse.newBuilder().setConfig(mergedValue.get()).build());
                responseStreamObserver.onCompleted();
              } else {
                responseStreamObserver.onError(Status.NOT_FOUND.asException());
              }
              return null;
            })
        .when(this.mockConfigService)
        .getConfig(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  private Optional<Value> mergeValues(List<Value> values) {
    if (values.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        values.stream().reduce(Value.getDefaultInstance(), ConfigServiceUtils::merge));
  }

  private String configContextOrDefault(String value) {
    return Optional.ofNullable(value).filter(not(String::isEmpty)).orElse(DEFAULT_CONFIG_CONTEXT);
  }

  private boolean isValidValue(Optional<Value> value) {
    return value.isPresent() && value.get().getKindCase() != Value.KindCase.NULL_VALUE;
  }

  private class TestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      Context ctx = Context.current().withValue(RequestContext.CURRENT, context);
      return Contexts.interceptCall(ctx, call, headers, next);
    }
  }

  @lombok.Value(staticConstructor = "of")
  private static class ResourceType {
    String namespace;
    String name;
  }
}
