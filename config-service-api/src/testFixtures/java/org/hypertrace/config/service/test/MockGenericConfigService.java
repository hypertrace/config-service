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
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceImplBase;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.DeleteConfigsRequest;
import org.hypertrace.config.service.v1.DeleteConfigsResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
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
  private Clock clock = Clock.systemUTC();
  private final RequestContext defaultContext = RequestContext.forTenantId("default tenant");
  private final InProcessServerBuilder serverBuilder;
  private final ManagedChannel configChannel;
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

  public MockGenericConfigService withClock(Clock clock) {
    this.clock = clock;
    return this;
  }

  @SuppressWarnings("unchecked")
  public MockGenericConfigService mockUpsert() {
    Mockito.doAnswer(
            invocation -> {
              UpsertConfigRequest request = invocation.getArgument(0, UpsertConfigRequest.class);
              StreamObserver<UpsertConfigResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);
              UpsertedConfig upsertedConfig =
                  this.writeToMap(
                      request.getResourceNamespace(),
                      request.getResourceName(),
                      request.getContext(),
                      request.getConfig());
              UpsertConfigResponse.Builder responseBuilder =
                  UpsertConfigResponse.newBuilder()
                      .setConfig(upsertedConfig.getConfig())
                      .setCreationTimestamp(upsertedConfig.getCreationTimestamp())
                      .setUpdateTimestamp(upsertedConfig.getUpdateTimestamp())
                      .setCreatedByEmail(upsertedConfig.getCreatedByEmail())
                      .setLastUserUpdateEmail(upsertedConfig.getLastUserUpdateEmail())
                      .setLastUserUpdateTimestamp(upsertedConfig.getLastUserUpdateTimestamp());
              if (upsertedConfig.hasPrevConfig()) {
                responseBuilder.setPrevConfig(upsertedConfig.getPrevConfig());
              }

              responseStreamObserver.onNext(responseBuilder.build());
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .upsertConfig(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  @SuppressWarnings("unchecked")
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

  public MockGenericConfigService mockGetAllWithFilter(
      Predicate<ContextSpecificConfig> filterPredicate) {

    Mockito.doAnswer(
            invocation -> {
              StreamObserver<GetAllConfigsResponse> responseObserver =
                  invocation.getArgument(1, StreamObserver.class);
              GetAllConfigsRequest request = invocation.getArgument(0, GetAllConfigsRequest.class);

              List<ContextSpecificConfig> matchingConfigs =
                  currentValues
                      .row(
                          ResourceType.of(
                              request.getResourceNamespace(), request.getResourceName()))
                      .values()
                      .stream()
                      .filter(
                          config -> {
                            if (request.hasFilter()) {
                              return filterPredicate.test(config);
                            }
                            return true;
                          })
                      .collect(Collectors.toList());

              GetAllConfigsResponse response =
                  GetAllConfigsResponse.newBuilder()
                      .addAllContextSpecificConfigs(matchingConfigs)
                      .build();

              responseObserver.onNext(response);
              responseObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .getAllConfigs(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  @SuppressWarnings("unchecked")
  public MockGenericConfigService mockDelete() {
    Mockito.doAnswer(
            invocation -> {
              DeleteConfigRequest request = invocation.getArgument(0, DeleteConfigRequest.class);
              StreamObserver<DeleteConfigResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);

              Optional<ContextSpecificConfig> configToDelete =
                  Optional.ofNullable(
                      currentValues.get(
                          ResourceType.of(
                              request.getResourceNamespace(), request.getResourceName()),
                          configContextOrDefault(request.getContext())));
              currentValues.remove(
                  ResourceType.of(request.getResourceNamespace(), request.getResourceName()),
                  configContextOrDefault(request.getContext()));
              if (configToDelete.isPresent()) {
                responseStreamObserver.onNext(
                    DeleteConfigResponse.newBuilder()
                        .setDeletedConfig(configToDelete.get())
                        .build());
                responseStreamObserver.onCompleted();
              } else {
                responseStreamObserver.onError(Status.NOT_FOUND.asException());
              }
              return null;
            })
        .when(this.mockConfigService)
        .deleteConfig(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  @SuppressWarnings("unchecked")
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

  public MockGenericConfigService mockUpsertAll() {
    Mockito.doAnswer(
            invocation -> {
              UpsertAllConfigsRequest request =
                  invocation.getArgument(0, UpsertAllConfigsRequest.class);
              StreamObserver<UpsertAllConfigsResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);

              List<UpsertedConfig> configs =
                  request.getConfigsList().stream()
                      .map(
                          configToUpsert ->
                              this.writeToMap(
                                  configToUpsert.getResourceNamespace(),
                                  configToUpsert.getResourceName(),
                                  configToUpsert.getContext(),
                                  configToUpsert.getConfig()))
                      .collect(Collectors.toUnmodifiableList());
              responseStreamObserver.onNext(
                  UpsertAllConfigsResponse.newBuilder().addAllUpsertedConfigs(configs).build());
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .upsertAllConfigs(ArgumentMatchers.any(), ArgumentMatchers.any());

    return this;
  }

  public MockGenericConfigService mockDeleteAll() {
    Mockito.doAnswer(
            invocation -> {
              DeleteConfigsRequest request = invocation.getArgument(0, DeleteConfigsRequest.class);
              StreamObserver<DeleteConfigsResponse> responseStreamObserver =
                  invocation.getArgument(1, StreamObserver.class);

              List<ContextSpecificConfig> configsToDelete =
                  request.getConfigsList().stream()
                      .map(
                          configToDelete ->
                              currentValues.get(
                                  ResourceType.of(
                                      configToDelete.getResourceNamespace(),
                                      configToDelete.getResourceName()),
                                  configContextOrDefault(configToDelete.getContext())))
                      .collect(Collectors.toUnmodifiableList());

              request
                  .getConfigsList()
                  .forEach(
                      configToDelete ->
                          currentValues.remove(
                              ResourceType.of(
                                  configToDelete.getResourceNamespace(),
                                  configToDelete.getResourceName()),
                              configContextOrDefault(configToDelete.getContext())));

              responseStreamObserver.onNext(
                  DeleteConfigsResponse.newBuilder().addAllDeletedConfigs(configsToDelete).build());
              responseStreamObserver.onCompleted();
              return null;
            })
        .when(this.mockConfigService)
        .deleteConfigs(ArgumentMatchers.any(), ArgumentMatchers.any());

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

  private UpsertedConfig writeToMap(String namespace, String name, String context, Value config) {
    ResourceType resourceType = ResourceType.of(namespace, name);
    String configContext = configContextOrDefault(context);
    ContextSpecificConfig existingConfig = currentValues.get(resourceType, configContext);
    long updateTimestamp = clock.millis();
    long creationTimestamp =
        Optional.ofNullable(existingConfig)
            .map(ContextSpecificConfig::getCreationTimestamp)
            .orElse(updateTimestamp);
    Optional<Value> previousConfig =
        Optional.ofNullable(
                currentValues.put(
                    resourceType,
                    configContext,
                    ContextSpecificConfig.newBuilder()
                        .setContext(configContext)
                        .setConfig(config)
                        .setCreationTimestamp(creationTimestamp)
                        .setUpdateTimestamp(updateTimestamp)
                        .setCreatedByEmail("")
                        .build()))
            .map(ContextSpecificConfig::getConfig);

    UpsertedConfig.Builder resultBuilder =
        UpsertedConfig.newBuilder()
            .setConfig(config)
            .setContext(configContext)
            .setCreationTimestamp(creationTimestamp)
            .setUpdateTimestamp(updateTimestamp)
            .setCreatedByEmail("")
            .setLastUserUpdateEmail("")
            .setLastUserUpdateTimestamp(updateTimestamp);

    previousConfig.ifPresent(resultBuilder::setPrevConfig);

    return resultBuilder.build();
  }

  private class TestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      RequestContext context =
          Optional.of(RequestContext.fromMetadata(headers))
              .filter(requestContext -> requestContext.getTenantId().isPresent())
              .orElse(defaultContext);
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
