package org.hypertrace.label.config.service;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.fixedWait;
import static java.util.function.Function.identity;

import com.github.rholder.retry.RetryerBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.CreateLabelRequest;
import org.hypertrace.label.config.service.v1.CreateLabelResponse;
import org.hypertrace.label.config.service.v1.DeleteLabelRequest;
import org.hypertrace.label.config.service.v1.DeleteLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelsRequest;
import org.hypertrace.label.config.service.v1.GetLabelsResponse;
import org.hypertrace.label.config.service.v1.GetOrCreateLabelsRequest;
import org.hypertrace.label.config.service.v1.GetOrCreateLabelsResponse;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelData;
import org.hypertrace.label.config.service.v1.LabelsConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.UpdateLabelRequest;
import org.hypertrace.label.config.service.v1.UpdateLabelResponse;

@Slf4j
public class LabelsConfigServiceImpl extends LabelsConfigServiceGrpc.LabelsConfigServiceImplBase {
  private static final String LABELS_CONFIG_SERVICE_CONFIG = "labels.config.service";
  private static final String SYSTEM_LABELS = "system.labels";
  private final LabelStore labelStore;
  private final List<Label> systemLabels;
  private final Map<String, Label> systemLabelsIdLabelMap;
  private final Map<String, Label> systemLabelsKeyLabelMap;

  public LabelsConfigServiceImpl(
      Channel configChannel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    labelStore =
        new LabelStore(
            ConfigServiceGrpc.newBlockingStub(configChannel)
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider()
                        .get()),
            configChangeEventGenerator);
    List<? extends ConfigObject> systemLabelsObjectList = null;
    if (config.hasPath(LABELS_CONFIG_SERVICE_CONFIG)) {
      Config labelConfig = config.getConfig(LABELS_CONFIG_SERVICE_CONFIG);
      if (labelConfig.hasPath(SYSTEM_LABELS)) {
        systemLabelsObjectList = labelConfig.getObjectList(SYSTEM_LABELS);
      }
    }
    if (systemLabelsObjectList != null) {
      systemLabels = buildSystemLabelList(systemLabelsObjectList);
      systemLabelsIdLabelMap =
          systemLabels.stream().collect(Collectors.toUnmodifiableMap(Label::getId, identity()));
      systemLabelsKeyLabelMap =
          systemLabels.stream()
              .collect(Collectors.toUnmodifiableMap(label -> label.getData().getKey(), identity()));
    } else {
      systemLabels = Collections.emptyList();
      systemLabelsIdLabelMap = Collections.emptyMap();
      systemLabelsKeyLabelMap = Collections.emptyMap();
    }
  }

  private List<Label> buildSystemLabelList(List<? extends ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(LabelsConfigServiceImpl::buildLabelFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static Label buildLabelFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    Label.Builder builder = Label.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  @Override
  public void createLabel(
      CreateLabelRequest request, StreamObserver<CreateLabelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      LabelData labelData = request.getData();
      if (isDuplicateKey(requestContext, labelData.getKey())) {
        // Creating a label with a name that clashes with one of system labels name
        responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS));
        return;
      }
      Label label =
          Label.newBuilder().setId(UUID.randomUUID().toString()).setData(labelData).build();
      if (request.hasCreatedByApplicationRuleId()) {
        label =
            label.toBuilder()
                .setCreatedByApplicationRuleId(request.getCreatedByApplicationRuleId())
                .build();
      }
      Label createdLabel = labelStore.upsertObject(requestContext, label).getData();
      responseObserver.onNext(CreateLabelResponse.newBuilder().setLabel(createdLabel).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getOrCreateLabels(
      GetOrCreateLabelsRequest request,
      StreamObserver<GetOrCreateLabelsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      // Try making the call twice, as the first call may fail if we concurrently try to create
      GetOrCreateLabelsResponse response =
          RetryerBuilder.<GetOrCreateLabelsResponse>newBuilder()
              .retryIfExceptionOfType(StatusRuntimeException.class)
              .withStopStrategy(stopAfterAttempt(2))
              .withWaitStrategy(fixedWait(100, TimeUnit.MILLISECONDS))
              .build()
              .call(() -> this.getOrCreateLabels(requestContext, request));
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      responseObserver.onError(exception);
    }
  }

  private GetOrCreateLabelsResponse getOrCreateLabels(
      RequestContext requestContext, GetOrCreateLabelsRequest request) {
    final Map<String, Label> existingLabelsMap = getLabelsMap(requestContext);
    List<Label> newLabels =
        request.getRequestsList().stream()
            .filter(labelRequest -> !existingLabelsMap.containsKey(labelRequest.getData().getKey()))
            .map(this::buildLabelFromRequest)
            .collect(Collectors.toList());
    Map<String, Label> createdLabelsMap;
    if (!newLabels.isEmpty()) {
      createdLabelsMap =
          labelStore.upsertObjects(requestContext, newLabels).stream()
              .map(org.hypertrace.config.objectstore.ConfigObject::getData)
              .collect(Collectors.toUnmodifiableMap(label -> label.getData().getKey(), identity()));
    } else {
      createdLabelsMap = Collections.emptyMap();
    }
    final Map<String, Label> allLabelsMap =
        ImmutableMap.<String, Label>builder()
            .putAll(existingLabelsMap)
            .putAll(createdLabelsMap)
            .build();

    @SuppressWarnings("ConstantConditions")
    List<Label> allLabels =
        request.getRequestsList().stream()
            .map(GetOrCreateLabelsRequest.LabelRequest::getData)
            .map(LabelData::getKey)
            .map(allLabelsMap::get)
            .collect(Collectors.toUnmodifiableList());
    return GetOrCreateLabelsResponse.newBuilder().addAllLabels(allLabels).build();
  }

  private Label buildLabelFromRequest(GetOrCreateLabelsRequest.LabelRequest request) {
    LabelData labelData = request.getData();
    Label.Builder labelBuilder =
        Label.newBuilder().setId(UUID.randomUUID().toString()).setData(labelData);
    if (request.hasCreatedByApplicationRuleId()) {
      labelBuilder.setCreatedByApplicationRuleId(request.getCreatedByApplicationRuleId());
    }
    return labelBuilder.build();
  }

  @Override
  public void getLabel(GetLabelRequest request, StreamObserver<GetLabelResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    String labelId = request.getId();
    try {
      Label label =
          labelStore
              .getData(requestContext, labelId)
              .orElseGet(
                  () ->
                      Optional.ofNullable(systemLabelsIdLabelMap.get(labelId))
                          .orElseThrow(Status.NOT_FOUND::asRuntimeException));
      responseObserver.onNext(GetLabelResponse.newBuilder().setLabel(label).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getLabels(
      GetLabelsRequest request, StreamObserver<GetLabelsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    List<Label> tenantLabels =
        labelStore.getAllObjects(requestContext).stream()
            .map(ContextualConfigObject::getData)
            .collect(Collectors.toUnmodifiableList());
    Map<String, Label> tenantLabelsMap =
        tenantLabels.stream()
            .collect(Collectors.toUnmodifiableMap(Label::getId, Function.identity()));
    List<Label> allLabels = new ArrayList<>(tenantLabels);
    systemLabels.stream()
        .filter(label -> !tenantLabelsMap.containsKey(label.getId()))
        .forEach(allLabels::add);
    responseObserver.onNext(GetLabelsResponse.newBuilder().addAllLabels(allLabels).build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateLabel(
      UpdateLabelRequest request, StreamObserver<UpdateLabelResponse> responseObserver) {
    try {
      LabelData updateLabelData = request.getData();
      RequestContext requestContext = RequestContext.CURRENT.get();

      if (isDuplicateKey(requestContext, request.getId(), updateLabelData.getKey())) {
        responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS));
        return;
      }
      Label oldLabel =
          labelStore
              .getData(requestContext, request.getId())
              .orElseGet(
                  () ->
                      Optional.ofNullable(systemLabelsIdLabelMap.get(request.getId()))
                          .orElseThrow(Status.NOT_FOUND::asRuntimeException));
      Label updateLabel = oldLabel.toBuilder().setData(updateLabelData).build();
      Label updateLabelInRes = labelStore.upsertObject(requestContext, updateLabel).getData();
      responseObserver.onNext(UpdateLabelResponse.newBuilder().setLabel(updateLabelInRes).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteLabel(
      DeleteLabelRequest request, StreamObserver<DeleteLabelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      String labelId = request.getId();
      if (systemLabelsIdLabelMap.containsKey(labelId)) {
        // Deleting a system label
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }
      labelStore
          .deleteObject(requestContext, labelId)
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteLabelResponse.newBuilder().build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private boolean isDuplicateKey(RequestContext requestContext, String id, String key) {
    return Optional.ofNullable(getLabelsMap(requestContext).get(key)).stream()
        .anyMatch(label -> !label.getId().equals(id));
  }

  private boolean isDuplicateKey(RequestContext requestContext, String key) {
    return getLabelsMap(requestContext).containsKey(key);
  }

  private Map<String, Label> getLabelsMap(RequestContext requestContext) {
    Map<String, Label> existingLabelsMap = new HashMap<>(systemLabelsKeyLabelMap);
    labelStore.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .forEach(label -> existingLabelsMap.put(label.getData().getKey(), label));
    return Collections.unmodifiableMap(existingLabelsMap);
  }
}
