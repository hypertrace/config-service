package org.hypertrace.label.config.service;

import com.google.common.util.concurrent.Striped;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.CreateLabel;
import org.hypertrace.label.config.service.v1.CreateLabelRequest;
import org.hypertrace.label.config.service.v1.CreateLabelResponse;
import org.hypertrace.label.config.service.v1.DeleteLabelRequest;
import org.hypertrace.label.config.service.v1.DeleteLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelsRequest;
import org.hypertrace.label.config.service.v1.GetLabelsResponse;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelsConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.UpdateLabelRequest;
import org.hypertrace.label.config.service.v1.UpdateLabelResponse;

@Slf4j
public class LabelsConfigServiceImpl extends LabelsConfigServiceGrpc.LabelsConfigServiceImplBase {
  private final ConfigServiceCoordinator configServiceCoordinator;
  private final String LABELS_CONFIG_SERVICE_CONFIG = "labels.config.service";
  private final String SYSTEM_LABELS = "system.labels";
  private final List<Label> systemLabels;
  private final Map<String, Label> systemLabelsIdLabelMap;
  private final Map<String, Label> systemLabelsKeyLabelMap;
  private final Striped<Lock> stripedLabelsLock;
  private static final int LABEL_LOCK_STRIPE_COUNT = 1000;
  private static final Duration WAIT_TIME = Duration.newBuilder().setSeconds(5).build();

  public LabelsConfigServiceImpl(Channel configChannel, Config config) {
    stripedLabelsLock = Striped.lazyWeakLock(LABEL_LOCK_STRIPE_COUNT);
    configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
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
          systemLabels.stream()
              .collect(Collectors.toUnmodifiableMap(Label::getId, Function.identity()));
      systemLabelsKeyLabelMap =
          systemLabels.stream()
              .collect(Collectors.toUnmodifiableMap(Label::getKey, Function.identity()));
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
    RequestContext requestContext = RequestContext.CURRENT.get();
    Lock labelsLock = this.stripedLabelsLock.get(requestContext.getTenantId());
    try {
      boolean lockAcquired = labelsLock.tryLock(WAIT_TIME.getSeconds(), TimeUnit.SECONDS);
      if (lockAcquired) {
        CreateLabel createLabel = request.getLabel();
        if (systemLabelsKeyLabelMap.containsKey(createLabel.getKey())
            || isDuplicateKey(createLabel.getKey())) {
          // Creating a label with a name that clashes with one of system labels name
          responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS));
          return;
        }
        Label label =
            Label.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setKey(createLabel.getKey())
                .build();
        Label createdLabel = configServiceCoordinator.upsertLabel(requestContext, label);
        responseObserver.onNext(CreateLabelResponse.newBuilder().setLabel(createdLabel).build());
        responseObserver.onCompleted();
      } else {
        throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
      }
    } catch (Exception e) {
      responseObserver.onError(e);
    } finally {
      labelsLock.unlock();
    }
  }

  @Override
  public void getLabel(GetLabelRequest request, StreamObserver<GetLabelResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    String labelId = request.getId();
    try {
      Label label;
      if (systemLabelsIdLabelMap.containsKey(labelId)) {
        label = systemLabelsIdLabelMap.get(labelId);
      } else {
        label = configServiceCoordinator.getLabel(requestContext, labelId);
      }
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
    List<Label> labelList = configServiceCoordinator.getAllLabels(requestContext);
    labelList.addAll(systemLabels);
    responseObserver.onNext(GetLabelsResponse.newBuilder().addAllLabels(labelList).build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateLabel(
      UpdateLabelRequest request, StreamObserver<UpdateLabelResponse> responseObserver) {
    Label updatedLabelInReq = request.getLabel();
    RequestContext requestContext = RequestContext.CURRENT.get();
    Lock labelsLock = this.stripedLabelsLock.get(requestContext.getTenantId());
    try {
      boolean lockAcquired = labelsLock.tryLock(WAIT_TIME.getSeconds(), TimeUnit.SECONDS);
      if (lockAcquired) {
        if (systemLabelsIdLabelMap.containsKey(updatedLabelInReq.getId())
            || systemLabelsKeyLabelMap.containsKey(updatedLabelInReq.getKey())) {
          // Updating a system label will error
          responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
          return;
        }
        if (isDuplicateKey(updatedLabelInReq.getKey())) {
          responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS));
          return;
        }
        configServiceCoordinator.getLabel(requestContext, updatedLabelInReq.getId());
        Label updatedLabelInRes =
            configServiceCoordinator.upsertLabel(requestContext, updatedLabelInReq);
        responseObserver.onNext(
            UpdateLabelResponse.newBuilder().setLabel(updatedLabelInRes).build());
        responseObserver.onCompleted();
      } else {
        throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
      }
    } catch (Exception e) {
      responseObserver.onError(e);
    } finally {
      labelsLock.unlock();
    }
  }

  @Override
  public void deleteLabel(
      DeleteLabelRequest request, StreamObserver<DeleteLabelResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    Lock labelsLock = this.stripedLabelsLock.get(requestContext.getTenantId());
    labelsLock.lock();
    try {
      boolean lockAcquired = labelsLock.tryLock(WAIT_TIME.getSeconds(), TimeUnit.SECONDS);
      if (lockAcquired) {
        String labelId = request.getId();
        if (systemLabelsIdLabelMap.containsKey(labelId)) {
          // Deleting a system label
          responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
          return;
        }
        configServiceCoordinator.getLabel(requestContext, labelId);
        configServiceCoordinator.deleteLabel(requestContext, labelId);
        responseObserver.onNext(DeleteLabelResponse.newBuilder().build());
        responseObserver.onCompleted();
      } else {
        throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
      }
    } catch (Exception e) {
      responseObserver.onError(e);
    } finally {
      labelsLock.unlock();
    }
  }

  private boolean isDuplicateKey(String key) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    List<Label> labelList = configServiceCoordinator.getAllLabels(requestContext);
    Optional<Label> match =
        labelList.stream().filter(label -> label.getKey().equals(key)).findAny();
    return match.isPresent();
  }
}
