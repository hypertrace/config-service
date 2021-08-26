package org.hypertrace.label.config.service;

import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.hypertrace.label.config.service.v1.LabelConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.UpdateLabelRequest;
import org.hypertrace.label.config.service.v1.UpdateLabelResponse;

@Slf4j
public class LabelConfigServiceImpl extends LabelConfigServiceGrpc.LabelConfigServiceImplBase {
  private final ConfigServiceCoordinator configServiceCoordinator;
  private final String LABEL_CONFIG_SERVICE_CONFIG = "label.config.service";
  private final String SYSTEM_LABELS = "system.labels";
  private final List<Label> systemLabels;
  private final Map<String, Label> systemLabelsIdLabelMap;
  private final Map<String, Label> systemLabelsKeyLabelMap;

  public LabelConfigServiceImpl(Channel configChannel, Config config) {
    configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
    Config labelConfig = config.getConfig(LABEL_CONFIG_SERVICE_CONFIG);
    List<? extends ConfigObject> systemLabelsObjectList = labelConfig.getObjectList(SYSTEM_LABELS);
    systemLabels = buildSystemLabelList(systemLabelsObjectList);
    systemLabelsIdLabelMap =
        systemLabels.stream().collect(Collectors.toMap(Label::getId, Function.identity()));
    systemLabelsKeyLabelMap =
        systemLabels.stream().collect(Collectors.toMap(Label::getKey, Function.identity()));
  }

  private List<Label> buildSystemLabelList(List<? extends ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(LabelConfigServiceImpl::buildLabelFromConfig)
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
      CreateLabel createLabel = request.getLabel();
      if (systemLabelsKeyLabelMap.containsKey(createLabel.getKey())) {
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
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getLabel(GetLabelRequest request, StreamObserver<GetLabelResponse> responseObserver) {
    String labelId = request.getId();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
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
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      if (systemLabelsIdLabelMap.containsKey(updatedLabelInReq.getId())
          || systemLabelsKeyLabelMap.containsKey(updatedLabelInReq.getKey())) {
        // Updating a system label will error
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }
      configServiceCoordinator.getLabel(requestContext, updatedLabelInReq.getId());
      Label updatedLabelInRes =
          configServiceCoordinator.upsertLabel(requestContext, updatedLabelInReq);
      responseObserver.onNext(UpdateLabelResponse.newBuilder().setLabel(updatedLabelInRes).build());
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
      configServiceCoordinator.getLabel(requestContext, labelId);
      configServiceCoordinator.deleteLabel(requestContext, labelId);
      responseObserver.onNext(DeleteLabelResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
