package org.hypertrace.label.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.label.config.service.v1.CreateLabelRequest;
import org.hypertrace.label.config.service.v1.CreateLabelResponse;
import org.hypertrace.label.config.service.v1.DeleteLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelsRequest;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelData;
import org.hypertrace.label.config.service.v1.LabelsConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.LabelsConfigServiceGrpc.LabelsConfigServiceBlockingStub;
import org.hypertrace.label.config.service.v1.UpdateLabelRequest;
import org.hypertrace.label.config.service.v1.UpdateLabelResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class LabelsConfigServiceImplTest {
  LabelsConfigServiceBlockingStub labelConfigStub;
  Config config;
  MockGenericConfigService mockGenericConfigService;
  List<Label> systemLabels =
      Arrays.asList("Critical", "Sensitive", "External", "Sentry").stream()
          .map(
              key ->
                  Label.newBuilder()
                      .setId(key)
                      .setData(LabelData.newBuilder().setKey(key).build())
                      .build())
          .collect(Collectors.toList());
  List<LabelData> createLabelDataList =
      Stream.of(0, 1, 2, 3, 4)
          .map(id -> LabelData.newBuilder().setKey("Label-" + id).build())
          .collect(Collectors.toList());

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Map<String, List<Map<String, String>>> systemLabelsConfigMap = new HashMap<>();
    systemLabelsConfigMap.put(
        "system.labels",
        systemLabels.stream()
            .map(
                systemLabel ->
                    Map.of("id", systemLabel.getId(), "data.key", systemLabel.getData().getKey()))
            .collect(Collectors.toList()));
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("labels.config.service", systemLabelsConfigMap);
    config = ConfigFactory.parseMap(configMap);
    Channel channel = mockGenericConfigService.channel();
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockGenericConfigService
        .addService(new LabelsConfigServiceImpl(channel, config, configChangeEventGenerator))
        .start();
    labelConfigStub = LabelsConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  private List<Label> createLabels() {
    // Creating or inserting labels
    return createLabelDataList.stream()
        .map(
            createLabel -> {
              CreateLabelResponse response =
                  labelConfigStub.createLabel(
                      CreateLabelRequest.newBuilder().setData(createLabel).build());
              return response.getLabel();
            })
        .collect(Collectors.toList());
  }

  @Test
  void test_createLabel() {
    List<LabelData> createdLabelsList =
        createLabels().stream().map(Label::getData).collect(Collectors.toList());
    assertEquals(createdLabelsList, createdLabelsList);
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelConfigStub.createLabel(
                  CreateLabelRequest.newBuilder().setData(createLabelDataList.get(0)).build());
            });
    assertEquals(Status.ALREADY_EXISTS, Status.fromThrowable(exception));
  }

  @Test
  void test_system_createLabel() {
    for (Label systemLabel : systemLabels) {
      CreateLabelRequest createLabelRequest =
          CreateLabelRequest.newBuilder()
              .setData(LabelData.newBuilder().setKey(systemLabel.getData().getKey()).build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                labelConfigStub.createLabel(createLabelRequest);
              });
      assertEquals(Status.ALREADY_EXISTS, Status.fromThrowable(exception));
    }
  }

  @Test
  void test_getLabel() {
    List<Label> createdLabelsList =
        createLabels().stream()
            .map(label -> label.toBuilder().build())
            .collect(Collectors.toList());
    // Querying each label one by one for the created or inserted labels in the previous step
    List<Label> getLabelList =
        createdLabelsList.stream()
            .map(
                label -> {
                  GetLabelResponse response =
                      labelConfigStub.getLabel(
                          GetLabelRequest.newBuilder().setId(label.getId()).build());
                  System.out.println(response.getLabel());
                  return response.getLabel();
                })
            .collect(Collectors.toList());
    assertEquals(createdLabelsList, getLabelList);
    // Get a label that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelConfigStub.getLabel(GetLabelRequest.newBuilder().setId("1").build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void test_system_getLabel() {
    for (Label systemLabel : systemLabels) {
      GetLabelRequest getLabelRequest =
          GetLabelRequest.newBuilder().setId(systemLabel.getId()).build();
      GetLabelResponse getLabelResponse = labelConfigStub.getLabel(getLabelRequest);
      assertEquals(systemLabel, getLabelResponse.getLabel());
    }
  }

  @Test
  void test_getLabels() {
    List<Label> createdLabelsList = createLabels();
    createdLabelsList.addAll(systemLabels);
    // Querying for all the labels at once for the created or inserted labels in the previous step
    List<Label> getLabels =
        labelConfigStub.getLabels(GetLabelsRequest.newBuilder().build()).getLabelsList();
    assertEquals(Set.copyOf(createdLabelsList), Set.copyOf(getLabels));
  }

  @Test
  void test_updateLabel() {
    List<Label> createdLabelsList = createLabels();
    Throwable exception1 =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelConfigStub.updateLabel(
                  UpdateLabelRequest.newBuilder()
                      .setId(createdLabelsList.get(1).getId())
                      .setData(
                          LabelData.newBuilder()
                              .setKey(createdLabelsList.get(0).getData().getKey())
                              .build())
                      .build());
            });
    assertEquals(Status.ALREADY_EXISTS, Status.fromThrowable(exception1));
    // Updating the labels by appending the keys of labels with a "new"
    List<Label> updateLabelsList =
        createdLabelsList.stream()
            .map(
                label ->
                    Label.newBuilder()
                        .setId(label.getId())
                        .setData(
                            LabelData.newBuilder().setKey(label.getData().getKey() + "new").build())
                        .build())
            .collect(Collectors.toList());
    List<Label> updatedLabelsList =
        updateLabelsList.stream()
            .map(
                updateLabel -> {
                  UpdateLabelResponse response =
                      labelConfigStub.updateLabel(
                          UpdateLabelRequest.newBuilder()
                              .setId(updateLabel.getId())
                              .setData(updateLabel.getData())
                              .build());
                  return response.getLabel();
                })
            .collect(Collectors.toList());
    assertEquals(updateLabelsList, updatedLabelsList);
    // Updating a label that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelConfigStub.updateLabel(
                  UpdateLabelRequest.newBuilder()
                      .setId("1")
                      .setData(LabelData.newBuilder().setKey("API-X").build())
                      .build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void test_system_updateLabel() {
    List<Label> createdLabelsList = createLabels();
    for (Label systemLabel : systemLabels) {
      UpdateLabelRequest updateLabelRequest =
          UpdateLabelRequest.newBuilder()
              .setId(systemLabel.getId())
              .setData(LabelData.newBuilder().setKey("1").build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                labelConfigStub.updateLabel(updateLabelRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
    for (Label systemLabel : systemLabels) {
      UpdateLabelRequest updateLabelRequest =
          UpdateLabelRequest.newBuilder()
              .setId(createdLabelsList.get(0).getId())
              .setData(LabelData.newBuilder().setKey(systemLabel.getData().getKey()).build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                labelConfigStub.updateLabel(updateLabelRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
  }

  @Test
  void test_deleteLabel() {
    List<Label> createdLabelsList = createLabels();
    // Deleting a label that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelConfigStub.deleteLabel(DeleteLabelRequest.newBuilder().setId("1").build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
    // Deleting each label one by one and verifying the delete operation
    createdLabelsList.stream()
        .forEach(
            label -> {
              labelConfigStub.deleteLabel(
                  DeleteLabelRequest.newBuilder().setId(label.getId()).build());
              List<Label> allLabels =
                  labelConfigStub.getLabels(GetLabelsRequest.newBuilder().build()).getLabelsList();
              assertEquals(false, allLabels.contains(label));
            });
    List<Label> allLabels =
        labelConfigStub.getLabels(GetLabelsRequest.newBuilder().build()).getLabelsList();
    assertEquals(systemLabels, allLabels);
  }

  @Test
  void test_system_deleteLabel() {
    for (Label systemLabel : systemLabels) {
      DeleteLabelRequest deleteLabelRequest =
          DeleteLabelRequest.newBuilder().setId(systemLabel.getId()).build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                labelConfigStub.deleteLabel(deleteLabelRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
  }
}
