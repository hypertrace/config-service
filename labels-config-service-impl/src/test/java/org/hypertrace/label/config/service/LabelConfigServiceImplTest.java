package org.hypertrace.label.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.label.config.service.v1.CreateLabel;
import org.hypertrace.label.config.service.v1.CreateLabelRequest;
import org.hypertrace.label.config.service.v1.CreateLabelResponse;
import org.hypertrace.label.config.service.v1.DeleteLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelRequest;
import org.hypertrace.label.config.service.v1.GetLabelResponse;
import org.hypertrace.label.config.service.v1.GetLabelsRequest;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.LabelConfigServiceGrpc.LabelConfigServiceBlockingStub;
import org.hypertrace.label.config.service.v1.UpdateLabelRequest;
import org.hypertrace.label.config.service.v1.UpdateLabelResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class LabelConfigServiceImplTest {
  LabelConfigServiceBlockingStub labelConfigStub;
  Config config;
  MockGenericConfigService mockGenericConfigService;
  List<Label> systemLabels =
      Arrays.asList("Critical", "Sensitive", "External", "Sentry").stream()
          .map(key -> Label.newBuilder().setId(key).setKey(key).build())
          .collect(Collectors.toList());
  List<CreateLabel> createLabelsList =
      Arrays.asList(0, 1, 2, 3, 4).stream()
          .map(id -> CreateLabel.newBuilder().setKey("Label-" + id).build())
          .collect(Collectors.toList());

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Map<String, List<Map<String, String>>> systemLabelsConfigMap = new HashMap<>();
    systemLabelsConfigMap.put(
        "system.labels",
        systemLabels.stream()
            .map(systemLabel -> Map.of("id", systemLabel.getId(), "key", systemLabel.getKey()))
            .collect(Collectors.toList()));
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("label.config.service", systemLabelsConfigMap);
    config = ConfigFactory.parseMap(configMap);
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService.addService(new LabelConfigServiceImpl(channel, config)).start();
    labelConfigStub = LabelConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  private List<Label> createLabels() {
    // Creating or inserting labels
    return createLabelsList.stream()
        .map(
            createLabel -> {
              CreateLabelResponse response =
                  labelConfigStub.createLabel(
                      CreateLabelRequest.newBuilder().setLabel(createLabel).build());
              return response.getLabel();
            })
        .collect(Collectors.toList());
  }

  @Test
  void test_createLabel() {
    List<CreateLabel> createdLabelsList =
        createLabels().stream()
            .map(label -> CreateLabel.newBuilder().setKey(label.getKey()).build())
            .collect(Collectors.toList());
    assertEquals(createLabelsList, createdLabelsList);
  }

  @Test
  void test_system_createLabel() {
    for (Label systemLabel : systemLabels) {
      CreateLabelRequest createLabelRequest =
          CreateLabelRequest.newBuilder()
              .setLabel(CreateLabel.newBuilder().setKey(systemLabel.getKey()).build())
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
    List<Label> createdLabelsList = createLabels();
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
    // Updating the labels by appending the keys of labels with a "new"
    List<Label> updateLabelsList =
        createdLabelsList.stream()
            .map(
                label ->
                    Label.newBuilder().setId(label.getId()).setKey(label.getKey() + "new").build())
            .collect(Collectors.toList());
    List<Label> updatedLabelsList =
        updateLabelsList.stream()
            .map(
                updateLabel -> {
                  UpdateLabelResponse response =
                      labelConfigStub.updateLabel(
                          UpdateLabelRequest.newBuilder().setLabel(updateLabel).build());
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
                      .setLabel(Label.newBuilder().setId("1").setKey("API-X").build())
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
              .setLabel(Label.newBuilder().setId(systemLabel.getId()).setKey("1").build())
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
              .setLabel(
                  Label.newBuilder()
                      .setId(createdLabelsList.get(0).getId())
                      .setKey(systemLabel.getKey())
                      .build())
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
