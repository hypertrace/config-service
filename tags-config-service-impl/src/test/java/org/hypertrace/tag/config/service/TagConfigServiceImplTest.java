package org.hypertrace.tag.config.service;

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
import org.hypertrace.tag.config.service.v1.CreateTag;
import org.hypertrace.tag.config.service.v1.CreateTagRequest;
import org.hypertrace.tag.config.service.v1.CreateTagResponse;
import org.hypertrace.tag.config.service.v1.DeleteTagRequest;
import org.hypertrace.tag.config.service.v1.GetTagRequest;
import org.hypertrace.tag.config.service.v1.GetTagResponse;
import org.hypertrace.tag.config.service.v1.GetTagsRequest;
import org.hypertrace.tag.config.service.v1.Tag;
import org.hypertrace.tag.config.service.v1.TagConfigServiceGrpc;
import org.hypertrace.tag.config.service.v1.TagConfigServiceGrpc.TagConfigServiceBlockingStub;
import org.hypertrace.tag.config.service.v1.UpdateTagRequest;
import org.hypertrace.tag.config.service.v1.UpdateTagResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class TagConfigServiceImplTest {
  TagConfigServiceBlockingStub tagConfigStub;
  Config config;
  MockGenericConfigService mockGenericConfigService;
  List<Tag> systemTags =
      Arrays.asList("Critical", "Sensitive", "External", "Sentry").stream()
          .map(key -> Tag.newBuilder().setId(key).setKey(key).build())
          .collect(Collectors.toList());
  List<CreateTag> createTagsList =
      Arrays.asList(0, 1, 2, 3, 4).stream()
          .map(id -> CreateTag.newBuilder().setKey("Tag-" + id).build())
          .collect(Collectors.toList());

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Map<String, List<Map<String, String>>> systemTagsConfigMap = new HashMap<>();
    systemTagsConfigMap.put(
        "system.tags",
        systemTags.stream()
            .map(systemTag -> Map.of("id", systemTag.getId(), "key", systemTag.getKey()))
            .collect(Collectors.toList()));
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("tag.config.service", systemTagsConfigMap);
    config = ConfigFactory.parseMap(configMap);
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService.addService(new TagConfigServiceImpl(channel, config)).start();
    tagConfigStub = TagConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  private List<Tag> createTags() {
    // Creating or inserting tags
    return createTagsList.stream()
        .map(
            createTag -> {
              CreateTagResponse response =
                  tagConfigStub.createTag(CreateTagRequest.newBuilder().setTag(createTag).build());
              return response.getTag();
            })
        .collect(Collectors.toList());
  }

  @Test
  void test_createTag() {
    List<CreateTag> createdTagsList =
        createTags().stream()
            .map(tag -> CreateTag.newBuilder().setKey(tag.getKey()).build())
            .collect(Collectors.toList());
    assertEquals(createTagsList, createdTagsList);
  }

  @Test
  void test_system_createTag() {
    for (Tag systemTag : systemTags) {
      CreateTagRequest createTagRequest =
          CreateTagRequest.newBuilder()
              .setTag(CreateTag.newBuilder().setKey(systemTag.getKey()).build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                tagConfigStub.createTag(createTagRequest);
              });
      assertEquals(Status.ALREADY_EXISTS, Status.fromThrowable(exception));
    }
  }

  @Test
  void test_getTag() {
    List<Tag> createdTagsList = createTags();
    // Querying each tag one by one for the created or inserted tags in the previous step
    List<Tag> getTagList =
        createdTagsList.stream()
            .map(
                tag -> {
                  GetTagResponse response =
                      tagConfigStub.getTag(GetTagRequest.newBuilder().setId(tag.getId()).build());
                  System.out.println(response.getTag());
                  return response.getTag();
                })
            .collect(Collectors.toList());
    assertEquals(createdTagsList, getTagList);
    // Get a tag that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              tagConfigStub.getTag(GetTagRequest.newBuilder().setId("1").build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void test_system_getTag() {
    for (Tag systemTag : systemTags) {
      GetTagRequest getTagRequest = GetTagRequest.newBuilder().setId(systemTag.getId()).build();
      GetTagResponse getTagResponse = tagConfigStub.getTag(getTagRequest);
      assertEquals(systemTag, getTagResponse.getTag());
    }
  }

  @Test
  void test_getTags() {
    List<Tag> createdTagsList = createTags();
    createdTagsList.addAll(systemTags);
    // Querying for all the tags at once for the created or inserted tags in the previous step
    List<Tag> getTags = tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
    assertEquals(Set.copyOf(createdTagsList), Set.copyOf(getTags));
  }

  @Test
  void test_updateTag() {
    List<Tag> createdTagsList = createTags();
    // Updating the tags by appending the keys of tags with a "new"
    List<Tag> updateTagsList =
        createdTagsList.stream()
            .map(tag -> Tag.newBuilder().setId(tag.getId()).setKey(tag.getKey() + "new").build())
            .collect(Collectors.toList());
    List<Tag> updatedTagsList =
        updateTagsList.stream()
            .map(
                updateTag -> {
                  UpdateTagResponse response =
                      tagConfigStub.updateTag(
                          UpdateTagRequest.newBuilder().setTag(updateTag).build());
                  return response.getTag();
                })
            .collect(Collectors.toList());
    assertEquals(updateTagsList, updatedTagsList);
    // Updating a tag that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              tagConfigStub.updateTag(
                  UpdateTagRequest.newBuilder()
                      .setTag(Tag.newBuilder().setId("1").setKey("API-X").build())
                      .build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void test_system_updateTag() {
    List<Tag> createdTagsList = createTags();
    for (Tag systemTag : systemTags) {
      UpdateTagRequest updateTagRequest =
          UpdateTagRequest.newBuilder()
              .setTag(Tag.newBuilder().setId(systemTag.getId()).setKey("1").build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                tagConfigStub.updateTag(updateTagRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
    for (Tag systemTag : systemTags) {
      UpdateTagRequest updateTagRequest =
          UpdateTagRequest.newBuilder()
              .setTag(
                  Tag.newBuilder()
                      .setId(createdTagsList.get(0).getId())
                      .setKey(systemTag.getKey())
                      .build())
              .build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                tagConfigStub.updateTag(updateTagRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
  }

  @Test
  void test_deleteTag() {
    List<Tag> createdTagsList = createTags();
    // Deleting a tag that does not exist
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              tagConfigStub.deleteTag(DeleteTagRequest.newBuilder().setId("1").build());
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
    // Deleting each tag one by one and verifying the delete operation
    createdTagsList.stream()
        .forEach(
            tag -> {
              tagConfigStub.deleteTag(DeleteTagRequest.newBuilder().setId(tag.getId()).build());
              List<Tag> allTags =
                  tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
              assertEquals(false, allTags.contains(tag));
            });
    List<Tag> allTags = tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
    assertEquals(systemTags, allTags);
  }

  @Test
  void test_system_deleteTag() {
    for (Tag systemTag : systemTags) {
      DeleteTagRequest deleteTagRequest =
          DeleteTagRequest.newBuilder().setId(systemTag.getId()).build();
      Throwable exception =
          assertThrows(
              StatusRuntimeException.class,
              () -> {
                tagConfigStub.deleteTag(deleteTagRequest);
              });
      assertEquals(Status.INVALID_ARGUMENT, Status.fromThrowable(exception));
    }
  }
}
