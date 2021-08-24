package org.hypertrace.tag.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Channel;
import java.util.Arrays;
import java.util.List;
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
  MockGenericConfigService mockGenericConfigService;
  List<CreateTag> createTagsList =
      Arrays.asList(0, 1, 2, 3, 4).stream()
          .map(id -> CreateTag.newBuilder().setKey("Tag-" + id).build())
          .collect(Collectors.toList());

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService.addService(new TagConfigServiceImpl(channel)).start();
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
    String exceptionMessage = "NONE";
    try {
      tagConfigStub.getTag(GetTagRequest.newBuilder().setId("1").build());
    } catch (Exception e) {
      exceptionMessage = e.getMessage();
      System.out.println(e);
    }
    assertEquals("UNKNOWN", exceptionMessage);
  }

  @Test
  void test_getTags() {
    List<Tag> createdTagsList = createTags();
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
    String exceptionMessage = "NONE";
    try {
      tagConfigStub.updateTag(
          UpdateTagRequest.newBuilder()
              .setTag(Tag.newBuilder().setId("1").setKey("API-X").build())
              .build());
    } catch (Exception e) {
      exceptionMessage = e.getMessage();
      System.out.println(e);
    }
    assertEquals("UNKNOWN", exceptionMessage);
  }

  @Test
  void test_deleteTag() {
    List<Tag> createdTagsList = createTags();
    // Deleting a tag that does not exist
    String exceptionMessage = "NONE";
    try {
      tagConfigStub.deleteTag(DeleteTagRequest.newBuilder().setId("1").build());
    } catch (Exception e) {
      exceptionMessage = e.getMessage();
      System.out.println(e);
    }
    assertEquals("UNKNOWN", exceptionMessage);
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
    assertEquals(true, allTags.isEmpty());
  }
}
