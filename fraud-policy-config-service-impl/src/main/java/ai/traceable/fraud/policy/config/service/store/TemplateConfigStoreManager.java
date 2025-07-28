package ai.traceable.fraud.policy.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.v1.CreateTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.Template;
import ai.traceable.fraud.policy.config.service.v1.UpdateTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.UpsertTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertTemplateResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TemplateConfigStoreManager {

  private final TemplateConfigStore templateConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public TemplateConfigStoreManager(
      TemplateConfigStore templateConfigStore, UuidGenerator uuidGenerator) {
    this.templateConfigStore = templateConfigStore;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateTemplateResponse createTemplate(
      RequestContext requestContext, CreateTemplateRequest request) {
    Template template = request.getTemplate();
    if (template.getTemplateId().isEmpty()) {
      template = template.toBuilder().setTemplateId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<Template> configObject =
        templateConfigStore.upsertObject(requestContext, template);
    Template created = buildTemplate(configObject);
    return CreateTemplateResponse.newBuilder().setTemplate(created).build();
  }

  public UpdateTemplateResponse updateTemplate(
      RequestContext requestContext, UpdateTemplateRequest request) throws StatusException {
    Template existing =
        fetchExistingTemplateOrThrow(request.getTemplate().getTemplateId(), requestContext);
    // do not allow update if the version provided is not the same as the one in the store.
    // caller must get latest and update if this exception is thrown
    if (!existing.getVersion().equals(request.getCurrentVersion())) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Current version does not match with the existing version.")
          .asException();
    }
    Template updatedTemplate =
        applyFieldMask(existing, request.getTemplate(), request.getUpdateMask());
    ContextualConfigObject<Template> configObject =
        templateConfigStore.upsertObject(requestContext, updatedTemplate);
    Template updated = buildTemplate(configObject);
    return UpdateTemplateResponse.newBuilder().setTemplate(updated).build();
  }

  public UpsertTemplateResponse upsertTemplate(
      RequestContext requestContext, UpsertTemplateRequest request) {
    Template template = request.getTemplate();
    if (template.getTemplateId().isEmpty()) {
      template = template.toBuilder().setTemplateId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<Template> configObject =
        templateConfigStore.upsertObject(requestContext, template);
    Template upserted = buildTemplate(configObject);
    return UpsertTemplateResponse.newBuilder().setTemplate(upserted).build();
  }

  public DeleteTemplateResponse deleteTemplateList(
      RequestContext requestContext, DeleteTemplateRequest request) throws StatusException {
    List<String> templateIds = request.getTemplateIdListList();
    if (templateIds.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No template IDs provided for deletion.")
          .asException();
    }
    List<DeletedContextualConfigObject<Template>> configObjects =
        templateConfigStore.deleteObjects(requestContext, templateIds);
    return DeleteTemplateResponse.newBuilder()
        .addAllTemplateList(buildTemplates(configObjects))
        .build();
  }

  public GetTemplateListResponse fetchTemplateList(
      RequestContext requestContext, GetTemplateListRequest request) {
    List<Template> templates = templateConfigStore.getAllConfigData(requestContext, request);
    return GetTemplateListResponse.newBuilder().addAllTemplateList(templates).build();
  }

  public GetTemplateResponse fetchTemplate(
      RequestContext requestContext, GetTemplateRequest request) throws StatusException {
    GetTemplateListRequest getTemplateListRequest =
        GetTemplateListRequest.newBuilder().addTemplateId(request.getTemplateId()).build();
    GetTemplateListResponse templateListResponse =
        fetchTemplateList(requestContext, getTemplateListRequest);
    if (templateListResponse.getTemplateListCount() == 0) {
      throw Status.NOT_FOUND
          .withDescription("No template of id = " + request.getTemplateId())
          .asException();
    }
    if (templateListResponse.getTemplateListCount() > 1) {
      throw Status.INTERNAL
          .withDescription(
              String.format(
                  "%d fraud policy templates found with id=%s",
                  templateListResponse.getTemplateListCount(), request.getTemplateId()))
          .asException();
    }
    return GetTemplateResponse.newBuilder()
        .setTemplate(templateListResponse.getTemplateList(0))
        .build();
  }

  private Template buildTemplate(ContextualConfigObject<Template> configObject) {
    return Template.newBuilder(configObject.getData()).build();
  }

  private List<Template> buildTemplates(
      List<DeletedContextualConfigObject<Template>> configObjects) {
    return configObjects.stream()
        .map(DeletedConfigObject::getDeletedData)
        .filter(Optional::isPresent)
        .map(data -> Template.newBuilder(data.get()).build())
        .collect(Collectors.toList());
  }

  private Template fetchExistingTemplateOrThrow(String templateId, RequestContext requestContext)
      throws StatusException {
    return templateConfigStore
        .getData(requestContext, templateId)
        .orElseThrow(Status.NOT_FOUND::asException);
  }
}
