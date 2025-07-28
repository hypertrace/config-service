package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.fraud.policy.config.service.v1.GetTemplateListRequest;
import ai.traceable.fraud.policy.config.service.v1.Template;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TemplateConfigStore
    extends IdentifiedObjectStoreWithFilter<Template, GetTemplateListRequest> {
  private static final String TEMPLATE_RESOURCE_NAME = "fraud-template";
  private static final String TEMPLATE_CONFIG_RESOURCE_NAMESPACE = "fraud-template-config";

  @Inject
  public TemplateConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        TEMPLATE_CONFIG_RESOURCE_NAMESPACE,
        TEMPLATE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<Template> buildDataFromValue(com.google.protobuf.Value value) {
    Template.Builder templateBuilder = Template.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, templateBuilder);
    return Optional.of(templateBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(Template template) {
    return ConfigProtoConverter.convertToValue(template);
  }

  @Override
  protected String getContextFromData(Template template) {
    return template.getTemplateId();
  }

  @Override
  protected Optional<Template> filterConfigData(Template data, GetTemplateListRequest request) {
    return Optional.of(data)
        .filter(
            template ->
                request.getTemplateIdCount() == 0
                    || request.getTemplateIdList().contains(template.getTemplateId()));
  }

  @Override
  public List<Template> getAllConfigData(
      RequestContext requestContext, GetTemplateListRequest request) {
    List<Template> templates = super.getAllConfigData(requestContext, request);
    return templates.stream()
        .filter(template -> filterConfigData(template, request).isPresent())
        .collect(Collectors.toUnmodifiableList());
  }
}
