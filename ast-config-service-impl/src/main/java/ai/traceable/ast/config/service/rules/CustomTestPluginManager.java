package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.store.CustomTestPluginStore;
import ai.traceable.ast.config.service.v1.CreateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.CustomTestPlugin;
import ai.traceable.ast.config.service.v1.CustomTestPluginFilter;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.StringList;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CustomTestPluginManager {

  private final CustomTestPluginStore customTestPluginStore;

  public List<CustomTestPlugin> getAllCustomTestPlugins(
      RequestContext requestContext, GetAllCustomTestPluginsRequest request) {
    List<CustomTestPlugin> customTestPluginsList =
        customTestPluginStore.getAllConfigData(requestContext);

    CustomTestPluginFilter filter = request.getFilter();
    switch (filter.getTypeCase()) {
      case ID_FILTER:
        return getIdFilteredCustomTestPlugins(customTestPluginsList, filter.getIdFilter());
      case TYPE_NOT_SET: // when no filter is selected return all custom test plugins
        return customTestPluginsList;
      default:
        log.error("Unknown CustomTestPluginFilter type: {}", filter.getTypeCase());
    }

    return Collections.emptyList();
  }

  public void deleteCustomTestPlugin(
      RequestContext requestContext, DeleteCustomTestPluginRequest request) {
    log.info("Deleting custom test plugin with id: {}", request.getId());
    customTestPluginStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  public CustomTestPlugin createCustomTestPlugin(
      RequestContext requestContext, CreateCustomTestPluginRequest request) {

    String id = UUID.randomUUID().toString();
    CreateCustomTestPlugin createCustomTestPlugin = request.getCreateCustomTestPlugin();

    CustomTestPlugin createdCustomTestPlugin =
        CustomTestPlugin.newBuilder()
            .setId(id)
            .setName(createCustomTestPlugin.getName())
            .setCodeSnippetDetails(createCustomTestPlugin.getCodeSnippetDetails())
            .setDescription(createCustomTestPlugin.getDescription())
            .setPluginType(createCustomTestPlugin.getPluginType())
            .setPluginSafetyType(createCustomTestPlugin.getPluginSafetyType())
            .setPluginDetails(createCustomTestPlugin.getPluginDetails())
            .putAllTags(createCustomTestPlugin.getTagsMap())
            .addAllPotentialGeneratedVulnerabilityTypes(
                createCustomTestPlugin.getPotentialGeneratedVulnerabilityTypesList())
            .setSampleData(createCustomTestPlugin.getSampleData())
            .build();

    return customTestPluginStore.upsertObject(requestContext, createdCustomTestPlugin).getData();
  }

  public CustomTestPlugin updateCustomTestPlugin(
      RequestContext requestContext, UpdateCustomTestPluginRequest request) {
    CustomTestPlugin newCustomTestPlugin =
        CustomTestPlugin.newBuilder(
                customTestPluginStore
                    .getData(requestContext, request.getUpdateCustomTestPlugin().getId())
                    .orElseThrow())
            .build();
    CustomTestPlugin updatedCustomtestPlugin =
        buildUpdatedCustomTestPlugin(newCustomTestPlugin, request.getUpdateCustomTestPlugin());
    return customTestPluginStore.upsertObject(requestContext, updatedCustomtestPlugin).getData();
  }

  private CustomTestPlugin buildUpdatedCustomTestPlugin(
      CustomTestPlugin existingCustomTestPlugin, UpdateCustomTestPlugin updatedCustomTestPlugin) {
    return CustomTestPlugin.newBuilder(existingCustomTestPlugin)
        .setName(updatedCustomTestPlugin.getName())
        .setCodeSnippetDetails(updatedCustomTestPlugin.getCodeSnippetDetails())
        .setDescription(updatedCustomTestPlugin.getDescription())
        .setPluginType(updatedCustomTestPlugin.getPluginType())
        .setPluginSafetyType(updatedCustomTestPlugin.getPluginSafetyType())
        .setPluginDetails(updatedCustomTestPlugin.getPluginDetails())
        .clearPotentialGeneratedVulnerabilityTypes()
        .addAllPotentialGeneratedVulnerabilityTypes(
            updatedCustomTestPlugin.getPotentialGeneratedVulnerabilityTypesList())
        .clearTags()
        .putAllTags(updatedCustomTestPlugin.getTagsMap())
        .setSampleData(updatedCustomTestPlugin.getSampleData())
        .build();
  }

  private List<CustomTestPlugin> getIdFilteredCustomTestPlugins(
      List<CustomTestPlugin> customTestPluginsList, StringList idFilter) {
    List<String> customTestPluginIds = idFilter.getValuesList();
    return customTestPluginsList.stream()
        .filter(customTestPlugin -> customTestPluginIds.contains(customTestPlugin.getId()))
        .collect(Collectors.toUnmodifiableList());
  }
}
