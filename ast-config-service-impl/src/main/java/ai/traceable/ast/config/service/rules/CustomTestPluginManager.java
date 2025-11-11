package ai.traceable.ast.config.service.rules;

import static ai.traceable.ast.config.service.v1.ApiType.API_TYPE_UNSPECIFIED;
import static ai.traceable.ast.config.service.v1.ApiType.UNRECOGNIZED;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.ast.config.service.store.CustomTestPluginStore;
import ai.traceable.ast.config.service.v1.ApiType;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomTestPluginManager {

  private final CustomTestPluginStore customTestPluginStore;

  @Inject
  public CustomTestPluginManager(CustomTestPluginStore customTestPluginStore) {
    this.customTestPluginStore = customTestPluginStore;
  }

  List<ApiType> DEFAULT_SUPPORTED_API_TYPES =
      Arrays.stream(ApiType.values())
          .filter(apiType -> apiType != API_TYPE_UNSPECIFIED && apiType != UNRECOGNIZED)
          .collect(toUnmodifiableList());

  public List<CustomTestPlugin> getAllCustomTestPlugins(
      RequestContext requestContext, GetAllCustomTestPluginsRequest request) {
    List<CustomTestPlugin> customTestPluginsList =
        customTestPluginStore.getAllConfigData(requestContext);
    customTestPluginsList = setDefaultSupportedApiTypes(customTestPluginsList);

    List<CustomTestPluginFilter> filters = request.getFiltersList();
    if (!filters.isEmpty()) {
      return applyFilters(customTestPluginsList, filters);
    }

    return customTestPluginsList;
  }

  List<CustomTestPlugin> setDefaultSupportedApiTypes(List<CustomTestPlugin> customTestPluginsList) {
    return customTestPluginsList.stream()
        .map(
            customTestPlugin -> {
              if (customTestPlugin.getSupportedApiTypesList().isEmpty()) {

                return customTestPlugin.toBuilder()
                    .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                    .build();
              }
              return customTestPlugin;
            })
        .collect(toUnmodifiableList());
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
            .addAllSupportedApiTypes(
                createCustomTestPlugin.getSupportedApiTypesList().isEmpty()
                    ? DEFAULT_SUPPORTED_API_TYPES
                    : createCustomTestPlugin.getSupportedApiTypesList())
            .setPluginScope(createCustomTestPlugin.getPluginScope())
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
        .addAllSupportedApiTypes(
            updatedCustomTestPlugin.getSupportedApiTypesValueList().isEmpty()
                ? DEFAULT_SUPPORTED_API_TYPES
                : updatedCustomTestPlugin.getSupportedApiTypesList())
        .setPluginScope(existingCustomTestPlugin.getPluginScope())
        .build();
  }

  private List<CustomTestPlugin> applyFilters(
      List<CustomTestPlugin> customTestPluginsList, List<CustomTestPluginFilter> filters) {

    for (CustomTestPluginFilter filter : filters) {
      switch (filter.getTypeCase()) {
        case ID_FILTER:
          customTestPluginsList =
              getIdFilteredCustomTestPlugins(customTestPluginsList, filter.getIdFilter());
          break;
        case ENV_ID_FILTER:
          customTestPluginsList =
              getEnvironmentFilteredCustomTestPlugins(
                  customTestPluginsList, filter.getEnvIdFilter());
          break;
        case TYPE_NOT_SET:
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown CustomTestPluginFilter type: " + filter.getTypeCase());
      }
    }

    return customTestPluginsList;
  }

  private List<CustomTestPlugin> getIdFilteredCustomTestPlugins(
      List<CustomTestPlugin> customTestPluginsList, StringList idFilter) {
    Set<String> pluginIdsToFilter = new HashSet<>(idFilter.getValuesList());
    return customTestPluginsList.stream()
        .filter(customTestPlugin -> pluginIdsToFilter.contains(customTestPlugin.getId()))
        .collect(toUnmodifiableList());
  }

  private List<CustomTestPlugin> getEnvironmentFilteredCustomTestPlugins(
      List<CustomTestPlugin> customTestPluginsList, StringList envIdFilter) {
    Set<String> requestedEnvironmentIds = new HashSet<>(envIdFilter.getValuesList());

    return customTestPluginsList.stream()
        .filter(
            customTestPlugin -> {

              // If plugin has environment scope, check if any requested env matches
              if (!customTestPlugin
                  .getPluginScope()
                  .getEnvironmentScope()
                  .getEnvironmentIdsList()
                  .isEmpty()) {
                List<String> pluginEnvironmentIds =
                    customTestPlugin.getPluginScope().getEnvironmentScope().getEnvironmentIdsList();

                // Return true if there's any intersection between requested and plugin environments
                return pluginEnvironmentIds.stream().anyMatch(requestedEnvironmentIds::contains);
              }

              // If plugin has scope but not environment scope, include it
              return true;
            })
        .collect(toUnmodifiableList());
  }
}
