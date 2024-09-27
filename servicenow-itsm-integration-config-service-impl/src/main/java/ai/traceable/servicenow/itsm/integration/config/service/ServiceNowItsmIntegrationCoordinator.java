package ai.traceable.servicenow.itsm.integration.config.service;

import static ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFieldType.SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ASSIGNEE_GROUP_REFERENCE;
import static ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFieldType.SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ASSIGNEE_REFERENCE;
import static ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFieldType.SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_CALLER_REFERENCE;
import static ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFieldType.SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ENUMERATED_CHOICE;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.DeleteServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowIntegrationDetails;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegration;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFieldsConfiguration;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationTablesConfiguration;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationRequest;
import com.google.inject.Inject;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ServiceNowItsmIntegrationCoordinator {
  private final ServiceNowItsmIntegrationStore serviceNowItsmIntegrationStore;

  public CreateServiceNowItsmIntegrationResponse createServiceNowItsmIntegration(
      CreateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {

    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials =
        buildIntegrationWithAuthCredentials(request);

    serviceNowItsmIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials);

    ServiceNowItsmIntegration.Builder integrationWithoutAuthCredentialsBuilder =
        ServiceNowItsmIntegration.newBuilder()
            .setId(integrationWithAuthCredentials.getId())
            .setIntegrationDetails(
                withDefaultTableConfigurationIfNotPresent(integrationWithAuthCredentials)
                    .getIntegrationDetails());

    return CreateServiceNowItsmIntegrationResponse.newBuilder()
        .setServiceNowItsmIntegration(integrationWithoutAuthCredentialsBuilder)
        .build();
  }

  public List<ServiceNowItsmIntegration> getServiceNowItsmIntegration(
      GetServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    List<ServiceNowItsmIntegrationWithAuthCredentials> integrationsWithAuthCredentials =
        serviceNowItsmIntegrationStore.getAllConfigData(requestContext, request.getFilter());
    return integrationsWithAuthCredentials.stream()
        .map(this::withDefaultTableConfigurationIfNotPresent)
        .map(
            integrationWithAuthCredentials ->
                ServiceNowItsmIntegration.newBuilder()
                    .setId(integrationWithAuthCredentials.getId())
                    .setIntegrationDetails(integrationWithAuthCredentials.getIntegrationDetails())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  public ServiceNowItsmIntegrationWithAuthCredentials
      getServiceNowItsmIntegrationWithAuthCredentials(
          GetServiceNowItsmIntegrationWithAuthCredentialsRequest request,
          RequestContext requestContext) {
    return serviceNowItsmIntegrationStore
        .getData(requestContext, request.getIntegrationId())
        .map(this::withDefaultTableConfigurationIfNotPresent)
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to fetch ServiceNow-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public List<ServiceNowItsmIntegrationWithAuthCredentials>
      getServiceNowItsmIntegrationsWithAuthCredentials(
          GetServiceNowItsmIntegrationsWithAuthCredentialsRequest request,
          RequestContext requestContext) {
    return serviceNowItsmIntegrationStore
        .getAllConfigData(requestContext, request.getFilter())
        .stream()
        .map(this::withDefaultTableConfigurationIfNotPresent)
        .collect(Collectors.toUnmodifiableList());
  }

  public void deleteServiceNowItsmIntegration(
      DeleteServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    serviceNowItsmIntegrationStore
        .deleteObject(requestContext, request.getIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to delete ServiceNow-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public ServiceNowItsmIntegration updateServiceNowItsmIntegration(
      UpdateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials =
        serviceNowItsmIntegrationStore
            .getData(requestContext, request.getIntegrationId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            "Unable to update ServiceNow-Integration with given Id as it does not exist")
                        .asRuntimeException());

    ServiceNowItsmIntegrationWithAuthCredentials updatedIntegrationWithAuthCredentials =
        buildUpdatedIntegrationWithAuthCredentials(request, integrationWithAuthCredentials);
    serviceNowItsmIntegrationStore.upsertObject(
        requestContext, updatedIntegrationWithAuthCredentials);

    return ServiceNowItsmIntegration.newBuilder()
        .setId(request.getIntegrationId())
        .setIntegrationDetails(
            withDefaultTableConfigurationIfNotPresent(updatedIntegrationWithAuthCredentials)
                .getIntegrationDetails())
        .build();
  }

  private ServiceNowItsmIntegrationWithAuthCredentials buildIntegrationWithAuthCredentials(
      CreateServiceNowItsmIntegrationRequest request) {
    ServiceNowIntegrationDetails.Builder integrationDetails =
        ServiceNowIntegrationDetails.newBuilder()
            .setName(request.getName())
            .setServerUrl(request.getServerUrl());
    if (request.hasScope()) {
      integrationDetails.setScope(request.getScope());
    }
    if (request.hasDescription()) {
      integrationDetails.setDescription(request.getDescription());
    }
    String uuid = UUID.randomUUID().toString();
    ServiceNowItsmIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
            .setId(uuid)
            .setIntegrationDetails(integrationDetails)
            .setAuthCredentials(request.getAuthCredentials());

    return integrationWithAuthCredentialsBuilder.build();
  }

  private ServiceNowItsmIntegrationWithAuthCredentials buildUpdatedIntegrationWithAuthCredentials(
      UpdateServiceNowItsmIntegrationRequest request,
      ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials) {
    ServiceNowItsmIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        integrationWithAuthCredentials.toBuilder();
    ServiceNowIntegrationDetails.Builder integrationDetailsAtBuilder =
        integrationWithAuthCredentialsBuilder.getIntegrationDetails().toBuilder();
    integrationDetailsAtBuilder.setName(request.getName());
    integrationDetailsAtBuilder.setDescription(request.getDescription());
    integrationDetailsAtBuilder.setServerUrl(request.getServerUrl());
    integrationDetailsAtBuilder.clearScope();
    if (request.hasScope()) {
      integrationDetailsAtBuilder.setScope(request.getScope());
    }
    integrationWithAuthCredentialsBuilder.setIntegrationDetails(integrationDetailsAtBuilder);
    if (request.hasAuthCredentials()) {
      integrationWithAuthCredentialsBuilder.setAuthCredentials(request.getAuthCredentials());
    }
    return integrationWithAuthCredentialsBuilder.build();
  }

  private ServiceNowItsmIntegrationWithAuthCredentials withDefaultTableConfigurationIfNotPresent(
      ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials) {
    ServiceNowIntegrationDetails integrationDetails =
        integrationWithAuthCredentials.getIntegrationDetails();
    if (integrationDetails.getTableConfigurationsList().isEmpty()) {
      return integrationWithAuthCredentials.toBuilder()
          .setIntegrationDetails(
              integrationDetails.toBuilder()
                  .addTableConfigurations(getDefaultTableConfiguration())
                  .build())
          .build();
    }
    return integrationWithAuthCredentials;
  }

  private ServiceNowItsmIntegrationTablesConfiguration getDefaultTableConfiguration() {
    List<Value> impactAndSeverityValues =
        List.of(
            Value.newBuilder()
                .setStructValue(
                    Struct.newBuilder()
                        .putFields("index", Values.of(3))
                        .putFields("displayName", Values.of("Low")))
                .build(),
            Value.newBuilder()
                .setStructValue(
                    Struct.newBuilder()
                        .putFields("index", Values.of(2))
                        .putFields("displayName", Values.of("Medium")))
                .build(),
            Value.newBuilder()
                .setStructValue(
                    Struct.newBuilder()
                        .putFields("index", Values.of(1))
                        .putFields("displayName", Values.of("High")))
                .build());
    return ServiceNowItsmIntegrationTablesConfiguration.newBuilder()
        .setTableConfigurationId("defaultConfiguration")
        .setTableName("incident")
        .addFieldsConfigurations(
            ServiceNowItsmIntegrationFieldsConfiguration.newBuilder()
                .setColumnName("caller_id")
                .setDisplayName("Caller")
                .setFieldType(SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_CALLER_REFERENCE))
        .addFieldsConfigurations(
            ServiceNowItsmIntegrationFieldsConfiguration.newBuilder()
                .setColumnName("assignment_group")
                .setDisplayName("Assignee Group")
                .setFieldType(SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ASSIGNEE_GROUP_REFERENCE))
        .addFieldsConfigurations(
            ServiceNowItsmIntegrationFieldsConfiguration.newBuilder()
                .setColumnName("assigned_to")
                .setDisplayName("Assignee")
                .setFieldType(SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ASSIGNEE_REFERENCE))
        .addFieldsConfigurations(
            ServiceNowItsmIntegrationFieldsConfiguration.newBuilder()
                .setColumnName("severity")
                .setDisplayName("Severity")
                .setFieldType(SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ENUMERATED_CHOICE)
                .setDefaultValue(impactAndSeverityValues.get(0))
                .addAllValues(impactAndSeverityValues))
        .addFieldsConfigurations(
            ServiceNowItsmIntegrationFieldsConfiguration.newBuilder()
                .setColumnName("impact")
                .setDisplayName("Impact")
                .setFieldType(SERVICE_NOW_ITSM_INTEGRATION_FIELD_TYPE_ENUMERATED_CHOICE)
                .setDefaultValue(impactAndSeverityValues.get(0))
                .addAllValues(impactAndSeverityValues))
        .build();
  }
}
