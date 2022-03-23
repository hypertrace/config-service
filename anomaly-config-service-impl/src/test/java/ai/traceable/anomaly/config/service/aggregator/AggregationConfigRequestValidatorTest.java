package ai.traceable.anomaly.config.service.aggregator;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

public class AggregationConfigRequestValidatorTest {

  private AggregationConfigRequestValidator aggregationConfigRequestValidator =
      new AggregationConfigRequestValidator();

  @Test
  public void testValidateUpdateAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    UpdateScopedAnomalyEventAggregationConfigRequest
        updateScopedAnomalyEventAggregationConfigRequestDefault =
            UpdateScopedAnomalyEventAggregationConfigRequest.newBuilder()
                .setScopedAnomalyEventAggregationConfig(
                    ScopedAnomalyEventAggregationConfig.getDefaultInstance())
                .build();
    UpdateScopedAnomalyEventAggregationConfigRequest
        updateScopedAnomalyEventAggregationConfigRequest =
            UpdateScopedAnomalyEventAggregationConfigRequest.newBuilder()
                .setScopedAnomalyEventAggregationConfig(
                    ScopedAnomalyEventAggregationConfig.newBuilder()
                        .setConfigScope(
                            AnomalyConfigScope.newBuilder()
                                .setApiScope(AnomalyApiScope.newBuilder().setId("1").build())
                                .build())
                        .setEventAggregationConfig(
                            EventAggregationConfig.newBuilder()
                                .setGlobalConfig(EventAggregationGlobalConfig.newBuilder().build())
                                .build()))
                .build();
    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator.validateUpdateAggregatorConfigurationRequest(
                RequestContext.CURRENT.get(), updateScopedAnomalyEventAggregationConfigRequest));
    // Invalid request
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator.validateUpdateAggregatorConfigurationRequest(
                requestContext, updateScopedAnomalyEventAggregationConfigRequestDefault));

    aggregationConfigRequestValidator.validateUpdateAggregatorConfigurationRequest(
        requestContext, updateScopedAnomalyEventAggregationConfigRequest);
  }

  @Test
  public void testValidateGetAllScopedAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    GetAllScopedAnomalyEventAggregationConfigsRequest
        getAllScopedAnomalyEventAggregationConfigsRequest =
            GetAllScopedAnomalyEventAggregationConfigsRequest.getDefaultInstance();

    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator
                .validateGetAllScopedAnomalyEventAggregationConfigsRequest(
                    RequestContext.CURRENT.get(),
                    getAllScopedAnomalyEventAggregationConfigsRequest));
    aggregationConfigRequestValidator.validateGetAllScopedAnomalyEventAggregationConfigsRequest(
        requestContext, getAllScopedAnomalyEventAggregationConfigsRequest);
  }

  @Test
  public void testValidateGetAllUnresolvedScopedAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest
        getAllUnresolvedScopedAnomalyEventAggregationConfigsRequest =
            GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest.getDefaultInstance();

    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator
                .validateGetAllUnresolvedScopedAnomalyEventAggregationConfigs(
                    RequestContext.CURRENT.get(),
                    getAllUnresolvedScopedAnomalyEventAggregationConfigsRequest));
    aggregationConfigRequestValidator.validateGetAllUnresolvedScopedAnomalyEventAggregationConfigs(
        requestContext, getAllUnresolvedScopedAnomalyEventAggregationConfigsRequest);
  }

  @Test
  public void testValidateGetScopedAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    GetScopedAnomalyEventAggregationConfigRequest
        getScopedAnomalyEventAggregationConfigRequestDefault =
            GetScopedAnomalyEventAggregationConfigRequest.getDefaultInstance();
    GetScopedAnomalyEventAggregationConfigRequest getScopedAnomalyEventAggregationConfigRequest =
        GetScopedAnomalyEventAggregationConfigRequest.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setApiScope(AnomalyApiScope.newBuilder().setId("id1").build())
                    .build())
            .build();
    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator.validateGetScopedAnomalyEventAggregationConfig(
                RequestContext.CURRENT.get(), getScopedAnomalyEventAggregationConfigRequest));
    // Invalid request
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator.validateGetScopedAnomalyEventAggregationConfig(
                requestContext, getScopedAnomalyEventAggregationConfigRequestDefault));

    aggregationConfigRequestValidator.validateGetScopedAnomalyEventAggregationConfig(
        requestContext, getScopedAnomalyEventAggregationConfigRequest);
  }

  @Test
  public void testValidateGetUnresolvedScopedAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    GetUnresolvedScopedAnomalyEventAggregationConfigRequest
        getScopedAnomalyEventAggregationConfigRequestDefault =
            GetUnresolvedScopedAnomalyEventAggregationConfigRequest.getDefaultInstance();
    GetUnresolvedScopedAnomalyEventAggregationConfigRequest
        getScopedAnomalyEventAggregationConfigRequest =
            GetUnresolvedScopedAnomalyEventAggregationConfigRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setApiScope(AnomalyApiScope.newBuilder().setId("id1").build())
                        .build())
                .build();
    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator
                .validateGetUnresolvedScopedAnomalyEventAggregationConfig(
                    RequestContext.CURRENT.get(), getScopedAnomalyEventAggregationConfigRequest));
    // Invalid request
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator
                .validateGetUnresolvedScopedAnomalyEventAggregationConfig(
                    requestContext, getScopedAnomalyEventAggregationConfigRequestDefault));

    aggregationConfigRequestValidator.validateGetUnresolvedScopedAnomalyEventAggregationConfig(
        requestContext, getScopedAnomalyEventAggregationConfigRequest);
  }

  @Test
  public void testValidateDeleteAggregationRequest() {
    RequestContext requestContext = RequestContext.forTenantId("customer1");
    DeleteScopedAnomalyEventAggregationConfigRequest
        deleteScopedAnomalyEventAggregationConfigRequest =
            DeleteScopedAnomalyEventAggregationConfigRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                        .build())
                .build();
    // Empty Request Context
    assertThrows(
        RuntimeException.class,
        () ->
            aggregationConfigRequestValidator
                .validateDeleteScopedAnomalyEventAggregationConfigRequest(
                    RequestContext.CURRENT.get(),
                    deleteScopedAnomalyEventAggregationConfigRequest));

    aggregationConfigRequestValidator.validateDeleteScopedAnomalyEventAggregationConfigRequest(
        requestContext, deleteScopedAnomalyEventAggregationConfigRequest);
  }
}
