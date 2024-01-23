package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataTypeResolverTest {
  static final DataType DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS =
      DataType.newBuilder().setId("dt-1").setRule(DataTypeRule.newBuilder()).build();
  static final DataType DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS =
      DataType.newBuilder()
          .setId("dt-2")
          .setRule(
              DataTypeRule.newBuilder()
                  .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                  .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                  .setEnabled(false))
          .build();

  static final DataSet DATA_SET_1 =
      DataSet.newBuilder()
          .setId("dataset-1")
          .setInfo(
              DataSetInfo.newBuilder()
                  .setName("data set 1")
                  .setEnabled(true)
                  .setColor("blue")
                  .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                  .setSensitivity(Sensitivity.SENSITIVITY_HIGH)
                  .addDataTypeIds(DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getId())
                  .addDataTypeIds(DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.getId()))
          .build();

  static final DataSet DATA_SET_2 =
      DataSet.newBuilder()
          .setId("dataset-2")
          .setInfo(
              DataSetInfo.newBuilder()
                  .setName("data set 2")
                  .setEnabled(false)
                  .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                  .setSensitivity(Sensitivity.SENSITIVITY_CRITICAL)
                  .addDataTypeIds(DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getId()))
          .build();

  RequestContext testContext = RequestContext.forTenantId("test-tenant");

  @Mock DataClassificationConfigServiceBlockingStub mockStub;
  @InjectMocks DataTypeResolver dataTypeResolver;

  @Test
  void testResolveWithNoDataSet() {
    when(mockStub.getDataSets(any())).thenReturn(GetDataSetsResponse.getDefaultInstance());
    assertEquals(
        List.of(DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS),
        this.dataTypeResolver.resolveInheritedDataTypeFields(
            testContext,
            List.of(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS)));
  }

  @Test
  void testResolveWithSingleDataSet() {
    when(mockStub.getDataSets(any()))
        .thenReturn(GetDataSetsResponse.newBuilder().addDataSets(DATA_SET_2).build());
    assertEquals(
        List.of(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
                .setRule(
                    DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                        .setDataSuppression(DATA_SET_2.getInfo().getDataSuppression())
                        .setSensitivity(DATA_SET_2.getInfo().getSensitivity())
                        .setEnabled(DATA_SET_2.getInfo().getEnabled()))
                .build(),
            DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS),
        this.dataTypeResolver.resolveInheritedDataTypeFields(
            testContext,
            List.of(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS)));
  }

  @Test
  void testEnabledDataSetsGivenResolutionPrecedence() {
    // Dataset 1 should have precedence since it is enabled
    when(mockStub.getDataSets(any()))
        .thenReturn(
            GetDataSetsResponse.newBuilder()
                .addDataSets(DATA_SET_1)
                .addDataSets(DATA_SET_2)
                .build());
    assertEquals(
        List.of(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
                .setRule(
                    DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                        .setDataSuppression(DATA_SET_1.getInfo().getDataSuppression())
                        .setSensitivity(DATA_SET_1.getInfo().getSensitivity())
                        .setColor(DATA_SET_1.getInfo().getColor())
                        .setEnabled(DATA_SET_1.getInfo().getEnabled()))
                .build(),
            DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.toBuilder()
                .setRule(
                    DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.getRule().toBuilder()
                        .setColor(DATA_SET_1.getInfo().getColor()))
                .build()),
        this.dataTypeResolver.resolveInheritedDataTypeFields(
            testContext,
            List.of(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS)));
  }

  @Test
  void testRedactedDataSetsGivenPrecedence() {
    // Dataset 2 should have precedence once we enable it since it's redacting, but it only contains
    // one data type - DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS
    when(mockStub.getDataSets(any()))
        .thenReturn(
            GetDataSetsResponse.newBuilder()
                .addDataSets(DATA_SET_1)
                .addDataSets(
                    DATA_SET_2.toBuilder()
                        .setInfo(DATA_SET_2.getInfo().toBuilder().setEnabled(true)))
                .build());
    assertEquals(
        List.of(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
                .setRule(
                    DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                        .setDataSuppression(DATA_SET_2.getInfo().getDataSuppression())
                        .setSensitivity(DATA_SET_2.getInfo().getSensitivity())
                        .setEnabled(true)) // enabled overridden in this test
                .build(),
            DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.toBuilder()
                .setRule(
                    DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.getRule().toBuilder()
                        .setColor(DATA_SET_1.getInfo().getColor()))
                .build()),
        this.dataTypeResolver.resolveInheritedDataTypeFields(
            testContext,
            List.of(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS)));
  }
}
