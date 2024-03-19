package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
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

  @InjectMocks DataTypeResolver dataTypeResolver;

  @Test
  void testResolveWithNoDataSet() {
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setEnabled(false) // With no data set references, its treated as an orphan
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                    .setSensitivity(Sensitivity.SENSITIVITY_LOW))
            .build(),
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, Collections.emptyList()));
    assertEquals(
        DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS,
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS, Collections.emptyList()));
  }

  @Test
  void testResolveWithSingleDataSet() {
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setDataSuppression(DATA_SET_2.getInfo().getDataSuppression())
                    .setSensitivity(DATA_SET_2.getInfo().getSensitivity())
                    .setEnabled(DATA_SET_2.getInfo().getEnabled())
                    .addDataSetId(DATA_SET_2.getId()))
            .build(),
        this.dataTypeResolver.resolve(DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, List.of(DATA_SET_2)));

    assertEquals(
        DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS.getRule().toBuilder()
                    .addDataSetId(DATA_SET_1.getId()))
            .build(),
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_ASSIGNED_RESOLVED_FIELDS, List.of(DATA_SET_1)));
  }

  @Test
  void testEnabledDataSetsGivenResolutionPrecedence() {
    // Dataset 1 should have precedence since it is enabled
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setDataSuppression(DATA_SET_1.getInfo().getDataSuppression())
                    .setSensitivity(DATA_SET_1.getInfo().getSensitivity())
                    .setEnabled(DATA_SET_1.getInfo().getEnabled())
                    .addDataSetId(DATA_SET_1.getId())
                    .addDataSetId(DATA_SET_2.getId()))
            .build(),
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, List.of(DATA_SET_1, DATA_SET_2)));
  }

  @Test
  void testRedactedDataSetsGivenPrecedence() {
    // Dataset 2 should have precedence once we enable it since it's redacting
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setDataSuppression(DATA_SET_2.getInfo().getDataSuppression())
                    .setSensitivity(DATA_SET_2.getInfo().getSensitivity())
                    .addDataSetId(DATA_SET_1.getId())
                    .addDataSetId(DATA_SET_2.getId())
                    .setEnabled(true))
            .build(),
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS,
            List.of(
                DATA_SET_1,
                DATA_SET_2.toBuilder()
                    .setInfo(DATA_SET_2.getInfo().toBuilder().setEnabled(true))
                    .build())));
  }

  @Test
  void testDataTypeSetRelationshipsAreMerged() {
    // DS2 has a ref to the DT, and the DT has a ref to DS1
    DataType dataTypeWithRef =
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .addDataSetId(DATA_SET_1.getId()))
            .build();
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setDataSuppression(DATA_SET_1.getInfo().getDataSuppression())
                    .setSensitivity(DATA_SET_1.getInfo().getSensitivity())
                    .setEnabled(DATA_SET_1.getInfo().getEnabled())
                    .addDataSetId(DATA_SET_1.getId())
                    .addDataSetId(DATA_SET_2.getId()))
            .build(),
        this.dataTypeResolver.resolve(dataTypeWithRef, List.of(DATA_SET_1, DATA_SET_2)));
  }

  @Test
  void testResolvingWithDatasetMissingFields() {
    DataSet dataSetMissingFields =
        DATA_SET_2.toBuilder().setInfo(DATA_SET_2.getInfo().toBuilder().clearSensitivity()).build();
    assertEquals(
        DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.toBuilder()
            .setRule(
                DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS.getRule().toBuilder()
                    .setDataSuppression(DATA_SET_2.getInfo().getDataSuppression())
                    .setSensitivity(Sensitivity.SENSITIVITY_LOW) // default
                    .setEnabled(DATA_SET_2.getInfo().getEnabled())
                    .addDataSetId(DATA_SET_2.getId()))
            .build(),
        this.dataTypeResolver.resolve(
            DATA_TYPE_WITH_UNSET_RESOLVED_FIELDS, List.of(dataSetMissingFields)));
  }
}
