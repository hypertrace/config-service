package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataClassificationConfigTest {
  @Test
  void systemDataTypesTest() {
    String jsonString =
        "data.classification.config.service.system : {\n"
            + "datasets.rp1 : [],"
            + "datasets.rp2 : [],"
            + "datatypes : {\n"
            + "rp1 : [\n"
            + "{\n"
            + "id : systemdatatyperp1,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp1,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "],\n"
            + "rp2 : [\n"
            + "{\n"
            + "id : systemdatatyperp2,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp2,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}";
    DataClassificationConfig config =
        new DataClassificationConfig(ConfigFactory.parseString(jsonString));
    // RP1 case
    List<DataType> rp1DataTypes =
        config.getSystemDataTypes(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1);
    assertEquals(1, rp1DataTypes.size());
    DataType actualDataType = rp1DataTypes.get(0);
    assertEquals("systemdatatyperp1", actualDataType.getId());
    assertEquals("systemdatatyperulerp1", actualDataType.getRule().getName());
    assertEquals(
        LOCATION_REQUEST_HEADER,
        actualDataType.getRule().getScopedPatternsList().get(0).getLocations(0));

    // RP2 case
    List<DataType> rp2DataTypes =
        config.getSystemDataTypes(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_UNSPECIFIED);
    assertEquals(1, rp2DataTypes.size());
    actualDataType = rp2DataTypes.get(0);
    assertEquals("systemdatatyperp2", actualDataType.getId());
    assertEquals("systemdatatyperulerp2", actualDataType.getRule().getName());
    assertEquals(
        LOCATION_REQUEST_HEADER,
        actualDataType.getRule().getScopedPatternsList().get(0).getLocations(0));
  }

  @Test
  void systemDataSetsTest() {
    String jsonString =
        "{\n"
            + "  data.classification.config.service.system: {\n"
            + "    datatypes.rp1: [],\n"
            + "    datatypes.rp2: [],\n"
            + "    \"datasets\": {\n"
            + "    \"rp1\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdatasetrp1\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinforp1\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatyperp1-1\",\n"
            + "            \"datatyperp1-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"rp2\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdatasetrp2\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinforp2\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatyperp2-1\",\n"
            + "            \"datatyperp2-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "  }\n"
            + "}";
    DataClassificationConfig config =
        new DataClassificationConfig(ConfigFactory.parseString(jsonString));
    // RP1 case
    List<DataSet> systemDataSets =
        config.getSystemDataSets(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1);
    assertEquals(1, systemDataSets.size());
    DataSet actualDataSet = systemDataSets.get(0);
    assertEquals("systemdatasetrp1", actualDataSet.getId());
    assertEquals("systemdatasetinforp1", actualDataSet.getInfo().getName());
    assertEquals(
        List.of("datatyperp1-1", "datatyperp1-2"), actualDataSet.getInfo().getDataTypeIdsList());

    // RP2 case
    systemDataSets =
        config.getSystemDataSets(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_UNSPECIFIED);
    assertEquals(1, systemDataSets.size());
    actualDataSet = systemDataSets.get(0);
    assertEquals("systemdatasetrp2", actualDataSet.getId());
    assertEquals("systemdatasetinforp2", actualDataSet.getInfo().getName());
    assertEquals(
        List.of("datatyperp2-1", "datatyperp2-2"), actualDataSet.getInfo().getDataTypeIdsList());

    assertEquals(Optional.of(actualDataSet), config.getSystemDataSet("systemdatasetrp2"));
    assertTrue(config.isSystemDataSet("systemdatasetrp2"));
    assertEquals(Optional.empty(), config.getSystemDataSet("madeUpId"));
    assertFalse(config.isSystemDataSet("madeUpId"));
  }
}
