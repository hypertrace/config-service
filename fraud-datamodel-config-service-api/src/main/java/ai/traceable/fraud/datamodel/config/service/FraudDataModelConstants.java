package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import java.util.Map;

public abstract class FraudDataModelConstants {
  public static final String COL_STR_PREFIX = "str";
  public static final String COL_LONG_PREFIX = "long";
  public static final String COL_BOOL_PREFIX = "bool";
  public static final String COL_DOUBLE_PREFIX = "double";
  public static final String COL_LIST_PREFIX = "list";
  public static final String COL_MAP_PREFIX = "map";
  public static final String COL_TIMESTAMP_PREFIX = "timestamp";
  private static final String COL_SEGMENT = ".col.";

  public static final Map<FieldType, String> TYPE_TO_COLUMN_LOOKUP_MAP =
      Map.of(
          FieldType.FIELD_TYPE_STR,
          COL_STR_PREFIX,
          FieldType.FIELD_TYPE_LONG,
          COL_LONG_PREFIX,
          FieldType.FIELD_TYPE_DOUBLE,
          COL_DOUBLE_PREFIX,
          FieldType.FIELD_TYPE_BOOL,
          COL_BOOL_PREFIX,
          FieldType.FIELD_TYPE_LIST,
          COL_LIST_PREFIX,
          FieldType.FIELD_TYPE_MAP,
          COL_MAP_PREFIX,
          FieldType.FIELD_TYPE_TIMESTAMP,
          COL_TIMESTAMP_PREFIX);

  public static final Map<String, Integer> DEFAULT_FIELD_MAP =
      Map.of(
          COL_STR_PREFIX, 100,
          COL_LONG_PREFIX, 100,
          COL_DOUBLE_PREFIX, 100,
          COL_BOOL_PREFIX, 20,
          COL_LIST_PREFIX, 10,
          COL_MAP_PREFIX, 10,
          COL_TIMESTAMP_PREFIX, 10);

  public static String getColumnPrefix() {
    return COL_SEGMENT;
  }
}
