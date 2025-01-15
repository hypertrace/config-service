package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.InternalFieldMetadata;
import java.util.Map;

public abstract class FraudDataModelConstants {
  public static final String COL_STR_PREFIX = "str";
  public static final String COL_LONG_PREFIX = "long";
  public static final String COL_BOOL_PREFIX = "bool";
  public static final String COL_DOUBLE_PREFIX = "double";
  public static final String COL_LIST_PREFIX = "list";
  public static final String COL_MAP_PREFIX = "map";
  public static final String COL_TIMESTAMP_PREFIX = "timestamp";
  public static final String COL_BINARY_PREFIX = "binary";
  public static final String COL_INT_PREFIX = "int";
  private static final String COL_SEGMENT = "_col";
  private static final String COL_INDEX_PREFIX = "idx";

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
          COL_TIMESTAMP_PREFIX,
          FieldType.FIELD_TYPE_BINARY,
          COL_BINARY_PREFIX,
          FieldType.FIELD_TYPE_INT,
          COL_INT_PREFIX);

  public static final Map<String, Integer> DEFAULT_FIELD_MAP =
      Map.of(
          COL_STR_PREFIX,
          100,
          COL_LONG_PREFIX,
          100,
          COL_DOUBLE_PREFIX,
          100,
          COL_BOOL_PREFIX,
          20,
          COL_LIST_PREFIX,
          10,
          COL_MAP_PREFIX,
          10,
          COL_TIMESTAMP_PREFIX,
          10,
          COL_BINARY_PREFIX,
          5,
          COL_INT_PREFIX,
          20,
          COL_INDEX_PREFIX,
          25);

  public static String getColumnPrefix() {
    return COL_SEGMENT;
  }

  public static String getKeyPrefix(InternalFieldMetadata fieldMetadata) {
    if (fieldMetadata.getIndexed()) {
      return COL_INDEX_PREFIX;
    }
    FieldType fieldType = fieldMetadata.getFieldType();
    return TYPE_TO_COLUMN_LOOKUP_MAP.get(fieldType);
  }

  public static String getColumnName(InternalFieldMetadata fieldMetadata, int index) {
    String fieldPrefix = getKeyPrefix(fieldMetadata);
    String colPrefix = getColumnPrefix();
    return fieldPrefix + colPrefix + index;
  }
}
