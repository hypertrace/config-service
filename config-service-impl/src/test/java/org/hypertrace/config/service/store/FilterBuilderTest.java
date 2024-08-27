package org.hypertrace.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import java.util.List;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.LogicalOperator;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.config.service.v1.RelationalOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilterBuilderTest {

  private FilterBuilder filterBuilder;

  @BeforeEach
  void init() {
    this.filterBuilder = new FilterBuilder();
  }

  @Test
  void buildDocStoreFilter1() {
    Filter filter =
        Filter.newBuilder()
            .setRelationalFilter(
                RelationalFilter.newBuilder()
                    .setConfigJsonPath("key1")
                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                    .setValue(Values.of("value1")))
            .build();

    org.hypertrace.core.documentstore.Filter docFilter =
        org.hypertrace.core.documentstore.Filter.eq("config.key1", "value1");
    assertEquals(docFilter.toString(), filterBuilder.buildDocStoreFilter(filter).toString());

    Filter filter2 =
        Filter.newBuilder()
            .setRelationalFilter(
                RelationalFilter.newBuilder()
                    .setConfigJsonPath("key2")
                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                    .setValue(Values.of(300)))
            .build();

    org.hypertrace.core.documentstore.Filter docFilter2 =
        org.hypertrace.core.documentstore.Filter.eq("config.key2", 300.0);
    assertEquals(docFilter2.toString(), filterBuilder.buildDocStoreFilter(filter2).toString());
  }

  @Test
  void buildDocStoreFilter2() {
    Filter filter =
        Filter.newBuilder()
            .setLogicalFilter(
                LogicalFilter.newBuilder()
                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                    .addOperands(
                        Filter.newBuilder()
                            .setRelationalFilter(
                                RelationalFilter.newBuilder()
                                    .setConfigJsonPath("key3")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                    .setValue(Values.of(true))))
                    .addOperands(
                        Filter.newBuilder()
                            .setRelationalFilter(
                                RelationalFilter.newBuilder()
                                    .setConfigJsonPath("key4")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_LTE)
                                    .setValue(Values.of(100)))))
            .build();

    org.hypertrace.core.documentstore.Filter docFilter =
        org.hypertrace.core.documentstore.Filter.eq("config.key3", true)
            .and(
                new org.hypertrace.core.documentstore.Filter(
                    org.hypertrace.core.documentstore.Filter.Op.LTE, "config.key4", 100.0));
    assertEquals(docFilter.toString(), filterBuilder.buildDocStoreFilter(filter).toString());
  }

  @Test
  void buildDocStoreFilter3() {
    Filter filter =
        Filter.newBuilder()
            .setLogicalFilter(
                LogicalFilter.newBuilder()
                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                    .addOperands(
                        Filter.newBuilder()
                            .setLogicalFilter(
                                LogicalFilter.newBuilder()
                                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addOperands(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilter.newBuilder()
                                                    .setConfigJsonPath("key5")
                                                    .setOperator(
                                                        RelationalOperator.RELATIONAL_OPERATOR_IN)
                                                    .setValue(
                                                        Value.newBuilder()
                                                            .setListValue(
                                                                ListValue.newBuilder()
                                                                    .addValues(
                                                                        Values.of("listValue1"))
                                                                    .addValues(
                                                                        Values.of("listValue2"))))))
                                    .addOperands(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilter.newBuilder()
                                                    .setConfigJsonPath("key6")
                                                    .setOperator(
                                                        RelationalOperator.RELATIONAL_OPERATOR_GTE)
                                                    .setValue(Values.of(100))))))
                    .addOperands(
                        Filter.newBuilder()
                            .setRelationalFilter(
                                RelationalFilter.newBuilder()
                                    .setConfigJsonPath("key7")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                    .setValue(Values.of("value7")))))
            .build();

    org.hypertrace.core.documentstore.Filter docFilter =
        new org.hypertrace.core.documentstore.Filter(
                org.hypertrace.core.documentstore.Filter.Op.IN,
                "config.key5",
                List.of("listValue1", "listValue2"))
            .and(
                new org.hypertrace.core.documentstore.Filter(
                    org.hypertrace.core.documentstore.Filter.Op.GTE, "config.key6", 100.0))
            .or(org.hypertrace.core.documentstore.Filter.eq("config.key7", "value7"));

    assertEquals(docFilter.toString(), filterBuilder.buildDocStoreFilter(filter).toString());
  }
}
