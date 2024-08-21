package org.hypertrace.config.service.store;

import static org.hypertrace.config.service.store.ConfigDocument.CONFIG_FIELD_NAME;

import io.grpc.Status;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.core.documentstore.Filter;

class FilterBuilder {

  public Filter buildDocStoreFilter(org.hypertrace.config.service.v1.Filter filter) {
    switch (filter.getTypeCase()) {
      case LOGICAL_FILTER:
        return evaluateCompositeExpression(filter.getLogicalFilter());
      case RELATIONAL_FILTER:
        return evaluateLeafExpression(filter.getRelationalFilter());
      case TYPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Filter type unset").asRuntimeException();
    }
  }

  private Filter evaluateCompositeExpression(LogicalFilter logicalFilter) {
    switch (logicalFilter.getOperator()) {
      case LOGICAL_OPERATOR_OR:
        {
          Filter[] childFilters =
              logicalFilter.getOperandsList().stream()
                  .map(this::buildDocStoreFilter)
                  .toArray(Filter[]::new);
          Filter filter = new Filter();
          filter.setOp(Filter.Op.OR);
          filter.setChildFilters(childFilters);
          return filter;
        }
      case LOGICAL_OPERATOR_AND:
        {
          Filter[] childFilters =
              logicalFilter.getOperandsList().stream()
                  .map(this::buildDocStoreFilter)
                  .toArray(Filter[]::new);
          Filter filter = new Filter();
          filter.setOp(Filter.Op.AND);
          filter.setChildFilters(childFilters);
          return filter;
        }
      case LOGICAL_OPERATOR_UNSPECIFIED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown logical operator while building filter")
            .asRuntimeException();
    }
  }

  private Filter evaluateLeafExpression(RelationalFilter relationalFilter) {
    switch (relationalFilter.getOperator()) {
      case RELATIONAL_OPERATOR_EQ:
        return new Filter(
            Filter.Op.EQ,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_NEQ:
        return new Filter(
            Filter.Op.NEQ,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_IN:
        return new Filter(
            Filter.Op.IN,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_NOT_IN:
        return new Filter(
            Filter.Op.NOT_IN,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_LT:
        return new Filter(
            Filter.Op.LT,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_GT:
        return new Filter(
            Filter.Op.GT,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_LTE:
        return new Filter(
            Filter.Op.LTE,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case RELATIONAL_OPERATOR_GTE:
        return new Filter(
            Filter.Op.GTE,
            buildConfigFieldPath(relationalFilter.getConfigJsonPath()),
            relationalFilter.getValue());
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown relational operator while building filter")
            .asRuntimeException();
    }
  }

  private String buildConfigFieldPath(String configJsonPath) {
    return String.format("%s.%s", CONFIG_FIELD_NAME, configJsonPath);
  }
}
