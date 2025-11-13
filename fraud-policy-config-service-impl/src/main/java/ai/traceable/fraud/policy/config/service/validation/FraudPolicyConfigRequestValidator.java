package ai.traceable.fraud.policy.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudPolicyConfigRequestValidator {
  private static final Set<String> REQUIRED_COLUMNS =
      Set.of(
          "primary_entity_type",
          "primary_entity_id",
          "primary_entity_name",
          "environment",
          "target",
          "target_type");

  public static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public static void validateRawSQLQuery(FraudPolicy fraudPolicy) {
    String rawSQLQuery = extractRawSQLQuery(fraudPolicy);
    if (rawSQLQuery == null || rawSQLQuery.trim().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("SQL query cannot be null or empty")
          .asRuntimeException();
    }

    // Simple case-sensitive check: just see if each required column appears in the query string
    Set<String> missingColumns = new HashSet<>();
    for (String requiredColumn : REQUIRED_COLUMNS) {
      if (!rawSQLQuery.contains(requiredColumn)) {
        missingColumns.add(requiredColumn);
      }
    }

    if (!missingColumns.isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "SQL query is missing required columns: %s", String.join(", ", missingColumns)))
          .asRuntimeException();
    }
  }

  private static String extractRawSQLQuery(FraudPolicy fraudPolicy) {
    String rawSqlQuery = null;
    if (fraudPolicy == null || !fraudPolicy.hasFraudPolicyRule()) {
      return rawSqlQuery;
    }

    var fraudPolicyRule = fraudPolicy.getFraudPolicyRule();
    if (!fraudPolicyRule.hasFraudRule()) {
      return rawSqlQuery;
    }

    var fraudRule = fraudPolicyRule.getFraudRule();

    // Extract raw SQL query based on which fraud rule type is present
    if (fraudRule.hasEntityGraphFraudRule()) {
      var entityGraphFraudRule = fraudRule.getEntityGraphFraudRule();
      if (entityGraphFraudRule.hasEntityGraphDataQuery()) {
        rawSqlQuery = entityGraphFraudRule.getEntityGraphDataQuery().getRawSqlQuery();
      }
    } else if (fraudRule.hasEntityMetricFraudRule()) {
      var entityMetricFraudRule = fraudRule.getEntityMetricFraudRule();
      if (entityMetricFraudRule.hasEntityMetricDataQuery()) {
        rawSqlQuery = entityMetricFraudRule.getEntityMetricDataQuery().getRawSqlQuery();
      }
    } else if (fraudRule.hasMetricFraudRule()) {
      var metricFraudRule = fraudRule.getMetricFraudRule();
      if (metricFraudRule.hasMetricDataQuery()) {
        rawSqlQuery = metricFraudRule.getMetricDataQuery().getRawSqlQuery();
      }
    }
    return rawSqlQuery;
  }
}
