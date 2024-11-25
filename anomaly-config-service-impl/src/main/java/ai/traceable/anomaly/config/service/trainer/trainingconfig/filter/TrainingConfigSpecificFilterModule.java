package ai.traceable.anomaly.config.service.trainer.trainingconfig.filter;

import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.DEMO_APPLICATION_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.LOCAL_TRAINING_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.METADATA_TRAINING_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.SENSITIVE_DATA_TRAINING_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.SESSION_TRAINING_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.VOLUMETRIC_TRAINING_CONFIG;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.API_NAMING_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.DEMO_APPLICATION_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.LOCAL_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.METADATA_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.SENSITIVE_DATA_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.SESSION_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase.VOLUMETRIC_TRAINING_CONFIG_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.API_PARAM_CONTAINS_URL;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.CONTENT_TYPE_OPTIONS_SECURITY_HEADER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.CSP_SECURITY_HEADER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.DEPRECATED_API_ROUTE;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.DIRECTORY_LISTING;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.ENUMERABLE_PARAM;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.EXCESS_DATA_EXPOSURE;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.HSTS_SECURITY_HEADER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.JAVA_SERIALIZED_OBJECT;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.JWT_ALGO_WEAKNESS;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.JWT_EXPIRY_AND_ISSUE_AT_NOT_SET;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.LACK_OF_ENCRYPTION;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.MASS_PARAMETER_ASSIGNMENT;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.MISSING_GATEWAY_POLICIES;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.MULTIPLE_API_VERSIONS;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.OPEN_REDIRECT;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.PARAM_CONTAINS_SENSITIVE_DATA;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.SENSITIVE_DATA_IN_ERROR_MESSAGE;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.SERVICE_USES_BASIC_AUTH;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig.ConfigCase.SQL_INJECTION_ERROR_BASED;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.API_PARAM_CONTAINS_URL_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.CONTENT_TYPE_OPTIONS_SECURITY_HEADER_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.CSP_SECURITY_HEADER_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.DEPRECATED_API_ROUTE_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.DIRECTORY_LISTING_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.ENUMERABLE_PARAM_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.EXCESS_DATA_EXPOSURE_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.HSTS_SECURITY_HEADER_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.JAVA_SERIALIZED_OBJECT_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.JWT_ALGO_WEAKNESS_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.JWT_EXPIRY_AND_ISSUE_AT_NOT_SET_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.LACK_OF_ENCRYPTION_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.MASS_PARAMETER_ASSIGNMENT_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.MISSING_GATEWAY_POLICIES_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.MULTIPLE_API_VERSIONS_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.OPEN_REDIRECT_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.PARAM_CONTAINS_SENSITIVE_DATA_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.SENSITIVE_DATA_IN_ERROR_MESSAGE_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.SERVICE_USES_BASIC_AUTH_FILTER;
import static ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTypeFilter.TypeCase.SQL_INJECTION_ERROR_BASED_FILTER;

import ai.traceable.anomaly.config.service.trainer.trainingconfig.filter.vulnerability.DefaultVulnerabilityTypeFilterMatcher;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.filter.vulnerability.VulnerabilityTypeFilterMatcher;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class TrainingConfigSpecificFilterModule extends AbstractModule {

  @Override
  protected void configure() {
    bindTrainingConfigFilters();
    bindVulnerabilityTypeFilters();
  }

  private void bindVulnerabilityTypeFilters() {
    Multibinder<VulnerabilityTypeFilterMatcher> vulnerabilityTypeFilterMatcherMultibinder =
        Multibinder.newSetBinder(binder(), VulnerabilityTypeFilterMatcher.class);

    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                PARAM_CONTAINS_SENSITIVE_DATA_FILTER, PARAM_CONTAINS_SENSITIVE_DATA));

    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                LACK_OF_ENCRYPTION_FILTER, LACK_OF_ENCRYPTION));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                API_PARAM_CONTAINS_URL_FILTER, API_PARAM_CONTAINS_URL));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                CONTENT_TYPE_OPTIONS_SECURITY_HEADER_FILTER, CONTENT_TYPE_OPTIONS_SECURITY_HEADER));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                HSTS_SECURITY_HEADER_FILTER, HSTS_SECURITY_HEADER));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                JAVA_SERIALIZED_OBJECT_FILTER, JAVA_SERIALIZED_OBJECT));

    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                SERVICE_USES_BASIC_AUTH_FILTER, SERVICE_USES_BASIC_AUTH));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                CSP_SECURITY_HEADER_FILTER, CSP_SECURITY_HEADER));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(DIRECTORY_LISTING_FILTER, DIRECTORY_LISTING));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(ENUMERABLE_PARAM_FILTER, ENUMERABLE_PARAM));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                DEPRECATED_API_ROUTE_FILTER, DEPRECATED_API_ROUTE));

    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                MULTIPLE_API_VERSIONS_FILTER, MULTIPLE_API_VERSIONS));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                EXCESS_DATA_EXPOSURE_FILTER, EXCESS_DATA_EXPOSURE));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(new DefaultVulnerabilityTypeFilterMatcher(OPEN_REDIRECT_FILTER, OPEN_REDIRECT));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                MASS_PARAMETER_ASSIGNMENT_FILTER, MASS_PARAMETER_ASSIGNMENT));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                SQL_INJECTION_ERROR_BASED_FILTER, SQL_INJECTION_ERROR_BASED));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(JWT_ALGO_WEAKNESS_FILTER, JWT_ALGO_WEAKNESS));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                MISSING_GATEWAY_POLICIES_FILTER, MISSING_GATEWAY_POLICIES));
    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                SENSITIVE_DATA_IN_ERROR_MESSAGE_FILTER, SENSITIVE_DATA_IN_ERROR_MESSAGE));

    vulnerabilityTypeFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultVulnerabilityTypeFilterMatcher(
                JWT_EXPIRY_AND_ISSUE_AT_NOT_SET_FILTER, JWT_EXPIRY_AND_ISSUE_AT_NOT_SET));
  }

  private void bindTrainingConfigFilters() {

    Multibinder<TrainingConfigTypeFilterMatcher> trainingConfigSpecificFilterMatcherMultibinder =
        Multibinder.newSetBinder(binder(), TrainingConfigTypeFilterMatcher.class);

    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                METADATA_TRAINING_CONFIG_FILTER, METADATA_TRAINING_CONFIG));
    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                SESSION_TRAINING_CONFIG_FILTER, SESSION_TRAINING_CONFIG));

    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                API_NAMING_TRAINING_CONFIG_FILTER, SESSION_TRAINING_CONFIG));

    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                SENSITIVE_DATA_TRAINING_CONFIG_FILTER, SENSITIVE_DATA_TRAINING_CONFIG));
    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                LOCAL_TRAINING_CONFIG_FILTER, LOCAL_TRAINING_CONFIG));
    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                VOLUMETRIC_TRAINING_CONFIG_FILTER, VOLUMETRIC_TRAINING_CONFIG));
    trainingConfigSpecificFilterMatcherMultibinder
        .addBinding()
        .toInstance(
            new DefaultTrainingConfigTypeFilterMatcher(
                DEMO_APPLICATION_TRAINING_CONFIG_FILTER, DEMO_APPLICATION_CONFIG));
  }
}
