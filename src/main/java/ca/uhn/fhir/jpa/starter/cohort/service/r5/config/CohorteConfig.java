package ca.uhn.fhir.jpa.starter.cohort.service.r5.config;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.cr.common.RepositoryFactoryForRepositoryInterface;
import ca.uhn.fhir.cr.config.CrBaseConfig;
import ca.uhn.fhir.cr.config.ProviderLoader;
import ca.uhn.fhir.cr.config.ProviderSelector;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.jpa.starter.cohort.provider.r5.CohorteProvider;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteService;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.CohorteServiceFactory;
import ca.uhn.fhir.rest.server.RestfulServer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;

import java.util.List;
import java.util.Map;

@Configuration
@Conditional({CohortConfigCondition.class})
@Import({RepositoryConfig.class, CrBaseConfig.class})
public class CohorteConfig {

	@Bean
	CohorteServiceFactory cohorteServiceFactory(@Lazy RepositoryFactoryForRepositoryInterface theRepositoryFactory) {
		return (rd) -> new CohorteService(theRepositoryFactory.create(rd), CohorteEvaluationOptions.defaultOptions());
	}

	@Bean
	public CohorteProvider cohortProvider(CohorteServiceFactory cohorteServiceFactory) {
		return new CohorteProvider(cohorteServiceFactory);
	}

	@Bean(
		name = {"cohortOperationLoader"}
	)
	public ProviderLoader populateOperationLoader(ApplicationContext theApplicationContext, FhirContext theFhirContext, RestfulServer theRestfulServer) {
		ProviderSelector selector = new ProviderSelector(theFhirContext, Map.of(FhirVersionEnum.R5, List.of(CohorteProvider.class)));
		return new ProviderLoader(theRestfulServer, theApplicationContext, selector);
	}
}
