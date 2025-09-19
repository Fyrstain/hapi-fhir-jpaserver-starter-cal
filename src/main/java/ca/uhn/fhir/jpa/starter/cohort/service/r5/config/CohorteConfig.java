package ca.uhn.fhir.jpa.starter.cohort.service.r5.config;

import ca.uhn.fhir.cr.common.RepositoryFactoryForRepositoryInterface;
import ca.uhn.fhir.cr.config.CrBaseConfig;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.jpa.starter.cohort.provider.r5.CohorteProvider;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteService;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.CohorteServiceFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
@Import({RepositoryConfig.class, CrBaseConfig.class})
public class CohorteConfig {

	@Bean
	CohorteServiceFactory cohorteServiceFactory(@Lazy RepositoryFactoryForRepositoryInterface theRepositoryFactory) {
		return (rd) -> new CohorteService(theRepositoryFactory.create(rd), CohorteEvaluationOptions.defaultOptions());
	}

	@Bean
	public CohorteProvider datamartOperationProvider(CohorteServiceFactory cohorteServiceFactory) {
		return new CohorteProvider(cohorteServiceFactory);
	}
}
