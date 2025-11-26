package ca.uhn.fhir.jpa.starter.cohort.service.r5.config;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.cr.common.RepositoryFactoryForRepositoryInterface;
import ca.uhn.fhir.cr.config.CrBaseConfig;
import ca.uhn.fhir.cr.config.ProviderLoader;
import ca.uhn.fhir.cr.config.ProviderSelector;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.jpa.starter.cohort.provider.r5.StudyInstantiateProvider;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.StudyInstantiateServiceImpl;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.StudyInstantiateServiceFactory;
import ca.uhn.fhir.rest.server.RestfulServer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;

import java.util.List;
import java.util.Map;

@Configuration
@Conditional({CohortConfigCondition.class})
@Import({RepositoryConfig.class, CrBaseConfig.class})
public class StudyInstantiateConfig {

	@Bean
	StudyInstantiateServiceFactory studyInstantiateServiceFactory(@Lazy RepositoryFactoryForRepositoryInterface repositoryFactory){
		return requestDetails -> new StudyInstantiateServiceImpl(repositoryFactory.create(requestDetails));
	}

	@Bean
	public StudyInstantiateProvider studyInstantiateProvider(StudyInstantiateServiceFactory factory){
		return new StudyInstantiateProvider(factory);
	}

	@Bean(
		name = {"studyOperationLoader"}
	)
	public ProviderLoader populateOperationLoader(ApplicationContext theApplicationContext, FhirContext theFhirContext, RestfulServer theRestfulServer) {
		ProviderSelector selector = new ProviderSelector(theFhirContext, Map.of(FhirVersionEnum.R5, List.of(StudyInstantiateProvider.class)));
		return new ProviderLoader(theRestfulServer, theApplicationContext, selector);
	}
}
