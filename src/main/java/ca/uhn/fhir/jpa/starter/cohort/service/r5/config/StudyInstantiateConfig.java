package ca.uhn.fhir.jpa.starter.cohort.service.r5.config;

import ca.uhn.fhir.cr.common.RepositoryFactoryForRepositoryInterface;
import ca.uhn.fhir.cr.config.CrBaseConfig;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.jpa.starter.cohort.provider.r5.StudyInstantiateProvider;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.StudyInstantiateServiceImpl;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.StudyInstantiateServiceFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
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
}
