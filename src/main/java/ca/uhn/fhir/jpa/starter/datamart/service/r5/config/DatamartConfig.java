package ca.uhn.fhir.jpa.starter.datamart.service.r5.config;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.cr.common.RepositoryFactoryForRepositoryInterface;
import ca.uhn.fhir.cr.config.CrBaseConfig;
import ca.uhn.fhir.cr.config.ProviderLoader;
import ca.uhn.fhir.cr.config.ProviderSelector;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.jpa.starter.datamart.provider.r5.DatamartProvider;
import ca.uhn.fhir.jpa.starter.datamart.provider.r5.ExportDatamartProvider;
import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.impl.DatamartExportServiceFactory;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.impl.DatamartServiceFactory;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartExportService;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartService;
import ca.uhn.fhir.rest.server.RestfulServer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;

import java.util.Arrays;
import java.util.Map;

@Configuration
@Conditional({DatamartConfigCondition.class})
@Import({RepositoryConfig.class, CrBaseConfig.class})
public class DatamartConfig {

	@Bean
	DatamartServiceFactory datamartServiceFactory(@Lazy RepositoryFactoryForRepositoryInterface theRepositoryFactory) {
		return (rd) -> new DatamartService(theRepositoryFactory.create(rd), DatamartEvaluationOptions.defaultOptions());
	}

	@Bean
	public DatamartProvider datamartOperationProvider(DatamartServiceFactory datamartServiceFactory) {
		return new DatamartProvider(datamartServiceFactory);
	}

	@Bean
	DatamartExportServiceFactory datamartExportServiceFactory(@Lazy RepositoryFactoryForRepositoryInterface theRepositoryFactory) {
		return (rd) -> new DatamartExportService(theRepositoryFactory.create(rd));
	}

	@Bean
	public ExportDatamartProvider datamartExportOperationProvider(DatamartExportServiceFactory datamartExportServiceFactory) {
		return new ExportDatamartProvider(datamartExportServiceFactory);
	}

	@Bean(
		name = {"datamartOperationLoader"}
	)
	public ProviderLoader populateOperationLoader(ApplicationContext theApplicationContext, FhirContext theFhirContext, RestfulServer theRestfulServer) {
		ProviderSelector selector = new ProviderSelector(theFhirContext, Map.of(FhirVersionEnum.R5, Arrays.asList(ExportDatamartProvider.class, DatamartProvider.class)));
		return new ProviderLoader(theRestfulServer, theApplicationContext, selector);
	}
}
