package ca.uhn.fhir.jpa.starter.cohort.provider.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.StudyInstantiateServiceFactory;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.hl7.fhir.r5.model.*;

public class StudyInstantiateProvider {
	
	private StudyInstantiateServiceFactory myFactory;

	public StudyInstantiateProvider(StudyInstantiateServiceFactory myFactory) {
		this.myFactory = myFactory;
	}

	/**
	 * Provides the implementation of the FHIR operation <a href=
	 * "https://www.isis.com/OperationDefinition/OD-StudyInstantiate">$instantiate-study</a>.
	 * This operation allows cohort selection associated with a ResearchStudy using specified parameters
	 * for inclusion criteria, data access, and supporting terminologies.
	 *
	 * @param studyUrl       The canonical URL of the ResearchStudy used for instantiate-study.
	 * @param researchStudyEndpoint  The endpoint containing the ResearchStudy and associated resources.
	 * @return A {@link Group} object representing the eligible cohort of patients.
	 */
	@Operation(name = "$instantiate-study", idempotent = true, type = ResearchStudy.class)
	public Parameters instantiateStudy(
		@OperationParam(name = "studyUrl") CanonicalType studyUrl,
		@OperationParam(name = "researchStudyEndpoint") Endpoint researchStudyEndpoint,
		RequestDetails requestDetails) {
		return myFactory.create(requestDetails).instantiateStudy(studyUrl, researchStudyEndpoint);
	}
}
