package ca.uhn.fhir.jpa.starter.cohort.provider.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.CohorteServiceFactory;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.hl7.fhir.r5.model.CanonicalType;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.Group;
import org.hl7.fhir.r5.model.ResearchStudy;

public class CohorteProvider {

	private final CohorteServiceFactory myFactory;

	public CohorteProvider(CohorteServiceFactory myFactory) {
		this.myFactory = myFactory;
	}

	/**
	 * Provides the implementation of the FHIR operation <a href=
	 * "https://www.isis.com/OperationDefinition/OD-Cohorting">$cohorting</a>.
	 * This operation allows cohort selection associated with a ResearchStudy using specified parameters
	 * for inclusion criteria, data access, and supporting terminologies.
	 *
	 * @param researchStudyUrl       The canonical URL of the ResearchStudy used for cohorting.
	 * @param researchStudyEndpoint  The endpoint containing the ResearchStudy and associated resources.
	 * @param dataEndpoint           Endpoint providing access to the referenced data.
	 * @param theTerminologyEndpoint Optional endpoint providing access to the referenced terminology.
	 * @return A {@link Group} object representing the eligible cohort of patients.
	 */
	@Operation(name = "$cohorting", idempotent = true, type = ResearchStudy.class)
	public Group cohorting(
		@OperationParam(name = "researchStudyUrl") CanonicalType researchStudyUrl,
		@OperationParam(name = "researchStudyEndpoint") Endpoint researchStudyEndpoint,
		@OperationParam(name = "dataEndpoint") Endpoint dataEndpoint,
		@OperationParam(name = "terminologyEndpoint") Endpoint theTerminologyEndpoint,
		@OperationParam(name = "cqlEngineEndpoint") Endpoint cqlEngineEndpoint,
		RequestDetails requestDetails) {
		return myFactory.create(requestDetails).cohorting(
			researchStudyUrl,
			researchStudyEndpoint,
			dataEndpoint,
			theTerminologyEndpoint,
			cqlEngineEndpoint);
	}
}
