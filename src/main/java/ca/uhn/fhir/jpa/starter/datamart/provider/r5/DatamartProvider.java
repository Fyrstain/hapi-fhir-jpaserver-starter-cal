package ca.uhn.fhir.jpa.starter.datamart.provider.r5;

import ca.uhn.fhir.jpa.starter.datamart.service.r5.impl.DatamartServiceFactory;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.hl7.fhir.r5.model.CanonicalType;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.ListResource;
import org.hl7.fhir.r5.model.ResearchStudy;

public class DatamartProvider {
	
	private final DatamartServiceFactory myFactory;

	public DatamartProvider(DatamartServiceFactory theFactory) {
		this.myFactory = theFactory;
	}

	/**
	 * Provides the implementation of the FHIR operation
	 * <a href="https://www.isis.com/OperationDefinition/OD-GenerateDatamart">
	 * $generate-datamart</a>.
	 * This operation generates a datamart based on a ResearchStudy resource and returns a
	 * {@link ListResource} containing the evaluated research-variable parameters for a list of subjects.
	 *
	 * @param researchStudyUrl      The canonical URL of the ResearchStudy for datamart generation.
	 * @param researchStudyEndpoint The endpoint containing the ResearchStudy, inclusion variables,
	 *                              and CQL libraries defining research variables.
	 * @param dataEndpoint          Endpoint to access data referenced by the retrieval operations in the library.
	 * @param terminologyEndpoint   (Optional) Endpoint to access terminology (ValueSets, CodeSystems) referenced by the library.
	 * @return A {@link ListResource} with the evaluated datamart parameters.
	 */
	@Operation(name = "$generate-datamart", idempotent = true, type = ResearchStudy.class)
	public ListResource generateDatamart(
		@OperationParam(name = "researchStudyUrl") CanonicalType researchStudyUrl,
		@OperationParam(name = "researchStudyEndpoint") Endpoint researchStudyEndpoint,
		@OperationParam(name = "dataEndpoint") Endpoint dataEndpoint,
		@OperationParam(name = "terminologyEndpoint") Endpoint terminologyEndpoint,
		@OperationParam(name = "cqlEngineEndpoint") Endpoint cqlEngineEndpoint,
		RequestDetails requestDetails
	) {
		return myFactory.create(requestDetails).generateDatamart(
			researchStudyUrl,
			researchStudyEndpoint,
			dataEndpoint,
			terminologyEndpoint,
			cqlEngineEndpoint
		);
	}
}
