package ca.uhn.fhir.jpa.starter.datamart.service.r5;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.IdType;
import org.hl7.fhir.r5.model.Parameters;
import org.opencds.cqf.fhir.api.Repository;

public class RemoteCqlClient {

	private final IGenericClient client;

	public RemoteCqlClient(Endpoint endpoint, Repository repository) {
		FhirContext ctx = repository.fhirContext();
		ctx.getRestfulClientFactory().setServerValidationMode(
			ServerValidationModeEnum.NEVER
		);
		this.client = ctx.newRestfulGenericClient(endpoint.getAddress());
	}

	public Parameters evaluateLibrary(
		Parameters parameters, String libraryId
	) {
		Parameters outParams = client
			.operation()
			.onInstance(new IdType("Library", libraryId))
			.named("$evaluate")
			.withParameters(parameters)
			.returnResourceType(Parameters.class)
			.execute();
		return outParams;
	}
}
