package ca.uhn.fhir.jpa.starter.cdshooks.study.r5;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import org.hl7.fhir.r5.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteCqlClient {
	private static final Logger logger = LoggerFactory.getLogger(RemoteCqlClient.class);
	private final IGenericClient client;

	public RemoteCqlClient(FhirContext fhirContext, String cqlEndpoint) {
		fhirContext.getRestfulClientFactory().setServerValidationMode(
			ServerValidationModeEnum.NEVER
		);
		this.client = fhirContext.newRestfulGenericClient(cqlEndpoint);
	}

	public Parameters evaluateLibrary(
		Bundle patientData,
		String libraryId,
		String expression,
		String contentEndpoint,
		String dataEndpoint,
		String terminologyEndpoint
	) {
		Parameters evaluateParams = new Parameters();
		evaluateParams.addParameter()
			.setName("data")
			.setResource(patientData);
		evaluateParams.addParameter()
			.setName("expression")
			.setValue(new StringType(expression));
		evaluateParams.addParameter()
			.setName("dataEndpoint")
			.setResource(new Endpoint().setAddress(dataEndpoint));
		evaluateParams.addParameter()
			.setName("contentEndpoint")
			.setResource(new Endpoint().setAddress(contentEndpoint));
		evaluateParams.addParameter()
			.setName("terminologyEndpoint")
			.setResource(new Endpoint().setAddress(terminologyEndpoint));

		return client.operation()
			.onInstance(new IdType("Library", libraryId))
			.named("$evaluate")
			.withParameters(evaluateParams)
			.returnResourceType(Parameters.class).execute();
	}

	public Parameters evaluateLibrary(
		String patientId,
		String libraryId,
		String expression,
		String contentEndpoint,
		String dataEndpoint,
		String terminologyEndpoint
	) {
		Parameters evaluateParams = new Parameters();
		evaluateParams.addParameter()
			.setName("subject")
			.setValue(new StringType(patientId));
		evaluateParams.addParameter()
			.setName("expression")
			.setValue(new StringType(expression));
		evaluateParams.addParameter()
			.setName("dataEndpoint")
			.setResource(new Endpoint().setAddress(dataEndpoint));
		evaluateParams.addParameter()
			.setName("contentEndpoint")
			.setResource(new Endpoint().setAddress(contentEndpoint));
		evaluateParams.addParameter()
			.setName("terminologyEndpoint")
			.setResource(new Endpoint().setAddress(terminologyEndpoint));

		return client.operation()
			.onInstance(new IdType("Library", libraryId))
			.named("$evaluate")
			.withParameters(evaluateParams)
			.returnResourceType(Parameters.class).execute();
	}

	public boolean evaluateInclusionExpression(Bundle patientData,
															 String libraryId,
															 String expression,
															 String contentEndpoint,
															 String dataEndpoint,
															 String terminologyEndpoint) {
		Parameters parameters = evaluateLibrary(patientData, libraryId, expression, contentEndpoint, dataEndpoint, terminologyEndpoint);
		Parameters.ParametersParameterComponent expressionValue = parameters.getParameter(expression);
		return ((BooleanType) expressionValue.getValue()).booleanValue();
	}

	public boolean evaluateInclusionExpression(String patientId,
															 String libraryId,
															 String expression,
															 String contentEndpoint,
															 String dataEndpoint,
															 String terminologyEndpoint) {
		Parameters parameters = evaluateLibrary(patientId, libraryId, expression, contentEndpoint, dataEndpoint, terminologyEndpoint);
		Parameters.ParametersParameterComponent expressionValue = parameters.getParameter(expression);
		return ((BooleanType) expressionValue.getValue()).booleanValue();
	}
}