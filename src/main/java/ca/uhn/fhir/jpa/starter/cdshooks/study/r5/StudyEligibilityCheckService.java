package ca.uhn.fhir.jpa.starter.cdshooks.study.r5;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.rest.api.server.cdshooks.CdsServiceRequestContextJson;
import ca.uhn.fhir.rest.api.server.cdshooks.CdsServiceRequestJson;
import ca.uhn.hapi.fhir.cdshooks.api.CdsService;
import ca.uhn.hapi.fhir.cdshooks.api.CdsServiceFeedback;
import ca.uhn.hapi.fhir.cdshooks.api.CdsServicePrefetch;
import ca.uhn.hapi.fhir.cdshooks.api.json.*;
import org.hl7.fhir.r5.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@Profile("R5")
public class StudyEligibilityCheckService {
	private static final Logger LOG = LoggerFactory.getLogger(StudyEligibilityCheckService.class);

	/**
	 * Main CDS Hooks operation invoked on the patient-view hook.
	 *
	 * @param theCdsRequest JSON wrapper of the CDS service request, including context and prefetched data
	 * @return CdsServiceResponseJson containing a single card with eligibility information
	 */
	@CdsService(
		value = "research-eligibility-check",
		hook = "patient-view",
		title = "Research study eligibility verification.",
		description = "Evaluate the inclusion criteria defined in a CQL Library.",
		prefetch = {
			@CdsServicePrefetch(value = "patientData", query = "Patient/{{context.patientId}}")
		})
	public CdsServiceResponseJson exampleService(CdsServiceRequestJson theCdsRequest) {
		Bundle patientData = (Bundle) theCdsRequest.getPrefetch("patientData");
		CdsServiceResponseJson response = new CdsServiceResponseJson();
		CdsServiceResponseCardJson card = new CdsServiceResponseCardJson();

		CdsServiceRequestContextJson context = theCdsRequest.getContext();
		try {
			String fhirServer = theCdsRequest.getFhirServer();
			String patientId = getRequiredContextField(context, "patientId");
			String libraryId = getRequiredContextField(context, "libraryId");

			String studyId = getRequiredContextField(context, "studyId");
			String inclusionExpression = getRequiredContextField(context, "inclusionExpression");
			String contentServer = getRequiredContextField(context, "contentServer");
			String terminologyServer = context.getString("terminologyServer");
			String cqlEngineServer = getRequiredContextField(context, "CQLEngineServer");

			RemoteCqlClient cqlClient = new RemoteCqlClient(FhirContext.forR5(), cqlEngineServer);
			boolean isEligible;
			if (patientData != null) {
				isEligible = cqlClient.evaluateInclusionExpression(
					patientData,
					libraryId,
					inclusionExpression,
					contentServer,
					fhirServer,
					terminologyServer
				);
			} else {
				isEligible = cqlClient.evaluateInclusionExpression(
					patientId,
					libraryId,
					inclusionExpression,
					contentServer,
					fhirServer,
					terminologyServer
				);
			}


			if (isEligible) {
				card.setIndicator(CdsServiceIndicatorEnum.INFO);
				card.setSummary("Patient eligible for clinical study " + studyId);
				card.setDetail("Patient meets all inclusion criteria and has no exclusion conditions. Consider including this patient in the study.");
			} else {
				card.setIndicator(CdsServiceIndicatorEnum.WARNING);
				card.setSummary("Patient not eligible for study " + studyId);
				card.setDetail("Patient does not meet all inclusion criteria or has an exclusion condition.");
			}

		} catch (IllegalArgumentException e) {
			LOG.error("Missing or invalid parameter: {}", e.getMessage());
			card.setIndicator(CdsServiceIndicatorEnum.CRITICAL);
			card.setSummary("Missing parameter error");
			card.setDetail("Required parameter missing or invalid: " + e.getMessage());
		} catch (Exception e) {
			LOG.error("Error evaluating CQL for patient/study: {}", e.getMessage(), e);
			card.setIndicator(CdsServiceIndicatorEnum.CRITICAL);
			card.setSummary("Error evaluating eligibility");
			card.setDetail("An unexpected error occurred while evaluating eligibility.");
		}
		// Set source information
		CdsServiceResponseCardSourceJson source = new CdsServiceResponseCardSourceJson();
		source.setLabel("Research Eligibility CDS Service");
		source.setUrl("https://www.isis.com");
		card.setSource(source);

		response.addCard(card);
		return response;
	}

	/**
	 * Validate that the context contains a non-null, non-empty value for the given key.
	 *
	 * @param context   the CDS service context
	 * @param fieldName the field name
	 * @return the field value
	 * @throws IllegalArgumentException if the value is missing or empty
	 */
	private String getRequiredContextField(CdsServiceRequestContextJson context, String fieldName) {
		String value = context.getString(fieldName);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName);
		}
		return value;
	}

	private String getFhirServer(CdsServiceRequestJson request) {
		String value = request.getFhirServer();
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("fhirServer");
		}
		return value;
	}

	@CdsServiceFeedback("research-eligibility-check")
	public CdsServiceFeedbackJson feedback(CdsServiceFeedbackJson theFeedback) {
		theFeedback.setAcceptedSuggestions(List.of(new CdsServiceAcceptedSuggestionJson().setId(UUID.randomUUID().toString())));
		return theFeedback;
	}
}
