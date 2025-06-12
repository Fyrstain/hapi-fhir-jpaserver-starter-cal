package ca.uhn.fhir.jpa.starter.cdshooks.study.r5;

import ca.uhn.fhir.context.FhirContext;
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

	private RemoteCqlClient remoteCqlClient;

	public void setRemoteCqlClient(RemoteCqlClient remoteCqlClient) {
		this.remoteCqlClient = remoteCqlClient;
	}

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

		String fhirServer = theCdsRequest.getFhirServer();
		CdsServiceRequestContextJson context = theCdsRequest.getContext();
		String patientId = context.getString("patientId");
		String libraryId = context.getString("libraryId");
		String studyId = context.getString("studyId");
		String inclusionExpression = context.getString("inclusionExpression");
		String contentServer = context.getString("contentServer");
		String terminologyServer = context.getString("terminologyServer");
		String cqlEngineServer = context.getString("CQLEngineServer");

		RemoteCqlClient cqlClient = this.remoteCqlClient != null
			? this.remoteCqlClient
			: new RemoteCqlClient(FhirContext.forR5(), cqlEngineServer);
		try {
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

		} catch (Exception e) {
			// Log the error with context for debugging
			LOG.error("Error evaluating CQL expression for patient {} and study {}: {}", patientId, studyId, e.getMessage(), e);

			// Build an error card
			card.setIndicator(CdsServiceIndicatorEnum.CRITICAL);
			card.setSummary("Error evaluating eligibility");
			card.setDetail("An unexpected error occurred while evaluating eligibility.");
		}
		// Set source information
		CdsServiceResponseCardSourceJson source = new CdsServiceResponseCardSourceJson();
		source.setLabel("Research Eligibility CDS Service");
		source.setUrl("https://www.centreantoinelacassagne.org");
		card.setSource(source);

		response.addCard(card);
		return response;
	}

	@CdsServiceFeedback("research-eligibility-check")
	public CdsServiceFeedbackJson feedback(CdsServiceFeedbackJson theFeedback) {
		theFeedback.setAcceptedSuggestions(List.of(new CdsServiceAcceptedSuggestionJson().setId(UUID.randomUUID().toString())));
		return theFeedback;
	}
}
