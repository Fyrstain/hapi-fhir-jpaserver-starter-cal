package ca.uhn.fhir.jpa.starter;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.jpa.starter.cdshooks.study.r5.StudyEligibilityCheckService;
import ca.uhn.fhir.rest.api.server.cdshooks.CdsServiceRequestContextJson;
import ca.uhn.fhir.rest.api.server.cdshooks.CdsServiceRequestJson;
import ca.uhn.hapi.fhir.cdshooks.api.json.*;
import org.hl7.fhir.r5.model.Bundle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StudyEligibilityCheckServiceR5Test {

	@Mock
	private RemoteCqlClient mockCqlClient;

	@InjectMocks
	private StudyEligibilityCheckService service;

	private CdsServiceRequestJson request;
	private Bundle testBundle;

	@BeforeEach
	void setUp() {
		testBundle = new Bundle();
		request = new CdsServiceRequestJson();

		CdsServiceRequestContextJson context = new CdsServiceRequestContextJson();
		context.put("patientId", "Patient/123");
		context.put("libraryId", "Library/Study1");
		context.put("studyId", "STUDY-001");
		context.put("inclusionExpression", "InclusionCriteria");
		context.put("contentServer", "http://content-server");
		context.put("terminologyServer", "http://terminology-server");
		context.put("CQLEngineServer", "http://cql-engine");

		request.setContext(context);
		request.setFhirServer("http://fhir-server");
		request.addPrefetch("patientData", testBundle); // <-- doit correspondre au nom utilisé dans le service
	}

	@Test
	void testEligiblePatientWithPrefetch() throws Exception {
		when(mockCqlClient.evaluateInclusionExpression(
			any(Bundle.class),
			anyString(),
			anyString(),
			anyString(),
			anyString(),
			anyString())
		).thenReturn(true);

		CdsServiceResponseJson response = service.exampleService(request);

		assertCardProperties(
			response,
			CdsServiceIndicatorEnum.INFO,
			"Patient eligible for clinical study STUDY-001"
		);
	}

	@Test
	void testNotEligiblePatientWithPrefetch() throws Exception {
		when(mockCqlClient.evaluateInclusionExpression(
			any(Bundle.class),
			anyString(),
			anyString(),
			anyString(),
			anyString(),
			anyString())
		).thenReturn(false);

		CdsServiceResponseJson response = service.exampleService(request);

		assertCardProperties(
			response,
			CdsServiceIndicatorEnum.WARNING,
			"Patient not eligible for study STUDY-001"
		);
	}

	@Test
	void testEvaluationError() throws Exception {
		when(mockCqlClient.evaluateInclusionExpression(
			any(Bundle.class),
			anyString(),
			anyString(),
			anyString(),
			anyString(),
			anyString())
		).thenThrow(new RuntimeException("CQL Engine error"));

		CdsServiceResponseJson response = service.exampleService(request);

		assertCardProperties(
			response,
			CdsServiceIndicatorEnum.CRITICAL,
			"Error evaluating eligibility"
		);
	}

	@Test
	void testWithoutPrefetch() throws Exception {
		request.addPrefetch("patientData", null);

		when(mockCqlClient.evaluateInclusionExpression(
			anyString(),
			anyString(),
			anyString(),
			anyString(),
			anyString(),
			anyString())
		).thenReturn(true);

		CdsServiceResponseJson response = service.exampleService(request);

		assertCardProperties(
			response,
			CdsServiceIndicatorEnum.INFO,
			"Patient eligible for clinical study STUDY-001"
		);
	}

	@Test
	void testFeedbackHandling() {
		CdsServiceFeedbackJson feedback = new CdsServiceFeedbackJson();
		CdsServiceFeedbackJson result = service.feedback(feedback);

		assertNotNull(result.getAcceptedSuggestions());
		assertEquals(1, result.getAcceptedSuggestions().size());
		assertNotNull(result.getAcceptedSuggestions().get(0).getId());
	}

	private void assertCardProperties(
		CdsServiceResponseJson response,
		CdsServiceIndicatorEnum expectedIndicator,
		String expectedSummary
	) {
		assertEquals(1, response.getCards().size());
		CdsServiceResponseCardJson card = response.getCards().get(0);

		assertEquals(expectedIndicator, card.getIndicator());
		assertEquals(expectedSummary, card.getSummary());
		assertNotNull(card.getDetail());

		CdsServiceResponseCardSourceJson source = card.getSource();
		assertEquals("Research Eligibility CDS Service", source.getLabel());
		assertEquals("https://www.isis.com", source.getUrl());
	}
}