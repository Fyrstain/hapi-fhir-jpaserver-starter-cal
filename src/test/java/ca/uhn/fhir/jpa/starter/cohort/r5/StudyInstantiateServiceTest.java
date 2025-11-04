package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.Repositories;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.StudyInstantiateServiceImpl;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opencds.cqf.fhir.api.Repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link StudyInstantiateServiceImpl}.
 */
@ExtendWith(MockitoExtension.class)
class StudyInstantiateServiceTest {

	private static final String STUDY_CANONICAL = "http://example.org/study/RS-1";
	private static final String EV_A_ID = "EvidenceVariable/EV-A";
	private static final String EV_B_ID = "EvidenceVariable/EV-B";
	private static final String EV_A_CANONICAL = "http://example.org/ev/EV-A";
	private static final String EV_B_CANONICAL = "http://example.org/ev/EV-B";
	private static final String ENDPOINT_URL = "http://example.org/fhir";

	private Repository baseRepository;
	private Repository researchRepository;
	private FhirContext fhirContext;
	private IGenericClient client;
	private StudyInstantiateServiceImpl service;

	@BeforeEach
	void setUp() {
		baseRepository = mock(Repository.class);
		researchRepository = mock(Repository.class);
		fhirContext = mock(FhirContext.class, RETURNS_DEEP_STUBS);
		client = mock(IGenericClient.class, RETURNS_DEEP_STUBS);
		service = new StudyInstantiateServiceImpl(baseRepository);
	}

	@Test
	void instantiateStudyBuildsTransactionAndReturnsParameters() {
		try (MockedStatic<Repositories> repos = mockStatic(Repositories.class)) {
			// Arrange
			setupRepositoryProxy(repos);

			ResearchStudy studyDefinition = createResearchStudy(STUDY_CANONICAL, EV_A_ID);
			EvidenceVariable evA = createEvidenceVariable(EV_A_ID, EV_A_CANONICAL, EV_B_CANONICAL);
			EvidenceVariable evB = createEvidenceVariable(EV_B_ID, EV_B_CANONICAL, null);

			mockStudySearch(studyDefinition);
			mockEvidenceVariableRead(EV_A_ID, evA);
			mockEvidenceVariableSearch(evB);
			setupFhirClientMocks(new Bundle());

			// Act
			Parameters result = service.instantiateStudy(
				new CanonicalType(STUDY_CANONICAL),
				createEndpoint());

			// Assert
			assertParametersValid(result);
			verify(client.transaction()).withBundle(any(Bundle.class));
		}
	}

	@Test
	void instantiateStudyInvalidParametersThrows() {
		Repository baseRepo = mock(Repository.class);
		StudyInstantiateServiceImpl studyService = new StudyInstantiateServiceImpl(baseRepo);

		CanonicalType validCanonical = new CanonicalType(STUDY_CANONICAL);
		CanonicalType emptyCanonical = new CanonicalType("");
		Endpoint validEndpoint = createEndpoint();
		Endpoint emptyEndpoint = new Endpoint().setAddress("");

		assertAll(
			() -> assertThrows(InvalidRequestException.class, () -> studyService.instantiateStudy(null, validEndpoint)),
			() -> assertThrows(InvalidRequestException.class, () -> studyService.instantiateStudy(validCanonical, null)),
			() -> assertThrows(InvalidRequestException.class, () -> studyService.instantiateStudy(emptyCanonical, validEndpoint)),
			() -> assertThrows(InvalidRequestException.class, () -> studyService.instantiateStudy(validCanonical, emptyEndpoint))
		);
	}

	@Test
	void instantiateStudyStudyNotFoundThrows() {
		try (MockedStatic<Repositories> repos = mockStatic(Repositories.class)) {
			// Arrange
			CanonicalType studyCanonical = new CanonicalType(STUDY_CANONICAL);
			Endpoint studyEndpoint = createEndpoint();

			setupRepositoryProxy(repos);
			when(researchRepository.search(
				eq(Bundle.class),
				eq(ResearchStudy.class),
				any(),
				isNull()))
				.thenReturn(new Bundle());

			// Act & Assert
			assertThrows(ResourceNotFoundException.class,
				() -> service.instantiateStudy(
					studyCanonical,
					studyEndpoint));
		}
	}

	@Test
	void instantiateStudyTransactionError() {
		try (MockedStatic<Repositories> repos = mockStatic(Repositories.class)) {
			// Arrange
			setupRepositoryProxy(repos);

			ResearchStudy studyDefinition = createResearchStudy(STUDY_CANONICAL, null);
			mockStudySearch(studyDefinition);

			CanonicalType studyCanonical = new CanonicalType(STUDY_CANONICAL);
			Endpoint studyEndpoint = createEndpoint();

			when(baseRepository.fhirContext()).thenReturn(fhirContext);
			when(fhirContext.newRestfulGenericClient(anyString())).thenReturn(client);
			when(client.transaction().withBundle(any(Bundle.class)).execute())
				.thenThrow(new RuntimeException("boom"));

			// Act & Assert
			assertThrows(UnprocessableEntityException.class,
				() -> service.instantiateStudy(
					studyCanonical,
					studyEndpoint));
		}
	}

	// ==================== Helper Methods ====================

	private Endpoint createEndpoint() {
		return new Endpoint().setAddress(ENDPOINT_URL);
	}

	private ResearchStudy createResearchStudy(String canonical, String eligibilityRef) {
		ResearchStudy study = new ResearchStudy();
		study.setUrl(canonical);
		if (eligibilityRef != null) {
			study.setRecruitment(new ResearchStudy.ResearchStudyRecruitmentComponent()
				.setEligibility(new Reference(eligibilityRef)));
		}
		return study;
	}

	private EvidenceVariable createEvidenceVariable(String id, String canonical, String definitionCanonical) {
		EvidenceVariable ev = new EvidenceVariable();
		ev.setId(id);
		ev.setUrl(canonical);

		if (definitionCanonical != null) {
			EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic =
				new EvidenceVariable.EvidenceVariableCharacteristicComponent();
			characteristic.setDefinitionCanonical(definitionCanonical);
			ev.addCharacteristic(characteristic);
		}

		return ev;
	}

	private Bundle createBundleWithResource(Resource resource) {
		Bundle bundle = new Bundle();
		bundle.addEntry().setResource(resource);
		return bundle;
	}

	private void setupRepositoryProxy(MockedStatic<Repositories> repos) {
		repos.when(() -> Repositories.proxy(
				eq(baseRepository),
				eq(false),
				isNull(),
				any(Endpoint.class),
				isNull()))
			.thenReturn(researchRepository);
	}

	private void mockStudySearch(ResearchStudy study) {
		when(researchRepository.search(
			eq(Bundle.class),
			eq(ResearchStudy.class),
			any(),
			isNull()))
			.thenReturn(createBundleWithResource(study));
	}

	private void mockEvidenceVariableRead(String id, EvidenceVariable ev) {
		when(researchRepository.read(
			eq(EvidenceVariable.class),
			argThat(idArg -> id.equals(idArg.toUnqualifiedVersionless().getValue()))))
			.thenReturn(ev);
	}

	private void mockEvidenceVariableSearch(EvidenceVariable... evidenceVariables) {
		Bundle bundle = new Bundle();
		for (EvidenceVariable ev : evidenceVariables) {
			bundle.addEntry().setResource(ev);
		}
		when(researchRepository.search(
			eq(Bundle.class),
			eq(EvidenceVariable.class),
			any(),
			isNull()))
			.thenReturn(bundle);
	}

	private void setupFhirClientMocks(Bundle transactionResponse) {
		when(baseRepository.fhirContext()).thenReturn(fhirContext);
		when(fhirContext.newRestfulGenericClient(anyString())).thenReturn(client);
		when(client.transaction().withBundle(any(Bundle.class)).execute())
			.thenReturn(transactionResponse);
	}

	private void assertParametersValid(Parameters params) {
		assertEquals("studyInstanceUrl", params.getParameter().get(0).getName());
		assertNotNull(params.getParameter().get(0).getValue());
		assertEquals("transactionBundle", params.getParameter().get(1).getName());
		assertTrue(params.getParameter().get(1).getResource() instanceof Bundle);
	}

}
