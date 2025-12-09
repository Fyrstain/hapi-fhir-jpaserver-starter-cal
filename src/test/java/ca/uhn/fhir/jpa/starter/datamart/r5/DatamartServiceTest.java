package ca.uhn.fhir.jpa.starter.datamart.r5;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.Repositories;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartProcessor;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartService;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opencds.cqf.fhir.api.Repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DatamartServiceTest
 *
 * <p>Focuses on the service-level responsibilities:
 * <ul>
 *   <li>Locating the ResearchStudy</li>
 *   <li>Validating preconditions (phase must not be {@code initial})</li>
 *   <li>Assembling evaluation {@link Parameters} with endpoints</li>
 *   <li>Delegating to {@link DatamartProcessor}</li>
 *   <li>Persisting/Updating the {@link ListResource} and updating the {@link ResearchStudy}</li>
 * </ul>
 *
 * <p>Tests are written in the same style as the Cohorting tests: clear scenario names, helpers, and targeted assertions.</p>
 */
class DatamartServiceTest {

	private Repository repository;
	private DatamartEvaluationOptions settings;
	private DatamartService service;

	private static final String STUDY_URL = "http://example.org/study/RS-1";

	@BeforeEach
	void setUp() {
		repository = mock(Repository.class);
		settings = mock(DatamartEvaluationOptions.class);
		service = new DatamartService(repository, settings);
	}


	@Test
	void ResearchStudyNotFoundThrows404() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);

			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(new Bundle());

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.generateDatamart(
					canonical(STUDY_URL),
					endpoint("content"), endpoint("data"), endpoint("term"), endpoint("cql")
				)
			);
			assertTrue(ex.getMessage().contains(STUDY_URL));
		}
	}

	/** Test with OperationOutcome as only returned entry in Bundle search for ResearchStudy */
	@Test
	void ResearchStudySearchReturningOperationOutcomeThrowsUnprocessableEntity() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);

			OperationOutcome oo = new OperationOutcome();
			oo.addIssue().setDiagnostics("Search error");

			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundleWith(oo));

			assertThrows(UnprocessableEntityException.class, () ->
				service.generateDatamart(
					canonical(STUDY_URL),
					endpoint("content"), endpoint("data"), endpoint("term"), endpoint("cql")
				)
			);
		}
	}

	@Test
	void generateDatamartInitialPhaseThrowsPreconditionFailure() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, ResearchStudyUtils.INITIAL_PHASE);

		try (MockedStatic<Repositories> repo = Mockito.mockStatic(Repositories.class)) {
			repo.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);

			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundleWith(researchStudy));

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.generateDatamart(
					canonical(STUDY_URL),
					endpoint("content"), endpoint("data"), endpoint("term"), endpoint("cql")
				)
			);
			assertTrue(ex.getMessage().toLowerCase().contains("cohort"), "Precondition message should indicate cohorting is required");
		}
	}


	@Test
	void generateDatamartSuccessCreatesListAndUpdatesStudy() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, "post-cohort");

		ListResource listFromProcessor = new ListResource().setStatus(ListResource.ListStatus.CURRENT);

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			  MockedConstruction<RemoteCqlClient> cqlClientConstructed = Mockito.mockConstruction(RemoteCqlClient.class);
			  MockedConstruction<DatamartProcessor> datamartProcessorConstructed = Mockito.mockConstruction(
				  DatamartProcessor.class,
				  (mock, ctx) -> when(mock.generateDatamart(any(ResearchStudy.class), any(Parameters.class)))
					  .thenReturn(listFromProcessor)
			  )) {

			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);

			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundleWith(researchStudy));

			when(repository.create(any(ListResource.class)))
				.thenAnswer(inv -> {
					ListResource lr = inv.getArgument(0, ListResource.class);
					MethodOutcome mo = new MethodOutcome();
					mo.setId(new IdType("List", "list1"));
					return mo;
				});

			ListResource out = service.generateDatamart(
				canonical(STUDY_URL),
				endpoint("content"), endpoint("data"), endpoint("term"), endpoint("cql")
			);

			assertNotNull(out);
			assertEquals("list1", out.getIdElement().getIdPart(), "List must be created and returned with server-assigned id");

			Extension ext = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL);
			Extension eval = ext.getExtensionByUrl(ResearchStudyUtils.EVAL_EXT_NAME);
			assertEquals("List/list1", eval.getValueReference().getReference());

			Coding coding = researchStudy.getPhase().getCodingFirstRep();
			assertEquals(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM, coding.getSystem());
			assertEquals(ResearchStudyUtils.POST_DATAMART, coding.getCode());
		}
	}

	/** Test with Bundle containing OperationOutcome + ResearchStudy in the entries of the returned Bundle when searching for ResearchStudy */
	@Test
	void generateDatamartWithOperationOutcomeAndStudySucceeds() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, "post-cohort");
		OperationOutcome oo = new OperationOutcome();
		oo.addIssue().setDiagnostics("warning");

		ListResource listFromProcessor = new ListResource().setStatus(ListResource.ListStatus.CURRENT);

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			MockedConstruction<RemoteCqlClient> cqlClientConstructed = Mockito.mockConstruction(RemoteCqlClient.class);
			MockedConstruction<DatamartProcessor> datamartProcessorConstructed = Mockito.mockConstruction(
				DatamartProcessor.class,
				(mock, ctx) -> when(mock.generateDatamart(any(ResearchStudy.class), any(Parameters.class)))
					.thenReturn(listFromProcessor)
			)) {

			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);

			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundleWith(oo, researchStudy));

			when(repository.create(any(ListResource.class)))
				.thenAnswer(inv -> {
					ListResource lr = inv.getArgument(0, ListResource.class);
					MethodOutcome mo = new MethodOutcome();
					mo.setId(new IdType("List", "list1"));
					return mo;
				});

			ListResource out = service.generateDatamart(
				canonical(STUDY_URL),
				endpoint("content"), endpoint("data"), endpoint("term"), endpoint("cql")
			);

			assertNotNull(out);
			assertEquals("list1", out.getIdElement().getIdPart());
		}
	}

	@Test
	void updateResearchStudyWithExistingEvaluationUpdatesListOnly() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, "post-cohort");

		Extension extension = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL);
		extension.addExtension()
			.setUrl(ResearchStudyUtils.EVAL_EXT_NAME)
			.setValue(new Reference("List/listX"));

		ListResource list = new ListResource();
		ListResource updatedList = new ListResource();
		updatedList.setId("List/listX");

		when(repository.update(any(ListResource.class))).thenReturn(new MethodOutcome().setResource(updatedList));

		ListResource out = service.updateResearchStudyWithList(repository, researchStudy, list);

		assertEquals("listX", out.getIdElement().getIdPart());
		verify(repository).update(any(ListResource.class));
		verify(repository, never()).create(any(ListResource.class));
	}


	@Test
	void updateResearchStudyPhaseWithList() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, "post-cohort");

		ListResource list = new ListResource();

		when(repository.create(any(ListResource.class))).thenAnswer(inv -> {
			MethodOutcome mo = new MethodOutcome();
			mo.setId(new IdType("List", "list2"));
			return mo;
		});

		ListResource out = service.updateResearchStudyWithList(repository, researchStudy, list);

		assertEquals("list2", out.getIdElement().getIdPart());

		Extension eval = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).getExtensionByUrl(ResearchStudyUtils.EVAL_EXT_NAME);
		assertNotNull(eval);
		assertEquals("List/list2", eval.getValueReference().getReference());

		Coding coding = researchStudy.getPhase().getCodingFirstRep();
		assertEquals(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM, coding.getSystem());
		assertEquals(ResearchStudyUtils.POST_DATAMART, coding.getCode());

		verify(repository).create(any(ListResource.class));
		verify(repository).update(eq(researchStudy));
	}
	
	@Test
	void updateResearchStudyWithList() {
		ResearchStudy researchStudy = initiateStudyWithPhase(STUDY_URL, "post-cohort");

		ListResource list = new ListResource();
		ListResource created = new ListResource();
		created.setId("List/createdY");

		when(repository.create(any(ListResource.class)))
			.thenReturn(new MethodOutcome().setResource(created));

		ListResource updatedList = service.updateResearchStudyWithList(repository, researchStudy, list);

		assertSame(created, updatedList, "Should return the created resource when present in MethodOutcome");
		Extension eval = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).getExtensionByUrl(ResearchStudyUtils.EVAL_EXT_NAME);
		assertEquals("List/createdY", eval.getValueReference().getReference());
		verify(repository).update(eq(researchStudy));
	}

	private static ResearchStudy initiateStudyWithPhase(String url, String phaseCode) {
		ResearchStudy researchStudy = new ResearchStudy();
		researchStudy.setUrl(url);
		CodeableConcept phase = new CodeableConcept();
		phase.addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(phaseCode);
		researchStudy.setPhase(phase);
		researchStudy.addExtension(new Extension(ResearchStudyUtils.EXT_URL));
		return researchStudy;
	}

	private static Bundle bundleWith(Resource... resources) {
		Bundle bundle = new Bundle();
		for (Resource r : resources) {
			bundle.addEntry().setResource(r);
		}
		return bundle;
	}

	private static CanonicalType canonical(String url) {
		return new CanonicalType(url);
	}

	private static Endpoint endpoint(String id) {
		Endpoint ep = new Endpoint();
		ep.setId(id);
		return ep;
	}
}
