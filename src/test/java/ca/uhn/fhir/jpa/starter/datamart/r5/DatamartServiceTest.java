package ca.uhn.fhir.jpa.starter.datamart.r5;
/* 
import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.Repositories;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.*;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opencds.cqf.fhir.api.Repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatamartServiceTest {

	private static final String STUDY_URL = "http://example.org/studyUrl";

	@Mock
	private Repository repository;
	@Mock
	private DatamartEvaluationOptions settings;

	private DatamartService service;
	private CanonicalType studyUrl;
	private Endpoint rsEndpoint;
	private Endpoint dataEndpoint;
	private Endpoint termEndpoint;

	@BeforeEach
	void setUp() {
		service = new DatamartService(repository, settings);
		studyUrl = new CanonicalType(STUDY_URL);
		rsEndpoint = new Endpoint();
		dataEndpoint = new Endpoint();
		termEndpoint = new Endpoint();
	}

	@Test
	void generateDatamartStudyNotFoundException() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(eq(repository), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(new Bundle());

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.generateDatamart(studyUrl, rsEndpoint, dataEndpoint, termEndpoint)
			);
			assertTrue(ex.getMessage().contains("Unable to find ResearchStudy with url: " + STUDY_URL));
		}
	}

	@Test
	void generateDatamartInitialPhaseException() {
		ResearchStudy rs = new ResearchStudy();
		rs.setUrl(STUDY_URL);
		rs.getPhase().addCoding()
			.setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM)
			.setCode(ResearchStudyUtils.INITIAL_PHASE);
		Bundle bundle = new Bundle();
		bundle.addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundle);

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.generateDatamart(studyUrl, rsEndpoint, dataEndpoint, termEndpoint)
			);
			assertTrue(ex.getMessage().contains("cohorting is needed before generating the datamart"));
		}
	}

	// TODO: update the test to align with the new modification of using Identifier instead of reference
	@Test
	void generateDatamartSuccessfulFlow() {
		ResearchStudy rs = new ResearchStudy();
		rs.setUrl(STUDY_URL);
		rs.getPhase().addCoding()
			.setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM)
			.setCode("post-cohorting");
		Bundle bundle = new Bundle();
		bundle.addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		rs.addExtension(new Extension(ResearchStudyUtils.EXT_URL));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(eq(repository), anyBoolean(), eq(dataEndpoint), eq(rsEndpoint), eq(termEndpoint)))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(bundle);

			try (MockedConstruction<DatamartProcessor> procCons = Mockito.mockConstruction(DatamartProcessor.class, (mockProc, ctx) -> {
				when(mockProc.generateDatamart(eq(rs))).thenReturn(new ListResource());
			})) {
				MethodOutcome outcome = new MethodOutcome();
				outcome.setId(new IdType("ListResource", "list1"));
				when(repository.create(any(ListResource.class))).thenReturn(outcome);

				MethodOutcome updateOutcome = new MethodOutcome();
				updateOutcome.setId(outcome.getId());
				when(repository.update(eq(rs))).thenReturn(updateOutcome);

				ListResource result = service.generateDatamart(studyUrl, rsEndpoint, dataEndpoint, termEndpoint);

				assertNotNull(result);
				Coding coding = rs.getPhase().getCoding().get(0);
				assertEquals(ResearchStudyUtils.POST_DATAMART, coding.getCode());
				Extension ext = rs.getExtensionByUrl(ResearchStudyUtils.EXT_URL)
					.getExtension().stream()
					.filter(e -> "evaluation".equals(e.getUrl()))
					.findFirst().orElse(null);
				assertNotNull(ext);
				assertEquals("ListResource/list1", ((Reference) ext.getValue()).getReference());

				verify(repository).update(eq(rs));
			}
		}
	}
}
*/