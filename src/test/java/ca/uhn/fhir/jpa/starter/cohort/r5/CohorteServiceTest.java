package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteProcessor;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteService;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.Repositories;
import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CohorteServiceTest {

	private Repository repo;
	private CohorteService service;

	@BeforeEach
	void setup() {
		repo = Mockito.mock(Repository.class);
		service = new CohorteService(repo, new CohorteEvaluationOptions());
	}

	@Test
	void buildAndSaveGroup_setsFields_andSaves() {
		ResearchStudy study = new ResearchStudy();
		study.setId("study1");
		study.setName("MyStudy");
		study.setDescription("desc");

		Group group = new Group();
		service.buildAndSaveGroup(repo, group, study);

		assertEquals("group-study1", group.getIdElement().getIdPart());
		assertTrue(group.getActive());
		assertEquals(Group.GroupType.PERSON, group.getType());
		assertEquals("desc", group.getDescription());
		assertEquals("Patient Eligible for: MyStudy", group.getName());

		verify(repo, times(1)).update(group);
	}

	@Test
	void cohortingResearchStudyNotFoundThrows404() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repo);

			when(repo.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(new Bundle());

			CanonicalType studyUrl = new CanonicalType("http://example.org/study/RS-1");
			Endpoint contentEp = (Endpoint) new Endpoint().setId("content");

			assertThrows(ResourceNotFoundException.class, () ->
				service.cohorting(studyUrl, contentEp, null, null, null)
			);
		}
	}

	@Test
	void cohortingOperationOutcomeOnlyThrowsUnprocessableEntity() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repo);

			OperationOutcome oo = new OperationOutcome();
			oo.addIssue().setDiagnostics("search failed");

			Bundle b = new Bundle();
			b.addEntry().setResource(oo);

			when(repo.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(b);

			CanonicalType studyUrl = new CanonicalType("http://example.org/study/RS-1");
			Endpoint contentEp = (Endpoint) new Endpoint().setId("content");

			assertThrows(UnprocessableEntityException.class, () ->
				service.cohorting(studyUrl, contentEp, null, null, null)
			);
		}
	}

	@Test
	void cohortingWithOperationOutcomeAndStudySucceeds() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			MockedConstruction<RemoteCqlClient> cqlClientConstructed = Mockito.mockConstruction(RemoteCqlClient.class);
			MockedConstruction<CohorteProcessor> cohorteProcessorConstructed = Mockito.mockConstruction(
				CohorteProcessor.class,
				(mock, ctx) -> when(mock.cohorting(any(ResearchStudy.class), any(Parameters.class)))
					.thenReturn(new Group())
			)) {

			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), (IBaseResource) any(), any(), any()))
				.thenReturn(repo);

			ResearchStudy study = new ResearchStudy();
			study.setId("study1");
			study.setName("MyStudy");

			OperationOutcome oo = new OperationOutcome();
			oo.addIssue().setDiagnostics("warning");

			Bundle b = new Bundle();
			b.addEntry().setResource(oo);
			b.addEntry().setResource(study);

			when(repo.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(b);

			CanonicalType studyUrl = new CanonicalType("http://example.org/study/RS-1");
			Endpoint contentEp = (Endpoint) new Endpoint().setId("content");

			Group out = service.cohorting(studyUrl, contentEp, null, null, null);

			assertNotNull(out);
			// verify that buildAndSaveGroup and updateResearchStudyWithGroup called repo.update:
			verify(repo, atLeastOnce()).update(any());
		}
	}
}
