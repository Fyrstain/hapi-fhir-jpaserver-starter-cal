package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteService;
import org.hl7.fhir.r5.model.Group;
import org.hl7.fhir.r5.model.ResearchStudy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opencds.cqf.fhir.api.Repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
}
