package ca.uhn.fhir.jpa.starter.cohort.r5;
/* 
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteProcessor;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteService;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.Repositories;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CohorteServiceTest {

    private static final String STUDY_URL = "http://study-url";
    private static final String EVIDENCE_VARIABLE_REF = "EvidenceVariable/ev1";

    private Repository repository;
    private CohorteEvaluationOptions options;

    @BeforeEach
    void setup() {
        repository = mock(Repository.class);
        options = mock(CohorteEvaluationOptions.class);
    }

    @Test
    void CohortingThrowsExceptionWhenResearchStudyNotFound() {
        when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
                .thenReturn(new Bundle());

        try (MockedStatic<Repositories> repositoriesStaticMock = Mockito.mockStatic(Repositories.class)) {
            repositoriesStaticMock.when(() ->
                            Repositories.proxy(eq(repository), anyBoolean(), (IBaseResource) any(), any(), any()))
                    .thenReturn(repository);

            CohorteService service = new CohorteService(repository, options);
            ResourceNotFoundException exception = assertThrows(
                    ResourceNotFoundException.class,
                    () -> service.cohorting(new CanonicalType(STUDY_URL), new Endpoint(), new Endpoint(), new Endpoint())
            );
            assertTrue(exception.getMessage().contains("Unable to find ResearchStudy with url: " + STUDY_URL));
        }
    }

    @Test
    void CohortingNormalFlow() {

        Bundle study = createStudyBundle();
        ResearchStudy researchStudy = (ResearchStudy) study.getEntry().get(0).getResource();
        when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
                .thenReturn(study);

        try (MockedStatic<Repositories> repositoriesStaticMock = Mockito.mockStatic(Repositories.class)) {
            repositoriesStaticMock.when(() ->
                            Repositories.proxy(eq(repository), anyBoolean(), (IBaseResource) any(), any(), any()))
                    .thenReturn(repository);

            try (MockedConstruction<CohorteProcessor> mockedConstruction =
                         Mockito.mockConstruction(CohorteProcessor.class, (mock, context) -> {
                             when(mock.cohorting(any(ResearchStudy.class))).thenReturn(new Group());
                         })) {

                // Act
                CohorteService service = new CohorteService(repository, options);

                Group result = service.cohorting(new CanonicalType(STUDY_URL),
                        new Endpoint(), new Endpoint(), new Endpoint());

                // Assert
                assertNotNull(result);
                assertEquals("group-" + researchStudy.getIdElement().getIdPart(), result.getId());
                assertNotNull(researchStudy.getRecruitment().getActualGroup());

            }
        }
    }

    private Bundle createStudyBundle() {
        ResearchStudy study = new ResearchStudy();
        study.setUrl(STUDY_URL);
        study.setId("study1");
        study.setName("TestStudy");
        study.setDescription("A study for testing.");
        ResearchStudy.ResearchStudyRecruitmentComponent recruitment = new ResearchStudy.ResearchStudyRecruitmentComponent();
        recruitment.setEligibility(new Reference(EVIDENCE_VARIABLE_REF));
        study.setRecruitment(recruitment);
        Bundle bundle = new Bundle();
        bundle.addEntry().setResource(study);
        return bundle;
    }
}
*/