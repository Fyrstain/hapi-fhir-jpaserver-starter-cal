package ca.uhn.fhir.jpa.starter.cohort.r5;
/* 
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluation;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluationOptions;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteProcessor;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.RepositorySubjectProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.model.CompiledLibrary;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opencds.cqf.cql.engine.execution.CqlEngine;
import org.opencds.cqf.cql.engine.execution.Environment;
import org.opencds.cqf.cql.engine.execution.State;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.cql.Engines;
import org.opencds.cqf.fhir.cql.EvaluationSettings;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CohorteProcessorTest {

    private Repository repository;
    private CohorteEvaluationOptions options;
    private RepositorySubjectProvider subjectProvider;
    private CohorteProcessor processor;
    private CqlEngine engine;
    private Environment environment;
    private LibraryManager libraryManager;
    private State state;
    private CompiledLibrary compiledLibrary;

    @BeforeEach
    void setup() {
        repository = mock(Repository.class);
        options = mock(CohorteEvaluationOptions.class);
        subjectProvider = mock(RepositorySubjectProvider.class);
        processor = new CohorteProcessor(repository, options, subjectProvider);
        // CQL engine Config
        engine = mock(CqlEngine.class);
        environment = mock(Environment.class);
        libraryManager = mock(LibraryManager.class);
        state = mock(State.class);
        compiledLibrary = mock(CompiledLibrary.class);

        when(options.getEvaluationSettings()).thenReturn(EvaluationSettings.getDefault());
        when(engine.getEnvironment()).thenReturn(environment);
        when(engine.getState()).thenReturn(state);
        when(environment.getLibraryManager()).thenReturn(libraryManager);

        when(libraryManager.resolveLibrary(any())).thenReturn(compiledLibrary);
    }

    @Test
    void CohortingNormalFlow() {
        // Arrange
        String libraryUrl = "http://base-url/Library/Test123";
        ResearchStudy study = createResearchStudyWithValidEligibility("study1");
        EvidenceVariable evidenceVariable = createValidEvidenceVariable(libraryUrl);
        when(repository.read(eq(EvidenceVariable.class), any(IdType.class)))
                .thenReturn(evidenceVariable);
        when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
                .thenReturn(createLibraryBundle());
        when(subjectProvider.getSubjects(eq(repository), Collections.singletonList(any())))
                .thenReturn(java.util.stream.Stream.of("Patient/123"));

        try (MockedStatic<Engines> enginesMock = Mockito.mockStatic(Engines.class)) {
            enginesMock.when(() -> Engines.forRepository(eq(repository), any(), isNull()))
                    .thenReturn(engine);

            try (MockedConstruction<CohorteEvaluation> mockedEvaluation =
                         Mockito.mockConstruction(CohorteEvaluation.class, (mock, context) -> {
                             when(mock.evaluate(any(ResearchStudy.class), any(EvidenceVariable.class), anyList()))
                                     .thenReturn(new Group());
                         })) {

                // Act
                Group result = processor.cohorting(study);

                assertNotNull(result);
                assertEquals(1, mockedEvaluation.constructed().size());
            }
        }
    }

    @Test
    void testCohortingThrowsExceptionWhenNoEligibility() {
        // Arrange
        ResearchStudy study = createResearchStudyWithoutEligibility();

        // Act & Assert
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> processor.cohorting(study));
        assertTrue(exception.getMessage().contains("does not have an eligibility variable specified"));
    }

    @Test
    void testCohortingThrowsExceptionWhenEligibilityReferenceInvalid() {
        // Arrange
        ResearchStudy study = createResearchStudyWithInvalidEligibility();

        // Act & Assert
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> processor.cohorting(study));
        assertTrue(exception.getMessage().contains("does not have a valid eligibility reference"));
    }

    @Test
    void testCohortingThrowsExceptionWhenLibraryNotFound() {
        // Arrange
        String libraryUrl = "http://library-url";
        ResearchStudy study = createResearchStudyWithValidEligibility("study1");

        EvidenceVariable evidenceVariable = createValidEvidenceVariable(libraryUrl);
        when(repository.read(eq(EvidenceVariable.class), any(IdType.class))).thenReturn(evidenceVariable);

        Bundle emptyBundle = new Bundle();
        when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
                .thenReturn(emptyBundle);

        // Act & Assert
        Exception exception = assertThrows(ResourceNotFoundException.class,
                () -> processor.cohorting(study));
        assertTrue(exception.getMessage().contains("Unable to find Library with url: " + libraryUrl));
    }

    private ResearchStudy createResearchStudyWithoutEligibility() {
        ResearchStudy study = new ResearchStudy();
        study.setRecruitment(new ResearchStudy.ResearchStudyRecruitmentComponent());
        return study;
    }

    private ResearchStudy createResearchStudyWithInvalidEligibility() {
        ResearchStudy study = new ResearchStudy();
        ResearchStudy.ResearchStudyRecruitmentComponent recruitment = new ResearchStudy.ResearchStudyRecruitmentComponent();
        recruitment.setEligibility(new Reference("Patient/1"));
        study.setRecruitment(recruitment);
        return study;
    }

    private ResearchStudy createResearchStudyWithValidEligibility(String studyUrl) {
        ResearchStudy study = new ResearchStudy();
        study.setUrl(studyUrl);
        ResearchStudy.ResearchStudyRecruitmentComponent recruitment = new ResearchStudy.ResearchStudyRecruitmentComponent();
        recruitment.setEligibility(new Reference("EvidenceVariable/ev1"));
        study.setRecruitment(recruitment);
        return study;
    }

    private EvidenceVariable createValidEvidenceVariable(String libraryUrl) {
        EvidenceVariable ev = new EvidenceVariable();
        ev.setUrl("ev1");
        Extension ext = new Extension();
        ext.setUrl("http://hl7.org/fhir/StructureDefinition/cqf-library");
        ext.setValue(new CanonicalType(libraryUrl));
        ev.addExtension(ext);
        return ev;
    }

    private Bundle createLibraryBundle() {
        Bundle libraryBundle = new Bundle();
        Library library = new Library();
        library.setUrl("http://library-url");
        libraryBundle.addEntry(new Bundle.BundleEntryComponent().setResource(library));
        return libraryBundle;
    }
}
*/