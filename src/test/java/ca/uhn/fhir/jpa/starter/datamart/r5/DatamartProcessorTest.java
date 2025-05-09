package ca.uhn.fhir.jpa.starter.datamart.r5;
/*
import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.DatamartGeneration;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.DatamartProcessor;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.ResearchStudyUtils;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.cqframework.cql.cql2elm.CqlIncludeException;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.model.CompiledLibrary;
import org.hl7.elm.r1.VersionedIdentifier;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opencds.cqf.cql.engine.execution.CqlEngine;
import org.opencds.cqf.cql.engine.execution.Environment;
import org.opencds.cqf.cql.engine.execution.State;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.cql.Engines;
import org.opencds.cqf.fhir.cql.EvaluationSettings;
import org.opencds.cqf.fhir.cql.VersionedIdentifiers;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatamartProcessorTest {

    @Mock
    private Repository repository;
    @Mock
    private DatamartEvaluationOptions settings;

    private DatamartProcessor processor;
    private ResearchStudy study;
    private Group group;
    private EvidenceVariable variable;
    private String libraryUrl;
    private List<String> subjects;

    private CqlEngine engine;
    private Environment environment;
    private LibraryManager libraryManager;
    private State state;
    private CompiledLibrary compiledLibrary;

    @BeforeEach
    void setUp() throws Exception {
        processor = new DatamartProcessor(repository, settings);
        study = new ResearchStudy().setUrl("http://example.org/study");

        group = new Group();
        group.setId("grp1");

        variable = new EvidenceVariable();
        variable.setId("var1");
        libraryUrl = "http://example.org/Library/lib1";
        variable.addExtension(new Extension(
                "http://hl7.org/fhir/StructureDefinition/cqf-library",
                new CanonicalType(libraryUrl)
        ));

        subjects = List.of("Patient/p1", "Patient/p2");

        //CQl engine preparation
        engine = mock(CqlEngine.class);
        environment = mock(Environment.class);
        libraryManager = mock(LibraryManager.class);
        state = mock(State.class);
        compiledLibrary = mock(CompiledLibrary.class);
    }

    @Test
    void generateDatamartSuccessfulFlow() throws Exception {
        when(settings.getEvaluationSettings()).thenReturn(EvaluationSettings.getDefault());

        when(engine.getEnvironment()).thenReturn(environment);
        when(engine.getState()).thenReturn(state);
        when(environment.getLibraryManager()).thenReturn(libraryManager);
        when(libraryManager.resolveLibrary(any(VersionedIdentifier.class))).thenReturn(compiledLibrary);
        when(compiledLibrary.getLibrary()).thenReturn(mock(org.hl7.elm.r1.Library.class));

        try (MockedStatic<ResearchStudyUtils> utils = Mockito.mockStatic(ResearchStudyUtils.class)) {
            utils.when(() -> ResearchStudyUtils.getEligibleGroup(study, repository)).thenReturn(group);
            utils.when(() -> ResearchStudyUtils.getEvidenceVariable(study, repository)).thenReturn(variable);
            utils.when(() -> ResearchStudyUtils.getSubjectReferences(group, repository)).thenReturn(subjects);

            Bundle bundle = new Bundle();
            bundle.addEntry(new Bundle.BundleEntryComponent()
                    .setResource(new Library().setUrl(libraryUrl)));
            when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
                    .thenReturn(bundle);

            try (MockedStatic<Engines> enginesMock = Mockito.mockStatic(Engines.class)) {
                enginesMock.when(() -> Engines.forRepository(eq(repository), any(), isNull()))
                        .thenReturn(engine);

                VersionedIdentifier vid = VersionedIdentifiers.forUrl(libraryUrl);
                try (MockedConstruction<DatamartGeneration> gen = Mockito.mockConstruction(DatamartGeneration.class,
                        (mockGen, context) -> {
                            when(mockGen.generateDatamart(study, variable, subjects, vid))
                                    .thenReturn(new ListResource());
                        })) {

                    ListResource result = processor.generateDatamart(study);
                    assertNotNull(result);
                    assertEquals(1, gen.constructed().size());
                }
            }
        }
    }

    @Test
    void generateDatamartNoLibraryException() {
        variable.getExtensionByUrl("http://hl7.org/fhir/StructureDefinition/cqf-library")
                .setValue(new CanonicalType((String) null));

        try (MockedStatic<ResearchStudyUtils> utils = Mockito.mockStatic(ResearchStudyUtils.class)) {
            utils.when(() -> ResearchStudyUtils.getEligibleGroup(study, repository)).thenReturn(group);
            utils.when(() -> ResearchStudyUtils.getEvidenceVariable(study, repository)).thenReturn(variable);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> processor.generateDatamart(study));
            assertTrue(ex.getMessage().contains("does not have a valid library reference"));
        }
    }

    @Test
    void generateDatamartLibraryNotFoundException() {
        try (MockedStatic<ResearchStudyUtils> utils = Mockito.mockStatic(ResearchStudyUtils.class)) {
            utils.when(() -> ResearchStudyUtils.getEligibleGroup(study, repository)).thenReturn(group);
            utils.when(() -> ResearchStudyUtils.getEvidenceVariable(study, repository)).thenReturn(variable);

            when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
                    .thenReturn(new Bundle());

            ResourceNotFoundException ex = assertThrows(ResourceNotFoundException.class,
                    () -> processor.generateDatamart(study));
            assertTrue(ex.getMessage().contains("Unable to find Library with url: " + libraryUrl));
        }
    }

    @Test
    void generateDatamartCqlIncludeException() throws Exception {
        when(settings.getEvaluationSettings()).thenReturn(EvaluationSettings.getDefault());

        when(engine.getEnvironment()).thenReturn(environment);
        when(environment.getLibraryManager()).thenReturn(libraryManager);
        try (MockedStatic<ResearchStudyUtils> utils = Mockito.mockStatic(ResearchStudyUtils.class)) {
            utils.when(() -> ResearchStudyUtils.getEligibleGroup(study, repository)).thenReturn(group);
            utils.when(() -> ResearchStudyUtils.getEvidenceVariable(study, repository)).thenReturn(variable);

            Bundle bundle = new Bundle();
            bundle.addEntry(new Bundle.BundleEntryComponent()
                    .setResource(new Library().setUrl(libraryUrl)));
            when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
                    .thenReturn(bundle);

            try (MockedStatic<Engines> enginesMock = Mockito.mockStatic(Engines.class)) {
                enginesMock.when(() -> Engines.forRepository(eq(repository), any(), isNull()))
                        .thenReturn(engine);

                VersionedIdentifier vid = VersionedIdentifiers.forUrl(libraryUrl);
                when(libraryManager.resolveLibrary(eq(vid)))
                        .thenThrow(new CqlIncludeException("error", "system", "lib", "1.0.0"));

                IllegalStateException ex = assertThrows(IllegalStateException.class,
                        () -> processor.generateDatamart(study));
                System.out.print(ex.getMessage());
                assertTrue(ex.getMessage().contains("Unable to load CQL/ELM for library: " + vid.getId()));
                assertInstanceOf(CqlIncludeException.class, ex.getCause());
            }
        }
    }
}
*/