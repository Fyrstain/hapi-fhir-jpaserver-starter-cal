package ca.uhn.fhir.jpa.starter.datamart.r5;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.starter.datamart.service.Repositories;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartExportService;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.service.DatamartTransformation;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatamartExportServiceTest {

	@Mock
	private Repository repository;

	private DatamartExportService service;
	private CanonicalType studyUrl;
	private Endpoint studyEndpoint;
	private Endpoint mapEndpoint;
	private Endpoint termEndpoint;
	private Endpoint remoteEndpoint;
	private String type;
	private CanonicalType mapUrl;

	@BeforeEach
	void setUp() {
		service = new DatamartExportService(repository);
		studyUrl = new CanonicalType("http://example.com/study");
		studyEndpoint = new Endpoint();
		mapEndpoint = new Endpoint();
		termEndpoint = new Endpoint();
		remoteEndpoint = new Endpoint();
		remoteEndpoint.setAddress("http://remote/fhir");
		type = "text/csv";
		mapUrl = new CanonicalType("http://example.com/StructureMap/CSV");
	}

	@Test
	void exportDatamartResearchStudyNotFound() {
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(new Bundle());

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, type, mapUrl)
			);
			assertTrue(ex.getMessage().contains("Unable to find ResearchStudy with url: "));
		}
	}

	@Test
	void exportDatamartNotPostDatamartPhaseException() {
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(studyBundle);
			when(repository.search(eq(Bundle.class), eq(StructureMap.class), any(), isNull()))
				.thenReturn(new Bundle());

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, type, mapUrl)
			);
			assertTrue(ex.getMessage().contains("Unable to find StructureMap"));
		}
	}

	@Test
	void exportDatamartStructureMapNotFound_forCSV() {
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(studyBundle);
			when(repository.search(eq(Bundle.class), eq(StructureMap.class), any(), isNull()))
				.thenReturn(new Bundle());

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, type, mapUrl)
			);
			assertTrue(ex.getMessage().contains("Unable to find StructureMap"));
		}
	}

	@Test
	void exportDatamartCsvSuccess_usesTransformation() {
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));
		StructureMap sm = new StructureMap();
		Bundle smBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(sm));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull()))
				.thenReturn(studyBundle);
			when(repository.search(eq(Bundle.class), eq(StructureMap.class), any(), isNull()))
				.thenReturn(smBundle);

			Binary expected = new Binary();
			try (MockedConstruction<DatamartTransformation> cons = Mockito.mockConstruction(
				DatamartTransformation.class,
				(mockTrans, ctx) -> when(mockTrans.transform(eq(rs), eq(sm))).thenReturn(expected)
			)) {
				Binary result = service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, type, mapUrl);
				assertSame(expected, result);
				assertEquals(1, cons.constructed().size());
			}
		}
	}

	@Test
	void exportDatamartRestSuccess_transactionExecuted() {
		// Arrange a POST-DATAMART study with evaluation list
		String listId = "LIST-REST-1";
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Extension ext = new Extension(ResearchStudyUtils.EXT_URL);
		ext.addExtension(new Extension(ResearchStudyUtils.EVAL_EXT_NAME, new Reference("List/" + listId)));
		rs.addExtension(ext);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		// datamart bundle with one Parameters containing a Patient (so extractResources() is non-empty)
		Parameters p = new Parameters();
		p.addParameter().setName("x").addPart().setName("bundle").setResource(new Patient().setId("pat-1"));
		Bundle dmBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(p));

		// Mock repo proxy and searches
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			  MockedConstruction<DatamartTransformation> cons = Mockito.mockConstruction(
				  DatamartTransformation.class,
				  (mockTrans, ctx) -> when(mockTrans.fetchDataMartBundle(eq(listId))).thenReturn(dmBundle)
			  )) {

			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull())).thenReturn(studyBundle);

			// FhirContext spy to stub client creation but keep real parser
			FhirContext ctx = Mockito.spy(FhirContext.forR5());
			when(repository.fhirContext()).thenReturn(ctx);

			IGenericClient client = mock(IGenericClient.class, RETURNS_DEEP_STUBS);
			when(ctx.newRestfulGenericClient(anyString())).thenReturn(client);
			when(client.transaction().withBundle(any(Bundle.class)).execute()).thenReturn(new Bundle());

			// Act
			Binary result = service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, "rest", null);

			// Assert
			assertNotNull(result);
			assertEquals("application/fhir+json", result.getContentType());
			assertNotNull(result.getData());
			assertTrue(cons.constructed().size() >= 1);
		}
	}

	@Test
	void exportDatamartRest_noResources_throws() {
		String listId = "LIST-EMPTY";
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Extension ext = new Extension(ResearchStudyUtils.EXT_URL);
		ext.addExtension(new Extension(ResearchStudyUtils.EVAL_EXT_NAME, new Reference("List/" + listId)));
		rs.addExtension(ext);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		// datamart bundle with ONLY a List (no extractable resources)
		Bundle dmBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(new ListResource()));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			  MockedConstruction<DatamartTransformation> cons = Mockito.mockConstruction(
				  DatamartTransformation.class,
				  (mockTrans, ctx) -> when(mockTrans.fetchDataMartBundle(eq(listId))).thenReturn(dmBundle)
			  )) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull())).thenReturn(studyBundle);

			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, "rest", null)
			);
			assertTrue(ex.getMessage().toLowerCase().contains("resource"));
		}
	}

	@Test
	void exportDatamartBulkSuccess_writesManifest() {
		// Arrange: POST-DATAMART study with evaluation list
		String listId = "LIST-BULK-1";
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Extension ext = new Extension(ResearchStudyUtils.EXT_URL);
		ext.addExtension(new Extension(ResearchStudyUtils.EVAL_EXT_NAME, new Reference("List/" + listId)));
		rs.addExtension(ext);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));

		// datamart bundle with two resources (Patient + Observation) inside Parameters parts
		Parameters params = new Parameters();
		params.addParameter().addPart().setResource(new Patient().setId("p1"));
		params.getParameterFirstRep().addPart().setResource(new Observation().setId("o1"));
		Bundle dmBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(params));

		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class);
			  MockedConstruction<DatamartTransformation> cons = Mockito.mockConstruction(
				  DatamartTransformation.class,
				  (mockTrans, ctx) -> when(mockTrans.fetchDataMartBundle(eq(listId))).thenReturn(dmBundle)
			  )) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull())).thenReturn(studyBundle);

			// FhirContext for serialization used by BulkNdjsonWriter
			when(repository.fhirContext()).thenReturn(FhirContext.forR5());

			// repository.create(Binary) should return an outcome with an id each time
			when(repository.create(any(Binary.class), isNull())).thenAnswer(inv -> {
				Binary created = inv.getArgument(0);
				created.setId("Binary/" + System.nanoTime());
				ca.uhn.fhir.rest.api.MethodOutcome mo = new ca.uhn.fhir.rest.api.MethodOutcome();
				mo.setResource(created);
				return mo;
			});

			Binary result = service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, "bulk", null);

			assertNotNull(result);
			assertEquals("application/fhir+json", result.getContentType()); // manifest Parameters JSON
			assertTrue(result.getData() != null && result.getData().length > 0);
		}
	}

	@Test
	void exportDatamartCsv_missingMapUrl_throws() {
		ResearchStudy rs = new ResearchStudy().setUrl(studyUrl.getValue());
		rs.getPhase().addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode(ResearchStudyUtils.POST_DATAMART);
		Bundle studyBundle = new Bundle().addEntry(new Bundle.BundleEntryComponent().setResource(rs));
		try (MockedStatic<Repositories> reps = Mockito.mockStatic(Repositories.class)) {
			reps.when(() -> Repositories.proxy(any(Repository.class), anyBoolean(), any(Endpoint.class), any(Endpoint.class), any()))
				.thenReturn(repository);
			when(repository.search(eq(Bundle.class), eq(ResearchStudy.class), any(), isNull())).thenReturn(studyBundle);
			ResourceNotFoundException ex = assertThrows(
				ResourceNotFoundException.class,
				() -> service.exportDatamart(studyUrl, studyEndpoint, mapEndpoint, termEndpoint, remoteEndpoint, "text/csv", null)
			);
			assertTrue(ex.getMessage().contains("structureMapUrl is required"));
		}
	}
}
