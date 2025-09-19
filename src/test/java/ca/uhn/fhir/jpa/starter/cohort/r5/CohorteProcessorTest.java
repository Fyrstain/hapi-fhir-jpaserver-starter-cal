package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteProcessor;
import ca.uhn.fhir.jpa.starter.cohort.service.r5.RepositorySubjectProvider;
import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opencds.cqf.fhir.api.Repository;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CohorteProcessorTest {

	@Mock
	Repository repository;

	@Mock
	RemoteCqlClient cql;

	@Mock
	RepositorySubjectProvider subjectProvider;

	private CohorteProcessor processor;

	@BeforeEach
	void setup() {
		processor = new CohorteProcessor(repository, cql, subjectProvider);
	}

	private EvidenceVariable makeEvidenceVariableWithLibrary(String evId, String libraryCanonical, String exprName) {
		EvidenceVariable ev = new EvidenceVariable();
		ev.setId(evId);
		ev.addExtension()
			.setUrl("http://hl7.org/fhir/StructureDefinition/cqf-library")
			.setValue(new CanonicalType(libraryCanonical));
		EvidenceVariable.EvidenceVariableCharacteristicComponent c = ev.addCharacteristic();
		c.getDefinitionByCombination()
			.addCharacteristic(new EvidenceVariable.EvidenceVariableCharacteristicComponent()
				.setDefinitionExpression(new Expression().setExpression(exprName)));
		return ev;
	}

	@Test
	void Cohorting_CollectsOnlyEligibleSubjects() {
		ResearchStudy study = new ResearchStudy();
		study.setUrl("http://example.com/study");
		study.setId("study1");
		study.getRecruitment().setEligibility(new Reference("EvidenceVariable/ev1"));

		final String canonical = "http://example.com/Library/LibA|1.0.0";
		final String exprName = "InPopulation";
		EvidenceVariable ev = makeEvidenceVariableWithLibrary("ev1", canonical, exprName);

		when(repository.read(eq(EvidenceVariable.class), any(IdType.class))).thenReturn(ev);

		Bundle libBundle = new Bundle();
		libBundle.addEntry().setResource(new Library().setId("LibA"));
		when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
			.thenReturn(libBundle);

		when(subjectProvider.getSubjects(any(Repository.class), isNull(List.class)))
			.thenReturn(Stream.of("Patient/123", "Patient/456"));


		when(repository.read(eq(Patient.class), any(IdType.class)))
			.thenAnswer(inv -> {
				IdType id = inv.getArgument(1);
				String idPart = id.getIdPart() != null ? id.getIdPart() : "unknown";
				Patient p = new Patient();
				p.setId(id.getValue());
				p.addIdentifier().setSystem("urn:oid:0.1.2.3.4.5.6.7").setValue(idPart);
				return p;
			});

		when(cql.evaluateLibrary(any(Parameters.class), eq("LibA")))
			.thenAnswer(inv -> {
				Parameters in = inv.getArgument(0);
				String subject = in.getParameter("subject").getValue().primitiveValue();
				boolean inPop = subject != null && subject.endsWith("/123");
				Parameters out = new Parameters();
				out.addParameter().setName(exprName).setValue(new BooleanType(inPop));
				out.addParameter().setName("evaluate boolean").setValue(new BooleanType(inPop));
				return out;
			});

		Group result = processor.cohorting(study, new Parameters());

		assertNotNull(result);
		assertEquals(1, result.getMember().size(), "Only one subject should match the inclusion criteria");
	}
}
