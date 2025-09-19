package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.CohorteEvaluation;
import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opencds.cqf.fhir.api.Repository;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CohorteEvaluationTest {

	@Mock
	RemoteCqlClient cql;

	@Mock
	Repository repository;

	private CohorteEvaluation evaluation;

	private ResearchStudy study;

	private static EvidenceVariable initiateEvidenceVariable() {
		EvidenceVariable ev = new EvidenceVariable();
		ev.setUrl("http://example.org/ev/" + System.nanoTime());
		return ev;
	}

	private static EvidenceVariable.EvidenceVariableCharacteristicComponent expressionDef(String exprName) {
		return new EvidenceVariable.EvidenceVariableCharacteristicComponent()
			.setDefinitionExpression(new Expression().setExpression(exprName));
	}

	private static void addLibraryExtension(EvidenceVariable ev, String canonical) {
		ev.addExtension("http://hl7.org/fhir/StructureDefinition/cqf-library",
			new CanonicalType(canonical));
	}

	private static Bundle bundleWith(Resource r) {
		Bundle b = new Bundle();
		b.addEntry().setResource(r);
		return b;
	}

	private static Bundle bundleWithLibraryId(String id) {
		Library lib = new Library();
		lib.setId(id);
		return bundleWith(lib);
	}

	private static Parameters outBoolean(String name, boolean value) {
		Parameters p = new Parameters();
		// Return both the expression-specific param and the canonical "evaluate boolean"
		p.addParameter().setName(name).setValue(new BooleanType(value));
		p.addParameter().setName("evaluate boolean").setValue(new BooleanType(value));
		return p;
	}

	private static Parameters outString(String name, String value) {
		Parameters p = new Parameters();
		p.addParameter().setName(name).setValue(new StringType(value));
		p.addParameter().setName("evaluate boolean").setValue(new StringType(value));
		return p;
	}

	@BeforeEach
	void setUp() {
		evaluation = new CohorteEvaluation(cql, repository);
		study = new ResearchStudy();
		study.setId("study1");
		study.setName("MyStudy");
		study.setDescription("desc");

		lenient().when(repository.read(eq(Patient.class), any(IIdType.class))).thenAnswer(inv -> {
			IIdType id = inv.getArgument(1);
			Patient p = new Patient();
			p.setId(id.getValue());
			p.addIdentifier().setSystem("urn:sys").setValue("id-" + id.getIdPart());
			return p;
		});
	}

	@Test
	void evaluate_BuildsGroupMetadata_And_IncludesMatchingSubjects() {
		EvidenceVariable ev = initiateEvidenceVariable();
		addLibraryExtension(ev, "Library/LibA");
		ev.addCharacteristic(expressionDef("IsEligible"));

		when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
			.thenReturn(bundleWithLibraryId("LibA"));

		when(cql.evaluateLibrary(any(Parameters.class), eq("LibA"))).thenAnswer(inv -> {
			Parameters p = inv.getArgument(0);
			String subject = p.getParameter("subject").getValue().primitiveValue();
			boolean in = subject != null && subject.endsWith("/123");
			return outBoolean("IsEligible", in);
		});

		List<String> subjects = List.of("Patient/123", "Patient/456");
		Group group = evaluation.evaluate(study, ev, subjects, new Parameters(), "FallbackLib");

		assertNotNull(group);
		assertEquals("group-study1", group.getIdElement().getIdPart());
		assertTrue(group.getActive());
		assertEquals(Group.GroupType.PERSON, group.getType());
		assertEquals("desc", group.getDescription());
		assertEquals("Patient Eligible for: MyStudy", group.getName());

		assertEquals(1, group.getMember().size());
	}

	@Test
	void evaluate_VacuousTruth_NoCharacteristics_IncludesAllSubjects() {
		EvidenceVariable ev = initiateEvidenceVariable(); // no characteristic -> vacuous truth
		List<String> subjects = List.of("Patient/1", "Patient/2", "Patient/3");

		Group group = evaluation.evaluate(study, ev, subjects, null, "LibX");
		assertEquals(3, group.getMember().size());
	}

	@Test
	void definitionExpression_AddsParametrization_UnderNestedParameters() {
		EvidenceVariable ev = initiateEvidenceVariable();
		ev.addCharacteristic(expressionDef("InAge"));

		Expression expr = ev.getCharacteristicFirstRep().getDefinitionExpression();
		Extension container = new Extension("https://www.isis.com/StructureDefinition/EXT-EVParametrisation");
		container.addExtension("name", new StringType("minAge"));
		container.addExtension("value", new IntegerType(18));
		expr.addExtension(container);

		when(cql.evaluateLibrary(any(Parameters.class), eq("LibA"))).thenAnswer(inv -> {
			Parameters incoming = inv.getArgument(0);
			Parameters nested = (Parameters) incoming.getParameter("parameters").getResource();
			assertNotNull(nested, "Expected nested Parameters under 'parameters'");
			var pp = nested.getParameter().get(0);
			assertEquals("minAge", pp.getName());
			assertEquals(18, ((IntegerType) pp.getValue()).getValue().intValue());
			return outBoolean("InAge", true);
		});

		Group group = evaluation.evaluate(study, ev, List.of("Patient/7"), new Parameters(), "LibA");
		assertEquals(1, group.getMember().size());
	}

	@Test
	void readBoolean_NullParameters_Throws422() {
		EvidenceVariable ev = initiateEvidenceVariable();
		ev.addCharacteristic(expressionDef("Ok"));

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(null);

		assertThrows(UnprocessableEntityException.class, () ->
			evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L")
		);
	}

	@Test
	void readBoolean_EvaluationError_Throws422WithOutcome() {
		EvidenceVariable ev = initiateEvidenceVariable();
		ev.addCharacteristic(expressionDef("Risky"));

		OperationOutcome oo = new OperationOutcome();
		oo.addIssue().getDetails().setText("Null pointer");
		Parameters out = new Parameters();
		out.addParameter().setName("evaluation error").setResource(oo);

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(out);

		UnprocessableEntityException ex = assertThrows(UnprocessableEntityException.class, () ->
			evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L")
		);
		assertNotNull(ex.getMessage());
		assertFalse(ex.getMessage().isBlank());
	}

	@Test
	void readBoolean_WrongType_Throws422() {
		EvidenceVariable ev = initiateEvidenceVariable();
		ev.addCharacteristic(expressionDef("Flag"));

		when(cql.evaluateLibrary(any(Parameters.class), anyString()))
			.thenReturn(outString("Flag", "not-a-boolean"));

		assertThrows(UnprocessableEntityException.class, () ->
			evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L")
		);
	}

	@Test
	void readBoolean_NullValue_Throws422() {
		EvidenceVariable ev = initiateEvidenceVariable();
		ev.addCharacteristic(expressionDef("Flag"));

		Parameters out = new Parameters();
		out.addParameter().setName("Flag"); // no value
		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(out);

		assertThrows(UnprocessableEntityException.class, () ->
			evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L")
		);
	}

	@Test
	void resolveLibraryId_UsesRepositoryWhenCanonicalPresent_ElseFallback() {
		EvidenceVariable ev = initiateEvidenceVariable();
		addLibraryExtension(ev, "http://example.org/fhir/Library/TestLib|1.2.3");
		ev.addCharacteristic(expressionDef("X"));

		when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
			.thenReturn(new Bundle());

		ArgumentCaptor<String> libIdCap = ArgumentCaptor.forClass(String.class);
		when(cql.evaluateLibrary(any(Parameters.class), libIdCap.capture()))
			.thenReturn(outBoolean("X", true));

		evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "Fallback");
		assertEquals("TestLib", libIdCap.getValue(), "Expected library id to fallback to tail of canonical");
	}

	@Test
	void resolveLibraryId_RepositorySuccess_UsesResolvedLibraryId() {
		EvidenceVariable ev = initiateEvidenceVariable();
		addLibraryExtension(ev, "Library/ResolvedLib");
		ev.addCharacteristic(expressionDef("X"));

		when(repository.search(eq(Bundle.class), eq(Library.class), any(), isNull()))
			.thenReturn(bundleWithLibraryId("ResolvedLib"));

		when(cql.evaluateLibrary(any(Parameters.class), eq("ResolvedLib")))
			.thenReturn(outBoolean("X", true));

		evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "DoesNotMatter");
		verify(cql).evaluateLibrary(any(Parameters.class), eq("ResolvedLib"));
	}

	@Test
	void resolveEvidenceVariable_CanonicalNotFound_Throws404() {
		EvidenceVariable outer = initiateEvidenceVariable();
		outer.addCharacteristic(new EvidenceVariable.EvidenceVariableCharacteristicComponent()
			.setDefinitionCanonical("http://x/EV/unknown|9")
		);

		when(repository.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenReturn(new Bundle());

		assertThrows(ResourceNotFoundException.class, () ->
			evaluation.evaluate(study, outer, List.of("Patient/1"), new Parameters(), "L"));
	}

	@Test
	void definitionCanonical_ResolvesNestedEV() {
		EvidenceVariable nested = initiateEvidenceVariable();
		nested.addCharacteristic(expressionDef("C1"));

		EvidenceVariable outer = initiateEvidenceVariable();
		outer.addCharacteristic(new EvidenceVariable.EvidenceVariableCharacteristicComponent()
			.setDefinitionCanonical("http://x/EV/nested|1")
		);

		when(repository.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenReturn(bundleWith(nested));

		when(cql.evaluateLibrary(any(Parameters.class), anyString()))
			.thenReturn(outBoolean("C1", true));

		Group g = evaluation.evaluate(study, outer, List.of("Patient/42"), new Parameters(), "LibZ");
		assertEquals(1, g.getMember().size());
	}

	@Test
	void combination_AND_AllOf() {
		EvidenceVariable ev = initiateEvidenceVariable();
		var c = new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		var comb = new EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent();
		comb.setCode(EvidenceVariable.CharacteristicCombination.ALLOF);
		comb.addCharacteristic(expressionDef("A"));
		comb.addCharacteristic(expressionDef("B"));
		c.setDefinitionByCombination(comb);
		ev.addCharacteristic(c);

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenAnswer(inv -> {
			Parameters in = inv.getArgument(0);
			String subj = in.getParameter("subject").getValue().primitiveValue();
			Parameters out = new Parameters();
			out.addParameter().setName("A").setValue(new BooleanType(true));
			out.addParameter().setName("B").setValue(new BooleanType(subj != null && subj.endsWith("/1")));
			out.addParameter().setName("evaluate boolean").setValue(new BooleanType(subj != null && subj.endsWith("/1")));
			return out;
		});

		Group g1 = evaluation.evaluate(study, ev, List.of("Patient/1", "Patient/2"), new Parameters(), "L");
		assertEquals(1, g1.getMember().size());
	}

	@Test
	void combination_OR_AnyOf() {
		EvidenceVariable ev = initiateEvidenceVariable();
		var c = new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		var comb = new EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent();
		comb.setCode(EvidenceVariable.CharacteristicCombination.ANYOF);
		comb.addCharacteristic(expressionDef("A"));
		comb.addCharacteristic(expressionDef("B"));
		c.setDefinitionByCombination(comb);
		ev.addCharacteristic(c);

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(outBoolean("A", false), outBoolean("B", true));

		Group g = evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L");
		assertEquals(1, g.getMember().size(), "AnyOf should include subject when any child is true");
	}

	@Test
	void combination_XOR_ExtensionTrue() {
		EvidenceVariable ev = initiateEvidenceVariable();
		var c = new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		var comb = new EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent();
		comb.addExtension("https://www.isis.com/StructureDefinition/EXT-Exclusive-OR", new BooleanType(true));
		comb.addCharacteristic(expressionDef("A"));
		comb.addCharacteristic(expressionDef("B"));
		c.setDefinitionByCombination(comb);
		ev.addCharacteristic(c);

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(outBoolean("A", true), outBoolean("B", false));
		Group g1 = evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L");
		assertEquals(1, g1.getMember().size());

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(outBoolean("A", true), outBoolean("B", true));
		Group g2 = evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L");
		assertEquals(0, g2.getMember().size());
	}

	@Test
	void characteristic_Exclude_NegatesResult() {
		EvidenceVariable ev = initiateEvidenceVariable();
		EvidenceVariable.EvidenceVariableCharacteristicComponent c = expressionDef("A");
		c.setExclude(true);
		ev.addCharacteristic(c);

		when(cql.evaluateLibrary(any(Parameters.class), anyString())).thenReturn(outBoolean("A", true));
		Group g = evaluation.evaluate(study, ev, List.of("Patient/1"), new Parameters(), "L");
		assertEquals(0, g.getMember().size(), "Exclude should invert the boolean to false");
	}

	@Test
	void pseudonymizeIdentifier_EncryptsValue_AndKeepsSystem() {
		Identifier src = new Identifier().setSystem("urn:oid:0.1.2.3").setValue("real-123");
		Identifier out = evaluation.pseudonymizeIdentifier(src);

		assertEquals("urn:oid:0.1.2.3", out.getSystem());
		assertNotEquals("real-123", out.getValue());
		assertNotNull(out.getValue());
		assertFalse(out.getValue().isBlank());
	}
}
