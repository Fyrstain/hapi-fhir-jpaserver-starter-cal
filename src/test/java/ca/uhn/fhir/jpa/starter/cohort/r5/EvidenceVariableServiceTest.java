package ca.uhn.fhir.jpa.starter.cohort.r5;

import ca.uhn.fhir.jpa.starter.cohort.service.r5.EvidenceVariableService;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.r5.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opencds.cqf.fhir.api.Repository;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link EvidenceVariableService}.
 */
class EvidenceVariableServiceTest {

	private static final String EV_A_ID = "EvidenceVariable/EV-A";
	private static final String EV_B_ID = "EvidenceVariable/EV-B";
	private static final String EV_C_ID = "EvidenceVariable/EV-C";
	private static final String EV_A_CANONICAL = "http://example.org/ev/EV-A";
	private static final String EV_B_CANONICAL = "http://example.org/ev/EV-B";
	private static final String EV_C_CANONICAL = "http://example.org/ev/EV-C";

	private Repository repo;
	private EvidenceVariableService service;
	private Map<String, EvidenceVariable> instanceMap;
	private Map<String, String> urlMap;

	@BeforeEach
	void setUp() {
		repo = Mockito.mock(Repository.class);
		service = new EvidenceVariableService();
		instanceMap = new LinkedHashMap<>();
		urlMap = new LinkedHashMap<>();
	}

	@Test
	void createEvidenceVariableInstances() {
		// Arrange
		EvidenceVariable evB = createEvidenceVariable(EV_B_ID, EV_B_CANONICAL);
		EvidenceVariable evA = createEvidenceVariable(EV_A_ID, EV_A_CANONICAL);
		addCanonicalCharacteristic(evA, EV_B_CANONICAL);

		ResearchStudy study = createStudyWithEligibility(EV_A_ID);
		addVariableExtension(study, EV_B_ID);

		mockEvidenceVariableRead(EV_A_ID, evA);
		mockCanonicalSearch(evB);

		// Act
		service.createEvidenceVariableInstances(study, repo, instanceMap, urlMap);

		// Assert
		assertEquals(2, instanceMap.size());
		assertEquals(2, urlMap.size());
		assertTrue(instanceMap.containsKey(EV_A_ID));
		assertTrue(instanceMap.containsKey(EV_B_ID));

		assertInstanceHasUuidAndUrl(instanceMap.get(EV_A_ID), EV_A_CANONICAL);
		assertInstanceHasUuidAndUrl(instanceMap.get(EV_B_ID), EV_B_CANONICAL);
	}

	@Test
	void collectEvidenceVariablesFromResearchStudy() {
		// Arrange
		EvidenceVariable evC = createEvidenceVariable("EvidenceVariable/EV-C", null);
		EvidenceVariable evD = createEvidenceVariable("EvidenceVariable/EV-D", null);

		ResearchStudy study = createStudyWithEligibility("EvidenceVariable/EV-C");
		addVariableExtension(study, "EvidenceVariable/EV-D");

		mockEvidenceVariableRead("EvidenceVariable/EV-C", evC);
		mockEvidenceVariableRead("EvidenceVariable/EV-D", evD);

		// Act
		Map<String, EvidenceVariable> collected =
			service.collectEvidenceVariablesFromResearchStudy(study, repo);

		// Assert
		assertEquals(2, collected.size());
		assertTrue(collected.containsKey("EvidenceVariable/EV-C"));
		assertTrue(collected.containsKey("EvidenceVariable/EV-D"));
	}

	@Test
	void CreateInstanceForVariableInDefinitionByCombination() {
		// Arrange
		EvidenceVariable evC = createEvidenceVariable(EV_C_ID, EV_C_CANONICAL);
		EvidenceVariable evB = createEvidenceVariableWithCombination(EV_C_CANONICAL);
		EvidenceVariable evA = createEvidenceVariable(EV_A_ID, EV_A_CANONICAL);
		addReferenceCharacteristic(evA, "http://server/fhir/EvidenceVariable/EV-B");

		ResearchStudy study = createStudyWithEligibility(EV_A_ID);

		mockEvidenceVariableRead(EV_A_ID, evA);
		when(repo.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenReturn(createBundle(evB))
			.thenReturn(createBundle(evC));

		// Act
		service.createEvidenceVariableInstances(study, repo, instanceMap, urlMap);

		// Assert
		assertTrue(instanceMap.containsKey(EV_A_ID));
		assertTrue(instanceMap.containsKey(EV_B_ID));
		assertTrue(instanceMap.containsKey(EV_C_ID));

		EvidenceVariable aInstance = instanceMap.get(EV_A_ID);
		EvidenceVariable bInstance = instanceMap.get(EV_B_ID);
		EvidenceVariable cInstance = instanceMap.get(EV_C_ID);

		assertEquals(urlMap.get(EV_B_ID),
			aInstance.getCharacteristicFirstRep().getDefinitionReference().getReference());

		String rewrittenCanonical = bInstance.getCharacteristicFirstRep()
			.getDefinitionByCombination()
			.getCharacteristicFirstRep()
			.getDefinitionCanonical();
		assertEquals(cInstance.getUrl(), rewrittenCanonical);
	}

	@Test
	void createEvidenceVariableInstancesNoUrl() {
		// Arrange
		EvidenceVariable evF = createEvidenceVariable("EvidenceVariable/EV-F", null);
		ResearchStudy study = createStudyWithEligibility("EvidenceVariable/EV-F");
		mockEvidenceVariableRead("EvidenceVariable/EV-F", evF);

		// Act
		service.createEvidenceVariableInstances(study, repo, instanceMap, urlMap);

		// Assert
		assertNotNull(instanceMap.get("EvidenceVariable/EV-F"));
	}

	@Test
	void resolveByCanonicalErrorIsWrapped() {
		// Arrange
		when(repo.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenThrow(new RuntimeException("boom"));

		EvidenceVariable ev = createEvidenceVariable(EV_A_ID, null);
		addCanonicalCharacteristic(ev, EV_B_CANONICAL);

		ResearchStudy study = createStudyWithEligibility(EV_A_ID);
		mockEvidenceVariableRead(EV_A_ID, ev);

		// Act & Assert
		assertThrows(UnprocessableEntityException.class,
			() -> service.createEvidenceVariableInstances(study, repo, instanceMap, urlMap));
	}

	@Test
	@DisplayName("resolveEvidenceVariable: http reference with empty bundle returns null (no crash)")
	void resolveEvidenceVariable_httpEmpty_returnsNull() {
		// Arrange
		when(repo.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenReturn(new Bundle());

		EvidenceVariable ev = createEvidenceVariable(EV_A_ID, null);
		addReferenceCharacteristic(ev, "https://server/fhir/EvidenceVariable/NONE");

		ResearchStudy study = createStudyWithEligibility(EV_A_ID);
		mockEvidenceVariableRead(EV_A_ID, ev);

		// Act
		service.createEvidenceVariableInstances(study, repo, instanceMap, urlMap);

		// Assert
		assertTrue(instanceMap.containsKey(EV_A_ID));
	}

	// ==================== Helper Methods ====================

	private EvidenceVariable createEvidenceVariable(String id, String canonical) {
		EvidenceVariable ev = new EvidenceVariable();
		ev.setId(id);
		if (canonical != null) {
			ev.setUrl(canonical);
		}
		return ev;
	}

	private void addCanonicalCharacteristic(EvidenceVariable ev, String canonical) {
		EvidenceVariable.EvidenceVariableCharacteristicComponent ch =
			new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		ch.setDefinitionCanonical(canonical);
		ev.addCharacteristic(ch);
	}

	private void addReferenceCharacteristic(EvidenceVariable ev, String reference) {
		EvidenceVariable.EvidenceVariableCharacteristicComponent ch =
			new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		ch.setDefinitionReference(new Reference(reference));
		ev.addCharacteristic(ch);
	}

	private ResearchStudy createStudyWithEligibility(String eligibilityRef) {
		ResearchStudy study = new ResearchStudy();
		ResearchStudy.ResearchStudyRecruitmentComponent recruitment =
			new ResearchStudy.ResearchStudyRecruitmentComponent();
		recruitment.setEligibility(new Reference(eligibilityRef));
		study.setRecruitment(recruitment);
		return study;
	}

	private void addVariableExtension(ResearchStudy study, String variableRef) {
		Extension ext = new Extension().setUrl("parent");
		ext.addExtension(new Extension("variable", new Reference(variableRef)));
		study.addExtension(ext);
	}

	private void mockEvidenceVariableRead(String id, EvidenceVariable ev) {
		when(repo.read(eq(EvidenceVariable.class),
			argThat(idArg -> id.equals(idArg.toUnqualifiedVersionless().getValue()))))
			.thenReturn(ev);
	}

	private void mockCanonicalSearch(EvidenceVariable... evidenceVariables) {
		Bundle bundle = new Bundle();
		for (EvidenceVariable ev : evidenceVariables) {
			bundle.addEntry().setResource(ev);
		}
		when(repo.search(eq(Bundle.class), eq(EvidenceVariable.class), any(), isNull()))
			.thenReturn(bundle);
	}

	private void assertInstanceHasUuidAndUrl(EvidenceVariable instance, String expectedUrlPrefix) {
		assertNotNull(instance.getIdElement().getIdPart());
		assertTrue(instance.getUrl().endsWith("-instance-" + instance.getIdElement().getIdPart()));
		assertTrue(instance.getUrl().startsWith(expectedUrlPrefix));
	}

	private EvidenceVariable createEvidenceVariableWithCombination(String characteristicCanonical) {
		EvidenceVariable evB = createEvidenceVariable(EV_B_ID, EV_B_CANONICAL);

		EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic =
			new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		characteristic.setDefinitionCanonical(characteristicCanonical);

		EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent combination =
			new EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent();
		combination.addCharacteristic(characteristic);

		EvidenceVariable.EvidenceVariableCharacteristicComponent characteristicEvB =
			new EvidenceVariable.EvidenceVariableCharacteristicComponent();
		characteristicEvB.setDefinitionByCombination(combination);
		evB.addCharacteristic(characteristicEvB);

		return evB;
	}

	private Bundle createBundle(EvidenceVariable... evidenceVariables) {
		Bundle bundle = new Bundle();
		for (EvidenceVariable ev : evidenceVariables) {
			bundle.addEntry().setResource(ev);
		}
		return bundle;
	}
}
