package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.search.Searches;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class EvidenceVariableService {

	/**
	 * Collects EV definitions from the {@code studyDefinition}, creates instances for each,
	 * and rewrites internal references so that EV-to-EV links target the newly created instances.
	 *
	 * @param studyDefinition      the source {@link ResearchStudy} definition to inspect (must not be {@code null})
	 * @param researchRepo         the {@link Repository} used to resolve referenced EVs (must not be {@code null})
	 * @param variableInstanceMap  OUT map: key (definition id, unqualified versionless) → EV instance
	 * @param variableUrlMap       OUT map: key (definition id, unqualified versionless) → instance fullUrl ({@code urn:uuid:...})
	 * @throws NullPointerException if any required argument is {@code null}
	 */
	public void createEvidenceVariableInstances(
		ResearchStudy studyDefinition,
		Repository researchRepo,
		Map<String, EvidenceVariable> variableInstanceMap,
		Map<String, String> variableUrlMap
	) {
		Objects.requireNonNull(studyDefinition, "studyDefinition");
		Objects.requireNonNull(researchRepo, "researchRepo");
		Objects.requireNonNull(variableInstanceMap, "variableInstanceMap");
		Objects.requireNonNull(variableUrlMap, "variableUrlMap");

		Map<String, EvidenceVariable> definitionMap = collectEvidenceVariablesFromResearchStudy(studyDefinition, researchRepo);

		Map<String, String> canonicalToKeyMap = new LinkedHashMap<>();
		for (EvidenceVariable definition : definitionMap.values()) {
			String key = stableIdKey(definition);
			if (key != null && definition.hasUrl()) {
				canonicalToKeyMap.put(definition.getUrl(), key);
			}
		}

		Map<String, String> instanceCanonicalMap = new LinkedHashMap<>();
		for (Map.Entry<String, EvidenceVariable> entry : definitionMap.entrySet()) {
			String key = entry.getKey();
			EvidenceVariable definition = entry.getValue();

			String uuid = UUID.randomUUID().toString();

			EvidenceVariable instance = definition.copy();
			instance.setId(uuid);
			instance.setActual(Boolean.TRUE);

			String baseCanonical = definition.hasUrl()
				? definition.getUrl()
				: definition.getIdElement().toUnqualifiedVersionless().getValue();
			String instanceCanonical = baseCanonical + "-instance-" + uuid;
			instance.setUrl(instanceCanonical);
			instanceCanonicalMap.put(key, instanceCanonical);

			RelatedArtifact relatedArtifact = new RelatedArtifact();
			relatedArtifact.setType(RelatedArtifact.RelatedArtifactType.DERIVEDFROM);
			String defCanonical = definition.hasUrl()
				? definition.getUrl()
				: definition.getIdElement().toUnqualifiedVersionless().getValue();
			relatedArtifact.setResource(defCanonical);
			instance.addRelatedArtifact(relatedArtifact);

			variableInstanceMap.put(key, instance);
			variableUrlMap.put(key, "urn:uuid:" + uuid);
		}

		for (Map.Entry<String, EvidenceVariable> e : variableInstanceMap.entrySet()) {
			EvidenceVariable instance = e.getValue();
			rewriteInternalReferences(instance, canonicalToKeyMap, variableUrlMap, instanceCanonicalMap);
		}
	}

	/**
	 * Collects all {@link EvidenceVariable} <b>definitions</b> referenced by a {@link ResearchStudy}.
	 *
	 * @param study the {@link ResearchStudy} definition to inspect
	 * @param repo  the {@link Repository} used to resolve EV references
	 * @return a map keyed by <em>unqualified versionless</em> id → EV definition
	 */
	public Map<String, EvidenceVariable> collectEvidenceVariablesFromResearchStudy(ResearchStudy study, Repository repo) {
		Map<String, EvidenceVariable> evidenceVariableMap = new LinkedHashMap<>();

		if (study.hasRecruitment() && study.getRecruitment().hasEligibility()) {
			EvidenceVariable evidenceVariable = resolveEvidenceVariable(study.getRecruitment().getEligibility(), repo);
			if (evidenceVariable != null) collectNestedEvidenceVariables(evidenceVariable, repo, evidenceVariableMap);
		}

		for (Extension extension : study.getExtension()) {
			for (Extension subExtension : extension.getExtension()) {
				if (subExtension.getUrl().equalsIgnoreCase("variable") && subExtension.getValue() instanceof Reference reference) {
					EvidenceVariable evidenceVariable = resolveEvidenceVariable(reference, repo);
					if (evidenceVariable != null) collectNestedEvidenceVariables(evidenceVariable, repo, evidenceVariableMap);
				}
			}
		}

		return evidenceVariableMap;
	}

	/**
	 * Recursively collects an EV definition and all nested EV definitions it references,
	 *
	 * @param evidenceVariable     the EV definition to process
	 * @param repo                 repository used to resolve nested EVs
	 * @param evidenceVariableMap  accumulator map (key → EV)
	 */
	private void collectNestedEvidenceVariables(
		EvidenceVariable evidenceVariable,
		Repository repo,
		Map<String, EvidenceVariable> evidenceVariableMap
	) {
		String key = stableIdKey(evidenceVariable);
		if (key == null || evidenceVariableMap.containsKey(key)) return;

		evidenceVariableMap.put(key, evidenceVariable);

		if (!evidenceVariable.hasCharacteristic()) return;

		for (EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic : evidenceVariable.getCharacteristic()) {
			processCharacteristic(characteristic, repo, evidenceVariableMap);
		}
	}

	/**
	 * Processes one {@link EvidenceVariable.EvidenceVariableCharacteristicComponent}:
	 *
	 * @param characteristic       the characteristic to inspect
	 * @param repo                 repository used to resolve referenced EVs
	 * @param evidenceVariableMap  accumulator map for collected EVs
	 */
	private void processCharacteristic(
		EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic,
		Repository repo,
		Map<String, EvidenceVariable> evidenceVariableMap
	) {
		// DefinitionReference
		if (characteristic.hasDefinitionReference()) {
			EvidenceVariable nested = resolveEvidenceVariable(characteristic.getDefinitionReference(), repo);
			if (nested != null) collectNestedEvidenceVariables(nested, repo, evidenceVariableMap);
		}

		// DefinitionCanonical
		if (characteristic.hasDefinitionCanonical()) {
			String canonical = characteristic.getDefinitionCanonical();
			if (canonical != null && !canonical.isBlank()) {
				EvidenceVariable nested = resolveByCanonical(canonical, repo);
				if (nested != null) collectNestedEvidenceVariables(nested, repo, evidenceVariableMap);
			}
		}

		// DefinitionByCombination
		if (characteristic.hasDefinitionByCombination() && characteristic.getDefinitionByCombination().hasCharacteristic()) {
			for (EvidenceVariable.EvidenceVariableCharacteristicComponent subDefinition
				: characteristic.getDefinitionByCombination().getCharacteristic()) {
				processCharacteristic(subDefinition, repo, evidenceVariableMap);
			}
		}
	}

	/**
	 * Rewrites internal references of an EV <b>instance</b> to point to other EV <b>instances</b>.
	 *
	 * @param instance              the EV instance whose characteristics will be updated
	 * @param canonicalToKeyMap     index of definition canonical URL → definition key
	 * @param instanceFullUrlMap    definition key → instance fullUrl (urn:uuid:...) used in transaction bundles
	 * @param instanceCanonicalMap  definition key → instance canonical URL
	 */
	private void rewriteInternalReferences(
		EvidenceVariable instance,
		Map<String, String> canonicalToKeyMap,
		Map<String, String> instanceFullUrlMap,
		Map<String, String> instanceCanonicalMap
	) {
		if (!instance.hasCharacteristic()) return;

		for (EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic : instance.getCharacteristic()) {
			rewriteCharacteristic(characteristic, canonicalToKeyMap, instanceFullUrlMap, instanceCanonicalMap);
		}
	}

	/**
	 * Rewrites a single characteristic's EV references.
	 *
	 * @param characteristic        the characteristic to rewrite
	 * @param canonicalToKeyMap     index of definition canonical URL → definition key
	 * @param instanceFullUrlMap    definition key → instance fullUrl (urn:uuid:...)
	 * @param instanceCanonicalMap  definition key → instance canonical URL
	 */
	private void rewriteCharacteristic(
		EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic,
		Map<String, String> canonicalToKeyMap,
		Map<String, String> instanceFullUrlMap,
		Map<String, String> instanceCanonicalMap
	) {
		// DefinitionReference
		if (characteristic.hasDefinitionReference()) {
			Reference reference = characteristic.getDefinitionReference();
			String targetKey = keyFromOriginalRef(reference, canonicalToKeyMap);
			if (targetKey != null && instanceFullUrlMap.containsKey(targetKey)) {
				characteristic.setDefinitionReference(new Reference(instanceFullUrlMap.get(targetKey)));
			}
		}
		// DefinitionCanonical
		if (characteristic.hasDefinitionCanonical()) {
			String canonical = characteristic.getDefinitionCanonical();
			String targetKey = keyFromCanonical(canonical, canonicalToKeyMap);
			if (targetKey != null && instanceCanonicalMap.containsKey(targetKey)) {
				characteristic.setDefinitionCanonical(instanceCanonicalMap.get(targetKey));
			}
		}
		// DefinitionByCombination
		if (characteristic.hasDefinitionByCombination() && characteristic.getDefinitionByCombination().hasCharacteristic()) {
			for (EvidenceVariable.EvidenceVariableCharacteristicComponent subDefinition
				: characteristic.getDefinitionByCombination().getCharacteristic()) {
				rewriteCharacteristic(subDefinition, canonicalToKeyMap, instanceFullUrlMap, instanceCanonicalMap);
			}
		}
	}

	/**
	 * Resolves an {@link EvidenceVariable} by its canonical URL using the provided {@link Repository}.
	 *
	 * @param canonical the canonical URL to resolve
	 * @param repo      the repository used for the search
	 * @return the resolved {@link EvidenceVariable}.
	 * @throws UnprocessableEntityException if the search fails
	 */
	private EvidenceVariable resolveByCanonical(String canonical, Repository repo) {
		if (canonical == null || canonical.isBlank()) return null;
		try {
			Bundle bundle = repo.search(Bundle.class, EvidenceVariable.class, Searches.byCanonical(canonical), null);
			return (bundle != null && bundle.hasEntry()) ? (EvidenceVariable) bundle.getEntryFirstRep().getResource() : null;
		} catch (Exception exception) {
			throw new UnprocessableEntityException("Failed to resolve EvidenceVariable by canonical '" + canonical + "': " + exception.getMessage());
		}
	}

	/**
	 * Resolves an {@link EvidenceVariable} from a {@link Reference}.
	 *
	 * @param reference a reference to an {@link EvidenceVariable}
	 * @param repo      the repository used to perform the lookup
	 * @return the resolved {@link EvidenceVariable}
	 * @throws UnprocessableEntityException if the lookup fails
	 */
	private EvidenceVariable resolveEvidenceVariable(Reference reference, Repository repo) {
		if (reference == null || !reference.hasReference()) return null;
		String referenceValue = reference.getReference();
		try {
			if (referenceValue != null && (referenceValue.startsWith("http://") || referenceValue.startsWith("https://"))) {
				Bundle bundle = repo.search(Bundle.class, EvidenceVariable.class, Searches.byCanonical(referenceValue), null);
				if (bundle != null && bundle.hasEntry()) {
					return (EvidenceVariable) bundle.getEntryFirstRep().getResource();
				}
			} else {
				IdType id = new IdType(referenceValue);
				return repo.read(EvidenceVariable.class, id);
			}
		} catch (Exception exception) {
			throw new UnprocessableEntityException("Failed to resolve EvidenceVariable reference '" + referenceValue + "': " + exception.getMessage());
		}
		return null;
	}

	/**
	 * Computes a stable map key for an {@link EvidenceVariable}.
	 *
	 * @param evidenceVariable the EV for which to compute the key
	 * @return the id.
	 */
	private String stableIdKey(EvidenceVariable evidenceVariable) {
		if (evidenceVariable == null || evidenceVariable.getIdElement() == null) return null;
		return evidenceVariable.getIdElement().toUnqualifiedVersionless().getValue();
	}

	/**
	 * Attempts to map a canonical URL back to a definition key.
	 *
	 * @param canonical        the canonical URL to resolve
	 * @param canonicalToKeyMap index of definition canonical URL → definition key
	 * @return the definition key, or {@code null} if it cannot be derived
	 */
	private String keyFromCanonical(String canonical, Map<String, String> canonicalToKeyMap) {
		if (canonical == null) return null;
		String key = canonicalToKeyMap.get(canonical);
		if (key != null) return key;
		int slash = canonical.lastIndexOf('/');
		if (slash >= 0 && slash < canonical.length() - 1) {
			return "EvidenceVariable/" + canonical.substring(slash + 1);
		}
		return null;
	}

	/**
	 * Extracts the definition key from an original EV {@link Reference}.
	 *
	 * @param reference         the original EV reference
	 * @param canonicalToKeyMap index of definition canonical URL → definition key
	 * @return the definition key (unqualified versionless id), or {@code null} if not resolvable
	 */
	private String keyFromOriginalRef(Reference reference, Map<String, String> canonicalToKeyMap) {
		if (reference == null || !reference.hasReference()) return null;
		String refStr = reference.getReference();
		if (refStr == null) return null;
		if (refStr.startsWith("http://") || refStr.startsWith("https://")) {
			return keyFromCanonical(refStr, canonicalToKeyMap);
		}
		IdType id = new IdType(refStr);
		return id.toUnqualifiedVersionless().getValue();
	}
}
