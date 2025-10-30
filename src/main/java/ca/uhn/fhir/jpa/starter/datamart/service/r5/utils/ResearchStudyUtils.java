package ca.uhn.fhir.jpa.starter.datamart.service.r5.utils;

import ca.uhn.fhir.jpa.starter.datamart.service.CryptoUtils;
import ca.uhn.fhir.model.api.IQueryParameterType;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;

import java.util.*;
import java.util.stream.Collectors;

public class ResearchStudyUtils {
	public static final String EXT_URL =
		"https://www.isis.com/StructureDefinition/EXT-Datamart";
	public static final String CUSTOM_PHASE_SYSTEM =
		"https://www.isis.com/CodeSystem/COS-ResearchStudyPhase";
	public static final String INITIAL_PHASE = "initial";
	public static final String POST_DATAMART = "post-datamart";
	public static final String VAR_EXT_NAME = "variable";
	public static final String EVAL_EXT_NAME = "evaluation";
	private static final String ERR_MISSING_EXT =
		"ResearchStudy %s does not contain extension %s";
	private static final String ERR_MISSING_VAR_EXT =
		"Extension %s does not contain sub-extension '%s'";
	private static final String ERR_INVALID_REF_EV =
		"ResearchStudy %s does not have a valid eligibility reference to an EvidenceVariable";
	private static final String ERR_MISSING_GROUP =
		"ResearchStudy %s does not have an actualGroup defined in recruitment";
	private static final String ERR_INVALID_REF_Group =
		"ResearchStudy %s has an invalid actualGroup reference";
	private static final String ERR_NO_MEMBERS =
		"Group %s contains no members and thus no eligible patients";
	private static final String ERR_INVALID_MEMBER_REF =
		"Group %s contains invalid member reference";

	/**
	 * Retrieves the EvidenceVariable referenced by the custom Datamart extension
	 * on the given ResearchStudy.
	 *
	 * @param study      The ResearchStudy containing the custom Datamart extension.
	 * @param repository The FHIR repository for resource lookups.
	 * @return The EvidenceVariable resource referenced by the extension.
	 * @throws ResourceNotFoundException if the expected extension or reference is missing or invalid.
	 */
	public static EvidenceVariable getEvidenceVariable(ResearchStudy study, Repository repository) {

		Extension ext = study.getExtension().stream()
			.filter(e -> EXT_URL.equals(e.getUrl()))
			.findFirst()
			.orElseThrow(() -> new ResourceNotFoundException(
				String.format(ERR_MISSING_EXT, study.getUrl(), EXT_URL)));

		Extension varExt = ext.getExtension().stream()
			.filter(e -> VAR_EXT_NAME.equals(e.getUrl()))
			.findFirst()
			.orElseThrow(() -> new ResourceNotFoundException(
				String.format(ERR_MISSING_VAR_EXT, EXT_URL, VAR_EXT_NAME)));

		Reference evidenceVariableRef = varExt.getValueReference();

		if (evidenceVariableRef == null || !"EvidenceVariable".equals(evidenceVariableRef.getReferenceElement().getResourceType())) {
			throw new ResourceNotFoundException(
				String.format(ERR_INVALID_REF_EV, study.getUrl(), evidenceVariableRef.getReference()));
		}

		EvidenceVariable ev = repository.read(EvidenceVariable.class, new IdType(evidenceVariableRef.getReferenceElement().getIdPart()));
		return ev;
	}

	/**
	 * Retrieves the eligible Group defined in the given ResearchStudy.
	 *
	 * @param study      The {@link ResearchStudy} used as the basis for generateDatamart.
	 * @param repository The FHIR repository for resource lookups.
	 * @return The Group resource representing eligible patients.
	 * @throws IllegalArgumentException  if the actualGroup is missing or the reference is invalid.
	 * @throws ResourceNotFoundException if the group contains no members.
	 */
	public static Group getEligibleGroup(ResearchStudy study, Repository repository) {
		if (!study.getRecruitment().hasActualGroup()) {
			throw new IllegalArgumentException(
				String.format(ERR_MISSING_GROUP, study.getUrl()));
		}

		Reference groupRef = study.getRecruitment().getActualGroup();
		if (groupRef == null ||
			groupRef.getReferenceElement() == null ||
			!groupRef.getReferenceElement().getResourceType().equals("Group")) {
			throw new IllegalArgumentException(
				String.format(ERR_INVALID_REF_Group, study.getUrl())
			);
		}
		Group group = repository.read(Group.class, new IdType(groupRef.getReferenceElement().getIdPart()));

		if (group.getMember().isEmpty()) {
			throw new ResourceNotFoundException(
				String.format(ERR_NO_MEMBERS, groupRef.getId()));
		}
		return group;
	}

	/**
	 * Extracts the list of decrypted Patient references from the given Group.
	 *
	 * @param group The Group resource representing eligible patients.
	 * @return A list of strings in the format "Patient/{decryptedId}".
	 * @throws ResourceNotFoundException if any member reference is invalid or not a Patient.
	 */
	public static List<String> getSubjectReferences(Group group, Repository repository) {
		if (group.getMember().isEmpty()) {
			return List.of();
		}

		return group.getMember().stream()
			.map(Group.GroupMemberComponent::getEntity)
			.filter(Objects::nonNull)
			.map(entity -> referenceFromIdentifier(entity, repository))
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}

	private static String referenceFromIdentifier(Reference entity, Repository repository) {
		Identifier id = desanonmyseIdentifier(entity.getIdentifier());
		Map<String, List<IQueryParameterType>> params = new HashMap<>();
		params.put("identifier",
			Collections.singletonList(new TokenParam(id.getSystem(), id.getValue())));
		Bundle b = repository.search(Bundle.class, Patient.class, params, null);
		if(b.getEntry().isEmpty()) {
			return null;
		}
		Patient patient = (Patient) b.getEntry().get(0).getResource();
		IIdType pid = patient.getIdElement();
		return pid.getResourceType() + "/" + pid.getIdPart();
	}

	/**
	 * Decrypts an encrypted identifier to its original form.
	 *
	 * @param encryptedId The encrypted identifier string.
	 * @return The decrypted (original) identifier.
	 * @throws RuntimeException if decryption fails.
	 */
	public static Identifier desanonmyseIdentifier(Identifier encryptedId) {
		try {
			String original = CryptoUtils.decrypt(encryptedId.getValue());
			Identifier decrypted = encryptedId.copy();
			decrypted.setValue(original);
			return decrypted;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Pseudonymizes a real subject identifier by appending a configured encryption key, then computing the SHA-256 hash.
	 *
	 * @param original The original subject identifier.
	 * @return The pseudonymized identifier.
	 */
	public static Identifier pseudonymizeIdentifier(Identifier original) {
		try {
			String encrypted = ca.uhn.fhir.jpa.starter.cohort.service.r5.CryptoUtils.encrypt(original.getValue());
			Identifier copy = original.copy();
			copy.setValue(encrypted);
			return copy;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Retrieves the list of the datamart evaluation defined in the given ResearchStudy.
	 *
	 * @param study The {@link ResearchStudy} used as the basis to export datamart.
	 * @return The List resource representing evaluation parameters.
	 * @throws IllegalArgumentException if the evaluation extension is missing or the reference is invalid.
	 */
	public static String getEvaluationListId(ResearchStudy study) {

		Extension ext = study.getExtension().stream()
			.filter(e -> EXT_URL.equals(e.getUrl()))
			.findFirst()
			.orElseThrow(() -> new ResourceNotFoundException(
				String.format(ERR_MISSING_EXT, study.getUrl(), EXT_URL)));

		Extension evalExt = ext.getExtension().stream()
			.filter(e -> EVAL_EXT_NAME.equals(e.getUrl()))
			.findFirst()
			.orElseThrow(() -> new ResourceNotFoundException(
				String.format(ERR_MISSING_VAR_EXT, EXT_URL, EVAL_EXT_NAME)));

		Reference listRef = evalExt.getValueReference();

		if (listRef == null || !"List".equals(listRef.getReferenceElement().getResourceType())) {
			throw new ResourceNotFoundException(
				String.format(ERR_INVALID_REF_EV, study.getUrl(), listRef.getReference()));
		}
		return listRef.getReferenceElement().getIdPart();
	}
}
