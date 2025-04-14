package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.elm.r1.ExpressionDef;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.cql.engine.execution.CqlEngine;
import org.opencds.cqf.cql.engine.execution.Libraries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

public class CohorteEvaluation {
	@Value("${app.anonymization.encryption-key}")
	private String encryptionKey;
	private static final Logger logger = LoggerFactory.getLogger(CohorteEvaluation.class);
	private final CqlEngine cqlEngine;

	public CohorteEvaluation(CqlEngine cqlEngine) {
		this.cqlEngine = cqlEngine;
	}

	/**
	 * Evaluates the cohort for the provided ResearchStudy.
	 * @param researchStudy    The ResearchStudy that is the basis for cohort evaluation.
	 * @param evidenceVariable The EvidenceVariable containing the definition expression for evaluation.
	 * @param subjectIds       A list of subject identifiers (each must be in the format {subjectType}/{subjectId}).
	 * @return A Group resource containing the subjects that meet the evaluation criteria.
	 * @throws RuntimeException if any subject identifier is {@code null}.
	 */
	protected Group evaluate(ResearchStudy researchStudy, EvidenceVariable evidenceVariable, List<String> subjectIds) {
		logger.info("Evaluating Cohort {} with {} subject(s)", researchStudy.getUrl(), subjectIds.size());
		Group group = new Group();
		for (String subjectId : subjectIds) {
			if (subjectId == null) {
				throw new RuntimeException("SubjectId is required in order to calculate.");
			}

			Pair<String, String> subjectInfo = this.getSubjectTypeAndId(subjectId);
			String subjectTypePart = subjectInfo.getLeft();
			String subjectIdPart = subjectInfo.getRight();
			this.cqlEngine.getState().setContextValue(subjectTypePart, subjectIdPart);
			this.evaluateVariableDefinition(group, evidenceVariable, subjectTypePart, subjectIdPart);
		}

		return group;
	}

	/**
	 * Evaluates the definition expression of the given EvidenceVariable for a specific subject
	 * and adds the subject to the Group if the evaluation result is true.
	 *
	 * @param group            The Group resource to which eligible subjects will be added.
	 * @param evidenceVariable The EvidenceVariable containing the definition expression for cohort evaluation.
	 * @param subjectType      The type of the subject (e.g., "Patient").
	 * @param subjectId        The identifier of the subject.
	 * @throws IllegalArgumentException if the EvidenceVariable is missing the definition expression.
	 */
	protected void evaluateVariableDefinition(Group group, EvidenceVariable evidenceVariable, String subjectType, String subjectId) {
		String definitionExpression = evidenceVariable.getCharacteristic().stream().findFirst()
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionByCombination)
			.map(EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent::getCharacteristic)
			.filter(list -> !list.isEmpty())
			.map(list -> list.get(0))
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionExpression)
			.map(Expression::getExpression)
			.orElseThrow(() -> new IllegalArgumentException(String.format("DefinitionExpression is missing for %s", evidenceVariable.getUrl())));
		if (definitionExpression != null && !definitionExpression.isEmpty()) {
			Object result = this.evaluateDefinitionExpression(definitionExpression);
			if (result instanceof Boolean) {
				if (Boolean.TRUE.equals(result)) {
					group.addMember().setEntity(new Reference(String.format("%s/%s", subjectType, pseudonymizeRealId(subjectId))));
				}
			}
		}
	}

	/**
	 * Pseudonymizes a real subject identifier by appending a configured encryption key, then computing the SHA-256 hash.
	 *
	 * @param realId The original subject identifier.
	 * @return The pseudonymized identifier.
	 */
	public String pseudonymizeRealId(String realId) {
		String encryptedId = realId + encryptionKey;
		return DigestUtils.sha256Hex(encryptedId);
	}

	/**
	 * Evaluates the given CQL expression using the CQL evaluation engine.
	 *
	 * @param criteriaExpression The CQL expression to evaluate.
	 * @return The result of the evaluated expression.
	 */
	protected Object evaluateDefinitionExpression(String criteriaExpression) {
		ExpressionDef ref = Libraries.resolveExpressionRef(criteriaExpression, this.cqlEngine.getState().getCurrentLibrary());
		Object result = this.cqlEngine.getEvaluationVisitor().visitExpressionDef(ref, this.cqlEngine.getState());
		return result;
	}

	/**
	 * Parses the subject identifier to extract the subject type and identifier.
	 *
	 * @param subjectId The subject identifier string.
	 * @return A Pair where the left element is the subject type and the right element is the subject identifier.
	 * @throws IllegalArgumentException if the subject identifier does not follow the required format.
	 */
	protected Pair<String, String> getSubjectTypeAndId(String subjectId) {
		if (subjectId.contains("/")) {
			String[] subjectIdParts = subjectId.split("/");
			return Pair.of(subjectIdParts[0], subjectIdParts[1]);
		} else {
			throw new IllegalArgumentException(String.format("Unable to determine Subject type for id: %s. SubjectIds must be in the format {subjectType}/{subjectId} (e.g. Patient/123)", subjectId));
		}
	}

}
