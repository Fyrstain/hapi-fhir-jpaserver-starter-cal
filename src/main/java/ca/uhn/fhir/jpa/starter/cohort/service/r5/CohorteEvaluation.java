package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CohorteEvaluation {
	private static final Logger logger = LoggerFactory.getLogger(CohorteEvaluation.class);
	private final RemoteCqlClient cqlEngine;
	private final Repository repository;

	public CohorteEvaluation(RemoteCqlClient cqlEngine, Repository repository) {
		this.cqlEngine = cqlEngine;
		this.repository = repository;
	}

	/**
	 * Evaluates the cohort for the provided ResearchStudy.
	 *
	 * @param researchStudy    The ResearchStudy that is the basis for cohort evaluation.
	 * @param evidenceVariable The EvidenceVariable containing the definition expression for evaluation.
	 * @param subjectIds       A list of subject identifiers (each must be in the format {subjectType}/{subjectId}).
	 * @return A Group resource containing the subjects that meet the evaluation criteria.
	 * @throws RuntimeException if any subject identifier is {@code null}.
	 */
	public Group evaluate(ResearchStudy researchStudy, EvidenceVariable evidenceVariable, List<String> subjectIds, Parameters evaluateParams, String libId) {
		logger.info("Evaluating Cohort {} with {} subject(s)", researchStudy.getUrl(), subjectIds.size());
		Group group = new Group();
		evaluateParams.addParameter().setName("subject");
		for (String subjectId : subjectIds) {
			if (subjectId == null) {
				throw new RuntimeException("SubjectId is required in order to calculate.");
			}

			Pair<String, String> subjectInfo = this.getSubjectTypeAndId(subjectId);
			String subjectTypePart = subjectInfo.getLeft();
			String subjectIdPart = subjectInfo.getRight();
			evaluateParams.getParameter("subject").setValue(new StringType(subjectIdPart));
			//this.cqlEngine.getState().setContextValue(subjectTypePart, subjectIdPart);
			this.evaluateVariableDefinition(group, evidenceVariable, subjectTypePart, subjectIdPart, evaluateParams, libId);
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
	public void evaluateVariableDefinition(Group group, EvidenceVariable evidenceVariable, String subjectType, String subjectId, Parameters evaluateParams, String libId) {
		String definitionExpression = evidenceVariable.getCharacteristic().stream().findFirst()
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionByCombination)
			.map(EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent::getCharacteristic)
			.filter(list -> !list.isEmpty())
			.map(list -> list.get(0))
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionExpression)
			.map(Expression::getExpression)
			.orElseThrow(() -> new IllegalArgumentException(String.format("DefinitionExpression is missing for %s", evidenceVariable.getUrl())));
		if (definitionExpression != null && !definitionExpression.isEmpty()) {
			Object result = this.evaluateDefinitionExpression(definitionExpression, evaluateParams, libId);
			if (result instanceof BooleanType) {

				if (((BooleanType) result).booleanValue()) {
					Identifier patientIdent = repository.read(Patient.class, new IdType(subjectId)).getIdentifier().get(0);
					group.addMember().setEntity(new Reference().setIdentifier(pseudonymizeIdentifier(patientIdent)));
				}
			}
		}
	}

	/**
	 * Pseudonymizes a real subject identifier by appending a configured encryption key, then computing the SHA-256 hash.
	 *
	 * @param original The original subject identifier.
	 * @return The pseudonymized identifier.
	 */
	public Identifier pseudonymizeIdentifier(Identifier original) {
		try {
			String encrypted = CryptoUtils.encrypt(original.getValue());
			Identifier copy = original.copy();
			copy.setValue(encrypted);
			return copy;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Evaluates the given CQL expression using the CQL evaluation engine.
	 *
	 * @param criteriaExpression The CQL expression to evaluate.
	 * @return The result of the evaluated expression.
	 */
	public Object evaluateDefinitionExpression(String criteriaExpression, Parameters evaluateParams, String libId) {
		//ExpressionDef ref = Libraries.resolveExpressionRef(criteriaExpression, this.cqlEngine.getState().getCurrentLibrary());
		//Object result = this.cqlEngine.getEvaluationVisitor().visitExpressionDef(ref, this.cqlEngine.getState());
		//evaluateParams.addParameter().setName("expression").setValue(new StringType(criteriaExpression));
		Parameters result = cqlEngine.evaluateLibrary(evaluateParams, libId);
		return result.getParameter(criteriaExpression).getValue();
	}

	/**
	 * Parses the subject identifier to extract the subject type and identifier.
	 *
	 * @param subjectId The subject identifier string.
	 * @return A Pair where the left element is the subject type and the right element is the subject identifier.
	 * @throws IllegalArgumentException if the subject identifier does not follow the required format.
	 */
	public Pair<String, String> getSubjectTypeAndId(String subjectId) {
		if (subjectId.contains("/")) {
			String[] subjectIdParts = subjectId.split("/");
			return Pair.of(subjectIdParts[0], subjectIdParts[1]);
		} else {
			throw new IllegalArgumentException(String.format("Unable to determine Subject type for id: %s. SubjectIds must be in the format {subjectType}/{subjectId} (e.g. Patient/123)", subjectId));
		}
	}

}
