package ca.uhn.fhir.jpa.starter.datamart.service.r5;

import ca.uhn.fhir.rest.api.MethodOutcome;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DatamartGeneration {
	private static final Logger logger = LoggerFactory.getLogger(DatamartGeneration.class);
	private final RemoteCqlClient cqlEngine;
	private final Repository repository;

	public DatamartGeneration(RemoteCqlClient cqlEngine, Repository repository) {
		this.cqlEngine = cqlEngine;
		this.repository = repository;
	}

	/**
	 * Generate the datamart for the provided ResearchStudy.
	 *
	 * @param researchStudy    The ResearchStudy that is the basis for datamart generation.
	 * @param evidenceVariable The EvidenceVariable containing the definition expression for evaluation.
	 * @param subjectIds       A list of subject identifiers (each must be in the format {subjectType}/{subjectId}).
	 * @param id               The id of the CQL library containing the expression.
	 * @return A ListResource containing the evaluation result for each patient.
	 * @throws RuntimeException if any subject identifier is {@code null}.
	 */
	public ListResource generateDatamart(ResearchStudy researchStudy, EvidenceVariable evidenceVariable, List<String> subjectIds, Parameters evaluateParams, String id) {
		logger.info("Generating Datamart {} with {} subject(s)", researchStudy.getUrl(), subjectIds.size());
		ListResource listParams = new ListResource();
		listParams.setStatus(ListResource.ListStatus.CURRENT);
		listParams.setMode(Enumerations.ListMode.SNAPSHOT);
		listParams.setTitle("Evaluation parameters for study " + researchStudy.getUrl());
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
			this.evaluateVariableDefinition(listParams, evidenceVariable, subjectTypePart, subjectIdPart, evaluateParams, id);
		}

		return listParams;
	}

	/**
	 * Evaluates the definition expression of the given EvidenceVariable for a specific subject
	 * and adds the evaluation result to a ListResource.
	 *
	 * @param listParams       The ListResource to which the results will be added.
	 * @param evidenceVariable The EvidenceVariable containing the definition expression for datamart generation.
	 * @param subjectType      The type of the subject.
	 * @param subjectId        The identifier of the subject.
	 * @param libId            The id of the CQL library containing the expression.
	 * @throws IllegalArgumentException if the EvidenceVariable is missing the definition expression.
	 */
	public void evaluateVariableDefinition(ListResource listParams, EvidenceVariable evidenceVariable, String subjectType, String subjectId, Parameters evaluateParams, String libId) {
		/*String definitionExpression = evidenceVariable.getCharacteristic().stream().findFirst()
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionByCombination)
			.map(EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent::getCharacteristic)
			.filter(list -> !list.isEmpty())
			.map(list -> list.get(0))
			.map(EvidenceVariable.EvidenceVariableCharacteristicComponent::getDefinitionExpression)
			.map(Expression::getExpression)
			.orElseThrow(() -> new IllegalArgumentException(String.format("DefinitionExpression is missing for %s", evidenceVariable.getUrl())));*/
		Parameters result = this.evaluateDefinitionExpression(evaluateParams, libId);
		Patient patient = (Patient) result.getParameter("Patient").getResource();
		result.getParameter("Patient").setResource(null);
		result.getParameter("Patient").setValue(ResearchStudyUtils.pseudonymizeIdentifier(patient.getIdentifier().get(0)));
		MethodOutcome outcome = repository.create(result);
		listParams.addEntry().setItem(new Reference(String.format("%s/%s", outcome.getId().getResourceType(), outcome.getId().getIdPart())));
	}

	/**
	 * Evaluates the given CQL expression using the CQL evaluation engine.
	 *
	 * @param libId The id of the CQL library containing the expression.
	 * @return A FHIR Parameters resource containing the results of the evaluation.
	 */
	public Parameters evaluateDefinitionExpression(Parameters evaluateParam, String libId) {
		/*CqlFhirParametersConverter cqlFhirParametersConverter = Engines.getCqlFhirParametersConverter(this.repository.fhirContext());
		return (Parameters) cqlFhirParametersConverter.toFhirParameters(this.cqlEngine.evaluate(id));*/
		return cqlEngine.evaluateLibrary(evaluateParam, libId);
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
