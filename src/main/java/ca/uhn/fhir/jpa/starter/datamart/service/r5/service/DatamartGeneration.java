package ca.uhn.fhir.jpa.starter.datamart.service.r5.service;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.model.ExpressionInfo;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.EvidenceVariableUtils;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.LibraryUtils;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DatamartGeneration {
	private static final Logger logger = LoggerFactory.getLogger(DatamartGeneration.class);

	private static final String EXT_CQF_LIBRARY = "http://hl7.org/fhir/StructureDefinition/cqf-library";
	private static final String EXT_EV_PARAM = "https://www.isis.com/StructureDefinition/EXT-EVParametrisation";

	// Sub-extensions inside the container:
	private static final String SUB_NAME = "name";
	private static final String SUB_VALUE = "value";

	private final RemoteCqlClient cqlEngine;
	private final Repository repository;

	public DatamartGeneration(RemoteCqlClient cqlEngine, Repository repository) {
		this.cqlEngine = cqlEngine;
		this.repository = repository;
	}

	/**
	 * Generate a datamart {@link ListResource} for a given ResearchStudy.
	 *
	 * @param researchStudy    The ResearchStudy driving datamart generation (for logging/title only).
	 * @param evidenceVariable The root EvidenceVariable from which expressions are discovered.
	 * @param subjectIds       Subject identifiers (formatted as {@code {resourceType}/{id}}, e.g., {@code Patient/123}).
	 * @param evaluateParams   Base Parameters passed to the CQL engine (endpoints, etc.).
	 * @param fallbackLibId    Fallback CQL library id if no cqf-library is found on the EV/characteristic.
	 * @return A ListResource containing references to all generated Parameters resources.
	 */
	public ListResource generateDatamart(ResearchStudy researchStudy,
													 EvidenceVariable evidenceVariable,
													 List<String> subjectIds,
													 Parameters evaluateParams,
													 String fallbackLibId) {
		logger.info("Generating Datamart {} with {} subject(s)", researchStudy.getUrl(), subjectIds.size());
		ListResource listParams = new ListResource();
		listParams.setStatus(ListResource.ListStatus.CURRENT);
		listParams.setMode(Enumerations.ListMode.SNAPSHOT);
		listParams.setTitle("Evaluation parameters for study " + researchStudy.getUrl());

		List<ExpressionInfo> expressionDefs = collectExpressionDefinitions(evidenceVariable, fallbackLibId);
		if (expressionDefs.isEmpty()) {
			logger.warn("No expression definitions found in EvidenceVariable {}", evidenceVariable.getUrl());
		}

		for (String subjectId : subjectIds) {
			if (subjectId == null) {
				throw new RuntimeException("SubjectId is required in order to calculate.");
			}
			Pair<String, String> subjectInfo = getSubjectTypeAndId(subjectId);
			String subjectIdPart = subjectInfo.getRight();

			Parameters paramForSubject = evalExpressionsForSubject(expressionDefs, subjectIdPart, evaluateParams);

			MethodOutcome outcome = repository.create(paramForSubject);
			if (outcome.getId() != null) {
				listParams.addEntry().setItem(new Reference("Parameters/" + outcome.getId().getIdPart()));
			} else if (outcome.getResource() != null) {
				listParams.addEntry().setItem(new Reference(outcome.getResource().getIdElement().getValue()));
			} else {
				throw new InternalErrorException(String.format(
					"Cannot retrieve ID of created parameters for EvidenceVariable %s, Subject %s",
					evidenceVariable.getId(), subjectId));
			}
		}
		return listParams;
	}

	/**
	 * Recursively traverses the provided EvidenceVariable to collect all expression definitions.
	 *
	 * @param evidenceVariable The EvidenceVariable to inspect.
	 * @param fallbackLibId    Fallback library id when no cqf-library extension is present.
	 * @return A list of {@link ExpressionInfo} capturing expression names and associated library ids.
	 */
	private List<ExpressionInfo> collectExpressionDefinitions(EvidenceVariable evidenceVariable, String fallbackLibId) {
		List<ExpressionInfo> expression = new ArrayList<>();
		if (evidenceVariable == null || evidenceVariable.getCharacteristic().isEmpty()) {
			return expression;
		}
		for (EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic : evidenceVariable.getCharacteristic()) {
			if (characteristic.hasDefinitionExpression()) {
				Expression expr = characteristic.getDefinitionExpression();
				String expressionName = safe(expr.getExpression());
				if (expressionName != null) {
					String libId = LibraryUtils.resolveLibraryId(repository, evidenceVariable, fallbackLibId);
					expression.add(new ExpressionInfo(expressionName, libId, retrieveExpressionParameters(expr)));
				}
			}
			if (characteristic.hasDefinitionByCombination()) {
				EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent combChar = characteristic.getDefinitionByCombination();
				if (combChar.getCharacteristic() != null && !combChar.getCharacteristic().isEmpty()) {
					for (EvidenceVariable.EvidenceVariableCharacteristicComponent nested : combChar.getCharacteristic()) {
						collectExpressionDefinitions(nested, evidenceVariable, fallbackLibId, expression);
					}
				}
			}
			if (characteristic.hasDefinitionCanonical()) {
				String canonical = safe(characteristic.getDefinitionCanonical());
				if (canonical != null) {
					EvidenceVariable nestedEv = EvidenceVariableUtils.resolveEvidenceVariable(repository, canonical);
					if (nestedEv != null) {
						String nestedFallback = fallbackLibId;
						Extension extension = nestedEv.getExtensionByUrl(EXT_CQF_LIBRARY);
						if (extension != null && extension.getValue() instanceof CanonicalType c) {
							String nestedLibId = LibraryUtils.resolveLibraryId(repository, nestedEv, fallbackLibId);
							if (nestedLibId != null) {
								nestedFallback = nestedLibId;
							}
						}
						expression.addAll(collectExpressionDefinitions(nestedEv, nestedFallback));
					}
				}
			}
		}
		LinkedHashSet<ExpressionInfo> unique = new LinkedHashSet<>(expression);
		return new ArrayList<>(unique);
	}

	/**
	 * Helper used while descending into definitionByCombination to keep the EV context
	 * for library resolution.
	 *
	 * @param characteristic The characteristic being inspected.
	 * @param contextEv      The parent EvidenceVariable used to resolve library id when needed.
	 * @param fallbackLibId  Fallback library id.
	 * @param expressions    Accumulator for collected definitions.
	 */
	private void collectExpressionDefinitions(EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic,
															EvidenceVariable contextEv,
															String fallbackLibId,
															List<ExpressionInfo> expressions) {
		if (characteristic.hasDefinitionExpression()) {
			Expression expression = characteristic.getDefinitionExpression();
			String exprName = safe(expression.getExpression());
			if (exprName != null) {
				String libId = LibraryUtils.resolveLibraryId(repository, contextEv, fallbackLibId);
				expressions.add(new ExpressionInfo(exprName, libId, retrieveExpressionParameters(expression)));
			}
		}
		if (characteristic.hasDefinitionByCombination()) {
			EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent combChar = characteristic.getDefinitionByCombination();
			if (combChar.getCharacteristic() != null && !combChar.getCharacteristic().isEmpty()) {
				for (EvidenceVariable.EvidenceVariableCharacteristicComponent nestedChar : combChar.getCharacteristic()) {
					collectExpressionDefinitions(nestedChar, contextEv, fallbackLibId, expressions);
				}
			}
		}
		if (characteristic.hasDefinitionCanonical()) {
			String canonical = safe(characteristic.getDefinitionCanonical());
			if (canonical != null) {
				EvidenceVariable nestedEv = EvidenceVariableUtils.resolveEvidenceVariable(repository, canonical);
				if (nestedEv != null) {
					String nestedFallback = fallbackLibId;
					Extension extension = nestedEv.getExtensionByUrl(EXT_CQF_LIBRARY);
					if (extension != null && extension.getValue() instanceof CanonicalType c) {
						String nestedLibId = LibraryUtils.resolveLibraryId(repository, nestedEv, fallbackLibId);
						if (nestedLibId != null) {
							nestedFallback = nestedLibId;
						}
					}
					expressions.addAll(collectExpressionDefinitions(nestedEv, nestedFallback));
				}
			}
		}
	}

	/**
	 * Creates a {@link Parameters} resource containing evaluation results for a single subject.
	 *
	 * @param expressions List of expressions (and their library ids) to evaluate.
	 * @param subjectId   The logical id of the subject (without the resource type prefix).
	 * @param baseParams  Base Parameters (endpoints) for the CQL engine.
	 * @return A populated Parameters resource ready to persist.
	 */
	private Parameters evalExpressionsForSubject(List<ExpressionInfo> expressions,
																String subjectId,
																Parameters baseParams) {
		Parameters params = new Parameters();
		try {
			Patient patient = repository.read(Patient.class, new IdType(subjectId));
			if (patient != null && patient.hasIdentifier()) {
				Identifier original = patient.getIdentifierFirstRep();
				Identifier pseudo = ResearchStudyUtils.pseudonymizeIdentifier(original);
				params.addParameter().setName("Patient").setValue(pseudo);
			}
		} catch (Exception ignore) {
		}

		if (expressions.isEmpty()) {
			return null;
		}

		for (ExpressionInfo expression : expressions) {
			String libId = expression.libraryId;

			Parameters callParams = baseParams.copy();
			callParams.addParameter().setName("parameters").setResource(expression.parameters);
			callParams.getParameter().removeIf(p -> "subject".equals(p.getName()));
			callParams.addParameter().setName("subject").setValue(new StringType(subjectId));

			Parameters result = cqlEngine.evaluateLibrary(callParams, libId);

			Map<String, Parameters.ParametersParameterComponent> resultByName = new HashMap<>();
			if (result != null) {
				for (Parameters.ParametersParameterComponent pp : result.getParameter()) {
					resultByName.put(pp.getName(), pp);
				}
			}

			Parameters.ParametersParameterComponent resComp = resultByName.get(expression.expressionName);
			Parameters.ParametersParameterComponent param = params.addParameter();
			param.setName(expression.expressionName);
			if (resComp != null) {
				if (resComp.hasValue()) {
					param.setValue(resComp.getValue());
				} else if (resComp.getResource() != null) {
					param.setResource(resComp.getResource());
				}
			}
		}
		return params;
	}

	/**
	 * Parses a composite subject identifier in the form {@code {type}/{id}}.
	 *
	 * @param subjectId Composite subject id.
	 * @return A pair where the left is the type and the right is the id.
	 */
	public Pair<String, String> getSubjectTypeAndId(String subjectId) {
		if (subjectId == null || !subjectId.contains("/")) {
			throw new IllegalArgumentException(String.format(
				"Unable to determine Subject type for id: %s. SubjectIds must be in the format {subjectType}/{subjectId} (e.g. Patient/123)",
				subjectId));
		}
		String[] parts = subjectId.split("/");
		return Pair.of(parts[0], parts[1]);
	}

	/**
	 * Collects expression parameterization from {@value EXT_EV_PARAM} extensions and attaches
	 * them under a nested {@code parameters} resource in the evaluation parameters.
	 *
	 * @param expression the Expression potentially carrying parameterization extensions
	 * @return new Parameters containing a nested {@code Parameters} resource under parameter name {@code "parameters"}
	 */
	private Parameters retrieveExpressionParameters(Expression expression) {
		Parameters expressionParam = new Parameters();

		for (Extension extension : expression.getExtension()) {
			if (extension.getUrl().equals(EXT_EV_PARAM)) {
				Extension nameExt = extension.getExtensionByUrl(SUB_NAME);
				String name = nameExt != null ? ((StringType) nameExt.getValue()).getValue():null;

				Extension valueExt = extension.getExtensionByUrl(SUB_VALUE);
				DataType value = valueExt != null ? valueExt.getValue():null;
				if(value != null && name != null) expressionParam.addParameter(name, value);
			}
		}
		return expressionParam;
	}

	/**
	 * Returns {@code null} when the input is null/blank, otherwise the trimmed string.
	 */
	private String safe(String s) {
		return (s == null || s.isBlank()) ? null : s.trim();
	}
}


