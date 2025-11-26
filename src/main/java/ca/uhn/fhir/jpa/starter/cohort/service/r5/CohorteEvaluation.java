package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.instance.model.api.IBaseHasExtensions;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.search.Searches;

import java.util.ArrayList;
import java.util.List;

public class CohorteEvaluation {
	private static final String EXT_CQF_LIBRARY = "http://hl7.org/fhir/StructureDefinition/cqf-library";
	private static final String EXT_XOR = "https://www.isis.com/StructureDefinition/EXT-Exclusive-OR";
	private static final String EXT_EV_PARAM = "https://www.isis.com/StructureDefinition/EXT-EVParametrisation";

	// Sub-extensions inside the container:
	private static final String SUB_NAME = "name";
	private static final String SUB_VALUE = "value";
	private final RemoteCqlClient cql;
	private final Repository repository;

	public CohorteEvaluation(RemoteCqlClient cql, Repository repository) {
		this.cql = cql;
		this.repository = repository;
	}

	/**
	 * Evaluates the EvidenceVariable for each subject and returns a Group of included members.
	 * Now supports: nested combinations, exclude=true, XOR extension, canonical EV references.
	 *
	 * @param study             {@link ResearchStudy} used for Group metadata (id/name/description)
	 * @param evidenceVariable  root EvidenceVariable to evaluate (can be composite)
	 * @param subjects          list of subject references (e.g., {@code Patient/{id}})
	 * @param baseParams        base {@link Parameters} passed to the remote engine; may include endpoints or other flags
	 * @param fallbackLibraryId library id used
	 * @return a {@link Group} with included members
	 * @throws InvalidRequestException      on invalid EV authoring (e.g., missing expression name)
	 * @throws ResourceNotFoundException    when a canonical EV cannot be resolved
	 * @throws UnprocessableEntityException when the remote result cannot be interpreted as boolean
	 */
	public Group evaluate(
		ResearchStudy study,
		EvidenceVariable evidenceVariable,
		List<String> subjects,
		Parameters baseParams,
		String fallbackLibraryId) {

		Group group = new Group();
		group.setType(Group.GroupType.PERSON).setActive(true);
		group.setId("group-" + study.getIdElement().getIdPart());
		group.setName("Patient Eligible for: " + study.getName());
		group.setDescription(study.getDescription());

		for (String subjectId : subjects) {
			if (evaluateEvidenceVariable(evidenceVariable, subjectId, baseParams, fallbackLibraryId)) {
				List<Identifier> patientIdent = repository.read(Patient.class, new IdType(subjectId)).getIdentifier();
				if (!patientIdent.isEmpty())
					group.addMember().setEntity(new Reference().setIdentifier(pseudonymizeIdentifier(patientIdent.get(0))));
			}
		}
		return group;
	}

	/**
	 * Recursively evaluates an {@link EvidenceVariable}.
	 *
	 * @param variable          current EvidenceVariable node
	 * @param subjectId         subject reference
	 * @param baseParams        base parameters to pass to the remote engine and extended with expression parameters
	 * @param fallbackLibraryId fallback library id.
	 * @return {@code true} if the EV holds for the subject
	 */
	private boolean evaluateEvidenceVariable(
		EvidenceVariable variable,
		String subjectId,
		Parameters baseParams,
		String fallbackLibraryId) {

		if (variable.getCharacteristic().isEmpty()) return true; // vacuous truth

		if (variable.getCharacteristic().size() == 1) {
			return evalCharacteristic(variable, variable.getCharacteristic().get(0), subjectId, baseParams, fallbackLibraryId);
		}

		boolean all = true;
		for (EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic : variable.getCharacteristic()) {
			boolean r = evalCharacteristic(variable, characteristic, subjectId, baseParams, fallbackLibraryId);
			all = all && r;
			if (!all) break;
		}
		return all;
	}

	/**
	 * Evaluates a single characteristic node.
	 *
	 * @param contextEV         parent EV (used for library resolution and context in error messages)
	 * @param characteristic    characteristic to evaluate
	 * @param subjectId         subject reference
	 * @param baseParams        base parameters to augment and forward to the remote engine
	 * @param fallbackLibraryId fallback library id if no {@code cqf-library} is present
	 * @return {@code true} if this node evaluates to true
	 * @throws InvalidRequestException       when an expression name is missing or definitionCanonical is blank
	 * @throws UnsupportedOperationException when a {@code characteristic.definition[x]} type is not supported yet
	 * @throws ResourceNotFoundException     when a referenced EV canonical cannot be resolved
	 * @throws UnprocessableEntityException  when the remote result for an expression is not boolean
	 */
	private boolean evalCharacteristic(
		EvidenceVariable contextEV,
		EvidenceVariable.EvidenceVariableCharacteristicComponent characteristic,
		String subjectId,
		Parameters baseParams,
		String fallbackLibraryId) {

		boolean result;

		if (characteristic.hasDefinitionByCombination()) {
			EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent comb = characteristic.getDefinitionByCombination();

			LogicOp op = mapOp(comb.getCode());
			boolean xor = hasXorExtension(comb);
			if (xor) op = LogicOp.XOR;

			List<Boolean> childResults = new ArrayList<>();
			for (EvidenceVariable.EvidenceVariableCharacteristicComponent nested : comb.getCharacteristic()) {
				childResults.add(evalCharacteristic(contextEV, nested, subjectId, baseParams, fallbackLibraryId));
			}
			result = reduce(childResults, op);

		} else if (characteristic.hasDefinitionExpression()) {
			Expression expr = characteristic.getDefinitionExpression();
			if (expr.hasExtension(EXT_EV_PARAM)) {
				baseParams = addExpressionParameters(expr, baseParams);
			}
			String expressionName = safe(expr.getExpression());
			if (expressionName == null || expressionName.isBlank()) {
				throw new InvalidRequestException(String.format(
					"Expression is missing for EvidenceVariable '%s' (characteristic linkId='%s').",
					contextEV.getUrl(),
					characteristic.getLinkId())
				);
			} else {
				String libId = resolveLibraryId(contextEV, fallbackLibraryId);
				result = evalBooleanExpression(libId, expressionName, subjectId, baseParams);
			}

		} else if (characteristic.hasDefinitionCanonical()) {
			String canonical = safe(characteristic.getDefinitionCanonical());
			EvidenceVariable nestedEv = resolveEvidenceVariable(canonical);
			result = evaluateEvidenceVariable(nestedEv, subjectId, baseParams, fallbackLibraryId);
		} else {
			throw new UnsupportedOperationException(String.format(
				"This type of 'characteristic.definition[x]' is not supported yet for EvidenceVariable '%s' "
					+ "(characteristic linkId='%s'). Supported: definitionExpression, definitionCanonical, definitionByCombination.",
				contextEV,
				characteristic.getLinkId()
			));
		}

		if (characteristic.getExclude()) {
			result = !result;
		}
		return result;
	}

	/**
	 * Invokes the remote CQL engine for a boolean expression.
	 *
	 * @param libraryId      resolved Library id
	 * @param expressionName CQL identifier to evaluate
	 * @param subjectId      subject reference (e.g., {@code Patient/123})
	 * @param baseParams     base parameters (possibly already containing endpoints and expression parameters)
	 * @return boolean outcome of the expression
	 * @throws UnprocessableEntityException when the returned value is not boolean or the parameter is missing
	 */
	private boolean evalBooleanExpression(String libraryId, String expressionName, String subjectId, Parameters baseParams) {
		Parameters evaluateParam = cloneParams(baseParams);

		evaluateParam.addParameter().setName("subject").setValue(new StringType(subjectId));
		Parameters out = cql.evaluateLibrary(evaluateParam, libraryId);
		return readBoolean(out, expressionName);
	}

	/**
	 * Resolves the Library id to use for an expression
	 *
	 * @param evidenceVariable parent EV if not present on the node
	 * @param fallback         fallback library id
	 * @return a concrete library id to call
	 */
	private String resolveLibraryId(
		EvidenceVariable evidenceVariable,
		String fallback) {

		String canonical = readCqfLibraryCanonical(evidenceVariable);
		if (canonical == null) return fallback;

		try {
			Bundle b = repository.search(Bundle.class, Library.class, Searches.byCanonical(canonical), null);
			if (b.hasEntry() && b.getEntryFirstRep().hasResource()) {
				Resource r = b.getEntryFirstRep().getResource();
				if (r instanceof Library lib && lib.hasId()) {
					return lib.getIdElement().getIdPart();
				}
			}
		} catch (Exception ignore) {
		}

		String tail = tailId(canonical);
		return tail != null ? tail : fallback;
	}

	/**
	 * Resolves an {@link EvidenceVariable} by canonical URL (optionally versioned).
	 * This implementation uses a repository search by canonical and returns the first entry.
	 *
	 * @param canonical canonical URL, possibly with a {@code |version} suffix
	 * @return the resolved EvidenceVariable
	 * @throws ResourceNotFoundException if no matching EV is found
	 */

	private EvidenceVariable resolveEvidenceVariable(String canonical) {
		Bundle bundle = repository.search(Bundle.class, EvidenceVariable.class, Searches.byCanonical(canonical), null);
		if (bundle.hasEntry() && bundle.getEntryFirstRep().hasResource()) {
			return (EvidenceVariable) bundle.getEntryFirstRep().getResource();
		} else {
			throw new ResourceNotFoundException(
				String.format("EvidenceVariable resource with canonical '%s' was not found", canonical)
			);
		}
	}

	/**
	 * Maps FHIR combination code to an internal logical operator.
	 * Defaults to AND when code is null or unrecognized.
	 *
	 * @param code {@link EvidenceVariable.CharacteristicCombination} code (ANYOF → OR, otherwise AND)
	 * @return internal {@link LogicOp}
	 */
	private LogicOp mapOp(EvidenceVariable.CharacteristicCombination code) {
		if (code == EvidenceVariable.CharacteristicCombination.ANYOF) {
			return LogicOp.OR;
		}
		return LogicOp.AND;
	}

	/**
	 * Reduces a list of boolean values using the specified logical operator.
	 *
	 * @param values list of boolean operands
	 * @param op     operator (AND/OR/XOR)
	 * @return reduction result
	 */
	private boolean reduce(List<Boolean> values, LogicOp op) {
		switch (op) {
			case OR -> {
				return values.stream().anyMatch(Boolean::booleanValue);
			}
			case XOR -> {
				return values.stream().filter(Boolean::booleanValue).count() == 1;
			}
			default -> {
				return values.stream().allMatch(Boolean::booleanValue);
			}
		}
	}

	/**
	 * Checks whether a combination element carries the XOR extension with a true boolean value.
	 *
	 * @param extension element that may carry extensions
	 * @return true if the XOR extension is present and true
	 */
	private boolean hasXorExtension(IBaseHasExtensions extension) {
		if (!(extension instanceof Element e)) return false;
		for (Extension ext : e.getExtension()) {
			if (EXT_XOR.equals(ext.getUrl()) && ext.getValue() instanceof BooleanType bt) {
				return bt.booleanValue();
			}
		}
		return false;
	}

	/**
	 * Reads the canonical value from a {@code cqf-library} extension if present.
	 *
	 * @param evidenceVariable evidence variable
	 * @return canonical URL string or null
	 */
	private String readCqfLibraryCanonical(EvidenceVariable evidenceVariable) {
		Extension extension = evidenceVariable.getExtensionByUrl("http://hl7.org/fhir/StructureDefinition/cqf-library");
		if (extension != null) {
			return extension.getValueCanonicalType().getValue();
		}
		return null;
	}

	/**
	 * Creates a deep copy of {@link Parameters}.
	 *
	 * @param p source parameters (may be null)
	 * @return a non-null copy (empty if source is null)
	 */
	private Parameters cloneParams(Parameters p) {
		if (p == null) return new Parameters();
		return p.copy();
	}

	/**
	 * Collects expression parameterization from {@value EXT_EV_PARAM} extensions and attaches
	 * them under a nested {@code parameters} resource in the evaluation parameters.
	 *
	 * @param expression the Expression potentially carrying parameterization extensions
	 * @param baseParam  base Parameters to clone and augment
	 * @return new Parameters containing a nested {@code Parameters} resource under parameter name {@code "parameters"}
	 * @throws InvalidRequestException if {@code name} or {@code value[x]} is missing
	 */
	private Parameters addExpressionParameters(Expression expression, Parameters baseParam) {
		Parameters evalParam = cloneParams(baseParam);
		Parameters expressionParam = new Parameters();

		for (Extension extension : expression.getExtension()) {
			if (extension.getUrl().equals(EXT_EV_PARAM)) {
				Extension nameExtension = extension.getExtensionByUrl(SUB_NAME);
				Extension valueExtension = extension.getExtensionByUrl(SUB_VALUE);

				if(!nameExtension.isEmpty() && nameExtension.hasValue() &&
					!valueExtension.isEmpty() && valueExtension.hasValue()) {
					String name = ((StringType) nameExtension.getValue()).getValue();
					DataType value = valueExtension.getValue();
					expressionParam.addParameter(name, value);
				}
			}
		}
		evalParam.addParameter().setName("parameters").setResource(expressionParam);
		return evalParam;
	}

	/**
	 * Reads a boolean parameter from the evaluate CQL {@link Parameters} result.
	 *
	 * @param out  evaluate response
	 * @param name parameter name to read
	 * @return boolean value
	 * @throws UnprocessableEntityException if the parameter is missing or not a boolean
	 */
	private boolean readBoolean(Parameters out, String name) {
		if (out == null) {
			throw new UnprocessableEntityException("Remote $evaluate returned null Parameters (expected a boolean parameter named '" + name + "').");
		}

		if (out.getParameter("evaluation error") != null) {
			OperationOutcome oo = (OperationOutcome) out.getParameter("evaluation error").getResource();
			String text = oo.getIssue().get(0).getDetails().getText();
			oo.getIssue().get(0).getDetails().setText("expression" + "'" + name + "' evaluation error: " + text);
			throw new UnprocessableEntityException(oo);
		}
		Parameters.ParametersParameterComponent param = out.getParameter(name);
		DataType value = param.hasValue() ? param.getValue() : null;
		if (value instanceof BooleanType) {
			return ((BooleanType) value).booleanValue();
		} else {
			if (value != null) {
				throw new UnprocessableEntityException("Remote $evaluate parameter '" + name + "' has type " + value.fhirType() + " (expected boolean).");
			} else {
				throw new UnprocessableEntityException("$evaluate parameter '" + name + " is null (expected boolean).");
			}
		}
	}

	/**
	 * Strips the resource type prefix from a reference string.
	 *
	 * @param ref reference string
	 * @return id part, or null if {@code ref} is null
	 */
	private String stripPrefix(String ref) {
		if (ref == null) return null;
		int slash = ref.lastIndexOf('/');
		return (slash >= 0) ? ref.substring(slash + 1) : ref;
	}

	/**
	 * Extracts the terminal id from a canonical URL, ignoring any version suffix.
	 *
	 * @param canonical canonical URL with optional
	 * @return id tail or null
	 */
	private String tailId(String canonical) {
		if (canonical == null) return null;
		// Remove version
		String noVer = canonical.split("\\|")[0];
		int slash = noVer.lastIndexOf('/');
		return (slash >= 0) ? noVer.substring(slash + 1) : noVer;
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
			Identifier copy = new Identifier();
			copy.setSystem(original.getSystem());
			copy.setValue(encrypted);
			return copy;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns {@code null} if the input is blank; otherwise the input string.
	 *
	 * @param s input
	 * @return {@code null} if blank, else {@code s}
	 */
	private String safe(String s) {
		return (s == null || s.isBlank()) ? null : s;
	}

	/**
	 * Internal logical operator used to reduce child results.
	 */
	private enum LogicOp {AND, OR, XOR}
}
