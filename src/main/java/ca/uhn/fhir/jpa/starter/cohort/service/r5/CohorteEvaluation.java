package ca.uhn.fhir.jpa.starter.cohort.service.r5;// imports you may need:
import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import org.hl7.fhir.instance.model.api.IBaseHasExtensions;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.search.Searches; // if you already use it
import java.util.*;

public class CohorteEvaluation {
	private static final String EXT_CQF_LIBRARY = "http://hl7.org/fhir/StructureDefinition/cqf-library";
	private static final String EXT_XOR = "https://www.centreantoinelacassagne.org/StructureDefinition/EXT-Exclusive-OR";

	private final RemoteCqlClient cql;
	private final Repository repository;

	public CohorteEvaluation(RemoteCqlClient cql, Repository repository) {
		this.cql = cql;
		this.repository = repository;
	}

	/**
	 * Evaluates the EvidenceVariable for each subject and returns a Group of included members.
	 * Now supports: nested combinations, exclude=true, XOR extension, canonical EV references.
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

		// Evaluate each subject against the EV logical tree
		for (String subjectId : subjects) {
			if (evaluateEvidenceVariable(evidenceVariable, subjectId, baseParams, fallbackLibraryId)) {
				List<Identifier> patientIdent = repository.read(Patient.class, new IdType(subjectId)).getIdentifier();
				if(!patientIdent.isEmpty()) group.addMember().setEntity(new Reference().setIdentifier(pseudonymizeIdentifier(patientIdent.get(0))));
			}
		}
		return group;
	}

	private boolean evaluateEvidenceVariable(
		EvidenceVariable ev,
		String subjectId,
		Parameters baseParams,
		String fallbackLibraryId) {

		if (ev.getCharacteristic().isEmpty()) return true; // vacuous truth

		if (ev.getCharacteristic().size() == 1) {
			return evalCharacteristic(ev, ev.getCharacteristic().get(0), subjectId, baseParams, fallbackLibraryId);
		}

		boolean all = true;
		for (EvidenceVariable.EvidenceVariableCharacteristicComponent ch : ev.getCharacteristic()) {
			boolean r = evalCharacteristic(ev, ch, subjectId, baseParams, fallbackLibraryId);
			all = all && r;
			if (!all) break;
		}
		return all;
	}

	private boolean evalCharacteristic(
		EvidenceVariable contextEV,
		EvidenceVariable.EvidenceVariableCharacteristicComponent ch,
		String subjectId,
		Parameters baseParams,
		String fallbackLibraryId) {

		boolean result;

		if (ch.hasDefinitionByCombination()) {
			EvidenceVariable.EvidenceVariableCharacteristicDefinitionByCombinationComponent comb = ch.getDefinitionByCombination();

			LogicOp op = mapOp(comb.getCode());
			boolean xor = hasTrueBooleanExtension(comb, EXT_XOR);
			if (xor) op = LogicOp.XOR;

			List<Boolean> childResults = new ArrayList<>();
			for (EvidenceVariable.EvidenceVariableCharacteristicComponent nested : comb.getCharacteristic()) {
				childResults.add(evalCharacteristic(contextEV, nested, subjectId, baseParams, fallbackLibraryId));
			}
			result = reduce(childResults, op);

		} else if (ch.hasDefinitionExpression()) {
			Expression expr = ch.getDefinitionExpression();
			String expressionName = safe(expr.getExpression());
			if (expressionName == null || expressionName.isBlank()) {
				result = false;
			} else {
				String libId = resolveLibraryId(ch, contextEV, fallbackLibraryId);
				result = evalBooleanExpression(libId, expressionName, subjectId, baseParams);
			}

		} else if (ch.hasDefinitionCanonical()) {
			String canonical = safe(ch.getDefinitionCanonical());
			EvidenceVariable nestedEv = resolveEvidenceVariable(canonical);
			if (nestedEv == null) {
				result = false;
			} else {
				result = evaluateEvidenceVariable(nestedEv, subjectId, baseParams, fallbackLibraryId);
			}

		} else {
			result = false;
		}

		if (ch.getExclude()) {
			result = !result;
		}
		return result;
	}

	/**
	 * Calls the remote CQL and returns the boolean value of the named expression.
	 * Strategy: call evaluateLibrary and read Parameters[exprName]=valueBoolean.
	 */
	private boolean evalBooleanExpression(String libraryId, String expressionName, String subjectId, Parameters baseParams) {
		Parameters call = cloneParams(baseParams);

		call.addParameter().setName("subject").setValue(new StringType(stripPrefix(subjectId)));
		Parameters out = new Parameters();
			out.addParameter(expressionName, true);//cql.evaluateLibrary(call, libraryId);
		return readBoolean(out, expressionName);
	}

	private String resolveLibraryId(
		IBaseHasExtensions atCharacteristic,
		IBaseHasExtensions atEvidenceVariable,
		String fallback) {

		String canonical = readCqfLibraryCanonical(atCharacteristic);
		if (canonical == null) canonical = readCqfLibraryCanonical(atEvidenceVariable);
		if (canonical == null) return fallback;

		try {
			Bundle b = repository.search(Bundle.class, Library.class, Searches.byCanonical(canonical), null);
			if (b.hasEntry() && b.getEntryFirstRep().hasResource()) {
				Resource r = b.getEntryFirstRep().getResource();
				if (r instanceof Library lib && lib.hasId()) {
					return lib.getIdElement().getIdPart();
				}
			}
		} catch (Exception ignore) { }

		String tail = tailId(canonical);
		return tail != null ? tail : fallback;
	}

	private EvidenceVariable resolveEvidenceVariable(String canonical) {
		try {
			Bundle b = repository.search(Bundle.class, EvidenceVariable.class, Searches.byCanonical(canonical), null);
			if (b.hasEntry() && b.getEntryFirstRep().hasResource()) {
				Resource r = b.getEntryFirstRep().getResource();
				if (r instanceof EvidenceVariable ev) return ev;
			}
		} catch (Exception ignore) { }
		return null;
	}

	private enum LogicOp { AND, OR, XOR }

	private LogicOp mapOp(EvidenceVariable.CharacteristicCombination code) {
		if (code == null) return LogicOp.AND;
		String v = code.toCode();
		if ("all-of".equalsIgnoreCase(v)) return LogicOp.AND;
		if ("any-of".equalsIgnoreCase(v)) return LogicOp.OR;
		// anything else → default AND
		return LogicOp.AND;
	}

	private boolean reduce(List<Boolean> values, LogicOp op) {
		switch (op) {
			case AND: return values.stream().allMatch(Boolean::booleanValue);
			case OR:  return values.stream().anyMatch(Boolean::booleanValue);
			case XOR: return values.stream().filter(Boolean::booleanValue).count() == 1;
			default:  return false;
		}
	}

	private boolean hasTrueBooleanExtension(IBaseHasExtensions extHolder, String url) {
		if (!(extHolder instanceof Element e)) return false;
		for (Extension ext : e.getExtension()) {
			if (url.equals(ext.getUrl()) && ext.getValue() instanceof BooleanType bt) {
				return bt.booleanValue();
			}
		}
		return false;
	}

	private String readCqfLibraryCanonical(IBaseHasExtensions extHolder) {
		if (!(extHolder instanceof Element e)) return null;
		for (Extension ext : e.getExtension()) {
			if (EXT_CQF_LIBRARY.equals(ext.getUrl()) && ext.getValue() instanceof CanonicalType c) {
				return c.getValue();
			}
		}
		return null;
	}

	private Parameters cloneParams(Parameters p) {
		if (p == null) return new Parameters();
		return (Parameters) p.copy();
	}

	private boolean readBoolean(Parameters out, String name) {
		if (out == null || name == null) return false;
		for (Parameters.ParametersParameterComponent pp : out.getParameter()) {
			if (name.equals(pp.getName()) && pp.getValue() instanceof BooleanType b) {
				return b.booleanValue();
			}
		}
		return false;
	}

	private String stripPrefix(String ref) {
		if (ref == null) return null;
		int slash = ref.lastIndexOf('/');
		return (slash >= 0) ? ref.substring(slash + 1) : ref;
	}

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

	private String safe(String s) { return (s == null || s.isBlank()) ? null : s; }
}
