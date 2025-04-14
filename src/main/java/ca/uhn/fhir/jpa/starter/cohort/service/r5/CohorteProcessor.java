package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.cqframework.cql.cql2elm.CqlIncludeException;
import org.cqframework.cql.cql2elm.model.CompiledLibrary;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.cql.Engines;
import org.opencds.cqf.fhir.cql.VersionedIdentifiers;
import org.opencds.cqf.fhir.utility.search.Searches;

import java.util.List;
import java.util.stream.Collectors;

public class CohorteProcessor {
	private final Repository repository;
	private final CohorteEvaluationOptions settings;
	private final RepositorySubjectProvider subjectProvider;


	public CohorteProcessor(Repository repository, CohorteEvaluationOptions settings, RepositorySubjectProvider subjectProvider) {
		this.repository = repository;
		this.settings = settings;
		this.subjectProvider = subjectProvider;
	}

	/**
	 * Processes the cohort for a given {@link ResearchStudy} and populates the provided {@link Group}
	 * with eligible subjects.
	 *
	 * @param researchStudy The {@link ResearchStudy} used as the basis for cohorting.
	 * @return The updated {@link Group} containing eligible subjects based on the cohort evaluation.
	 * @throws IllegalArgumentException if the {@link ResearchStudy} does not have a valid eligibility variable or if the
	 *                                  associated {@link EvidenceVariable} lacks a valid library reference.
	 * @throws ResourceNotFoundException if no {@link Library} resource can be found using the extracted library URL.
	 * @throws IllegalStateException if the CQL/ELM library cannot be resolved or loaded due to a CQL include exception.
	 */
	protected Group cohorting(
		ResearchStudy researchStudy
	) {
		if (!researchStudy.getRecruitment().hasEligibility()) {
			throw new IllegalArgumentException(
				String.format("ResearchStudy %s does not have an eligibility variable specified", researchStudy.getUrl()));
		}

		Reference eligibilityReference = researchStudy.getRecruitment().getEligibility();
		if (eligibilityReference == null ||
			eligibilityReference.getReferenceElement() == null ||
			!eligibilityReference.getReferenceElement().getResourceType().equals("EvidenceVariable")) {
			throw new IllegalArgumentException(
				String.format("ResearchStudy %s does not have a valid eligibility reference to an EvidenceVariable", researchStudy.getUrl())
			);
		}
		EvidenceVariable evidenceVariable = repository.read(EvidenceVariable.class, new IdType(eligibilityReference.getReferenceElement().getIdPart()));
		String libraryUrl = evidenceVariable.getExtensionByUrl("http://hl7.org/fhir/StructureDefinition/cqf-library")
			.getValueCanonicalType().getValue();
		if (libraryUrl == null) {
			throw new IllegalArgumentException(
				String.format("EvidenceVariable %s does not have a valid library reference", evidenceVariable.getIdElement().getValue())
			);
		}

		Bundle b = this.repository.search(Bundle.class, Library.class, Searches.byCanonical(libraryUrl), null);
		if (b.getEntry().isEmpty()) {
			var errorMsg = String.format("Unable to find Library with url: %s", libraryUrl);
			throw new ResourceNotFoundException(errorMsg);
		}

		var id = VersionedIdentifiers.forUrl(libraryUrl);
		var context = Engines.forRepository(this.repository, this.settings.getEvaluationSettings(), null);

		CompiledLibrary lib;
		try {
			lib = context.getEnvironment().getLibraryManager().resolveLibrary(id);
		} catch (CqlIncludeException e) {
			throw new IllegalStateException(
				String.format(
					"Unable to load CQL/ELM for library: %s. Verify that the Library resource is available in your environment and has CQL/ELM content embedded.",
					id.getId()),
				e);
		}

		context.getState().init(lib.getLibrary());
		var subjects =
			subjectProvider.getSubjects(repository, (List<String>) null).collect(Collectors.toList());
		CohorteEvaluation cohorteEvaluation = new CohorteEvaluation(context);
		return cohorteEvaluation.evaluate(researchStudy, evidenceVariable, subjects);
	}
}
