package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.cql.VersionedIdentifiers;
import org.opencds.cqf.fhir.utility.search.Searches;

import java.util.List;
import java.util.stream.Collectors;

public class CohorteProcessor {
	private final Repository repository;
	private final ca.uhn.fhir.jpa.starter.common.RemoteCqlClient cqlClient;
	private final RepositorySubjectProvider subjectProvider;


	public CohorteProcessor(Repository repository, RemoteCqlClient cqlClient, RepositorySubjectProvider subjectProvider) {
		this.repository = repository;
		this.cqlClient = cqlClient;
		this.subjectProvider = subjectProvider;
	}

	/**
	 * Processes the cohort for a given {@link ResearchStudy} and populates the provided {@link Group}
	 * with eligible subjects.
	 *
	 * @param researchStudy The {@link ResearchStudy} used as the basis for cohorting.
	 * @return The updated {@link Group} containing eligible subjects based on the cohort evaluation.
	 * @throws IllegalArgumentException  if the {@link ResearchStudy} does not have a valid eligibility variable or if the
	 *                                   associated {@link EvidenceVariable} lacks a valid library reference.
	 * @throws ResourceNotFoundException if no {@link Library} resource can be found using the extracted library URL.
	 * @throws IllegalStateException     if the CQL/ELM library cannot be resolved or loaded due to a CQL include exception.
	 */
	public Group cohorting(
		ResearchStudy researchStudy, Parameters evaluateParams
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
		Extension cqfExtension = evidenceVariable.getExtensionByUrl("http://hl7.org/fhir/StructureDefinition/cqf-library");

		String libId = null;
		if (cqfExtension != null) {
			String libraryUrl = cqfExtension.getValueCanonicalType().getValue();
			Bundle b = this.repository.search(Bundle.class, Library.class, Searches.byCanonical(libraryUrl), null);
			if (b.getEntry().isEmpty()) {
				var errorMsg = String.format("Unable to find Library with url: %s", libraryUrl);
				throw new ResourceNotFoundException(errorMsg);
			}

			libId = VersionedIdentifiers.forUrl(libraryUrl).getId();
		}

		var subjects =
			subjectProvider.getSubjects(repository, (List<String>) null).collect(Collectors.toList());

		CohorteEvaluation cohorteEvaluation = new CohorteEvaluation(cqlClient, repository);
		return cohorteEvaluation.evaluate(researchStudy, evidenceVariable, subjects, evaluateParams, libId);
	}
}
