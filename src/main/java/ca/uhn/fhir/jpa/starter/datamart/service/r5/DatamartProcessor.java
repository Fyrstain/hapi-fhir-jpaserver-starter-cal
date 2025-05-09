package ca.uhn.fhir.jpa.starter.datamart.service.r5;

import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.cql.VersionedIdentifiers;
import org.opencds.cqf.fhir.utility.search.Searches;

public class DatamartProcessor {
	private final Repository repository;
	private final RemoteCqlClient cqlClient;

	public DatamartProcessor(Repository repository, RemoteCqlClient cqlClient) {
		this.repository = repository;
		this.cqlClient = cqlClient;
	}

	/**
	 * Generate a Datamart for a given {@link ResearchStudy} and populates the provided {@link ListResource}
	 * with datamart information.
	 *
	 * @param researchStudy The {@link ResearchStudy} used as the basis for datamart generation.
	 * @return The generated {@link ListResource} containing datamart information.
	 * @throws IllegalArgumentException  if the {@link EvidenceVariable} lacks a valid library reference.
	 * @throws ResourceNotFoundException if no {@link Library} resource can be found using the extracted library URL.
	 * @throws IllegalStateException     if the CQL/ELM library cannot be resolved or loaded due to a CQL include exception.
	 */
	public ListResource generateDatamart(
		ResearchStudy researchStudy, Parameters evaluateParams
	) {
		Group eligibleGroup = ResearchStudyUtils.getEligibleGroup(researchStudy, repository);
		EvidenceVariable evidenceVariable = ResearchStudyUtils.getEvidenceVariable(researchStudy, repository);
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

		var libId = VersionedIdentifiers.forUrl(libraryUrl).getId();
        /*var context = Engines.forRepository(this.repository, this.settings.getEvaluationSettings(), null);

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
        context.getState().init(lib.getLibrary());*/
		var subject = ResearchStudyUtils.getSubjectReferences(eligibleGroup, repository);
		DatamartGeneration datamartGeneration = new DatamartGeneration(cqlClient, repository);
		return datamartGeneration.generateDatamart(researchStudy, evidenceVariable, subject, evaluateParams, libId);
	}
}