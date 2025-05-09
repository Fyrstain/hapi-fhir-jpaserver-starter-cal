package ca.uhn.fhir.jpa.starter.datamart.service.r5;

import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.Repositories;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.search.Searches;

import java.util.Objects;

public class DatamartService implements DatamartServiceImpl {


	private final Repository repository;
	private final DatamartEvaluationOptions settings;

	public DatamartService(Repository repository, DatamartEvaluationOptions settings) {
		this.settings = settings;
		this.repository = Objects.requireNonNull(repository);
	}

	/**
	 * Generates a Datamart ListResource for the referenced ResearchStudy.
	 *
	 * @param researchStudyUrl      The canonical URL of the ResearchStudy for datamart generation.
	 * @param researchStudyEndpoint The endpoint containing the ResearchStudy, inclusion variables,
	 *                              and CQL libraries defining research variables.
	 * @param dataEndpoint          Endpoint to access data referenced by the retrieval operations in the library.
	 * @param terminologyEndpoint   (Optional) Endpoint to access terminology (ValueSets, CodeSystems) referenced by the library.
	 * @return The generated ListResource representing the Datamart.
	 */
	public ListResource generateDatamart(
		CanonicalType researchStudyUrl,
		Endpoint researchStudyEndpoint,
		Endpoint dataEndpoint,
		Endpoint terminologyEndpoint,
		Endpoint cqlEngineEndpoint
	) {
		Repository repo = Repositories.proxy(repository, false, dataEndpoint, researchStudyEndpoint, terminologyEndpoint);
		Bundle b = repo.search(Bundle.class, ResearchStudy.class, Searches.byCanonical(researchStudyUrl.getCanonical()), null);
		if (b.getEntry().isEmpty()) {
			var errorMsg = String.format("Unable to find ResearchStudy with url: %s", researchStudyUrl.getCanonical());
			throw new ResourceNotFoundException(errorMsg);
		}
		ResearchStudy researchStudy = (ResearchStudy) b.getEntry().get(0).getResource();
		if (Objects.equals(
			researchStudy.getPhase().getCode(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM), ResearchStudyUtils.INITIAL_PHASE)) {
			var errorMsg = String.format("A cohorting is needed before generating the datamart for ResearchStudy with url: %s", researchStudyUrl);
			throw new ResourceNotFoundException(errorMsg);
		}

		Parameters evaluateParams = new Parameters();
		evaluateParams.addParameter()
			.setName("dataEndpoint")
			.setResource(dataEndpoint);
		evaluateParams.addParameter()
			.setName("contentEndpoint")
			.setResource(researchStudyEndpoint);
		evaluateParams.addParameter()
			.setName("terminologyEndpoint")
			.setResource(terminologyEndpoint);

		RemoteCqlClient cqlClient = new RemoteCqlClient(cqlEngineEndpoint, repo);

		DatamartProcessor datamartProcessor = new DatamartProcessor(repo, cqlClient);
		ListResource list = datamartProcessor.generateDatamart(researchStudy, evaluateParams);
		updateResearchStudyWithList(repo, researchStudy, list);
		return list;
	}

	/**
	 * Updates the study phase and adds an evaluation extension to reference
	 * the generated ListResource containing datamart information.
	 *
	 * @param repo          The repository to use for updates.
	 * @param researchStudy The ResearchStudy to modify.
	 * @param listResource  The ListResource.
	 */
	public void updateResearchStudyWithList(Repository repo, ResearchStudy researchStudy, ListResource listResource) {
		Extension listReference = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).getExtensionByUrl("evaluation");
		if (listReference != null) {
			listResource.setId(listReference.getValueReference().getReferenceElement().getIdPart());
			repo.update(listResource);
		} else {
			MethodOutcome outcome = repo.create(listResource);
			researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).addExtension()
				.setUrl("evaluation")
				.setValue(new Reference(String.format("%s/%s", outcome.getId().getResourceType(), outcome.getId().getIdPart())));
			CodeableConcept phase = new CodeableConcept();
			phase.addCoding()
				.setCode(ResearchStudyUtils.POST_DATAMART)
				.setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM);
			researchStudy.setPhase(phase);

			repo.update(researchStudy);
		}
	}
}
