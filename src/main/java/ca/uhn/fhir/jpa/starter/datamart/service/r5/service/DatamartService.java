package ca.uhn.fhir.jpa.starter.datamart.service.r5.service;

import ca.uhn.fhir.jpa.starter.common.RemoteCqlClient;
import ca.uhn.fhir.jpa.starter.datamart.service.DatamartEvaluationOptions;
import ca.uhn.fhir.jpa.starter.datamart.service.Repositories;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.jpa.starter.datamart.service.r5.impl.DatamartServiceImpl;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;

import org.hl7.fhir.r5.model.*;
import org.hl7.fhir.r5.model.Bundle.BundleEntryComponent;
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
	 * @throws ResourceNotFoundException if the ResearchStudy cannot be found or if cohorting has not been performed.
	 * @throws UnprocessableEntityException if an OperationOutcome is received while searching for the ResearchStudy.
	 */
	public ListResource generateDatamart(
		CanonicalType researchStudyUrl,
		Endpoint researchStudyEndpoint,
		Endpoint dataEndpoint,
		Endpoint terminologyEndpoint,
		Endpoint cqlEngineEndpoint
	) {
		Repository repo = Repositories.proxy(repository, false, dataEndpoint, researchStudyEndpoint, terminologyEndpoint);
		Bundle studyBundle;

		try {
			studyBundle = repo.search(Bundle.class, ResearchStudy.class, Searches.byCanonical(researchStudyUrl.getCanonical()), null);
		} catch (Exception e) {
			throw new UnprocessableEntityException("Failed to search ResearchStudy by canonical: " + e.getMessage());
		}

		if (studyBundle.getEntry().isEmpty()) {
			var errorMsg = String.format("Unable to find ResearchStudy with url: %s", researchStudyUrl.getCanonical());
			throw new ResourceNotFoundException(errorMsg);
		}

		ResearchStudy researchStudy = null;
		OperationOutcome operationOutcome = null;

		for (BundleEntryComponent entry : studyBundle.getEntry()) {
			if (entry == null || entry.getResource() == null) continue;
			if (entry.getResource() instanceof ResearchStudy) {
				researchStudy = (ResearchStudy) entry.getResource();
				break;
			} else if (entry.getResource() instanceof OperationOutcome) {
				operationOutcome = (OperationOutcome) entry.getResource();
			}
		}
		if (researchStudy == null) {
			if (operationOutcome != null) {
				String diag = null;
				if (operationOutcome.hasIssue() && operationOutcome.getIssueFirstRep().hasDiagnostics()) {
					diag = operationOutcome.getIssueFirstRep().getDiagnostics();
				} else if (operationOutcome.hasText() && operationOutcome.getText().hasDiv()) {
					diag = operationOutcome.getText().getDivAsString();
				}
				String message = "Server returned an OperationOutcome while resolving ResearchStudy";
				if (diag != null && !diag.isBlank()) {
					message += ": " + diag;
				}
				throw new UnprocessableEntityException(message);
			}
			throw new ResourceNotFoundException("ResearchStudy with canonical '" + researchStudyUrl.getValue() + "' not found");
		}

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
		return updateResearchStudyWithList(repo, researchStudy, list);
	}

	/**
	 * Updates the study phase and adds an evaluation extension to reference
	 * the generated ListResource containing datamart information.
	 *
	 * @param repo          The repository to use for updates.
	 * @param researchStudy The ResearchStudy to modify.
	 * @param listResource  The ListResource.
	 * @return the updated or create List resource
	 */
	public ListResource updateResearchStudyWithList(Repository repo, ResearchStudy researchStudy, ListResource listResource) {
		Extension listReference = researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).getExtensionByUrl("evaluation");
		CodeableConcept phase = new CodeableConcept();
		phase.addCoding()
			.setCode(ResearchStudyUtils.POST_DATAMART)
			.setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM);
		if (listReference != null) {
			listResource.setId(listReference.getValueReference().getReferenceElement().getIdPart());
			repo.update(listResource);
			researchStudy.setPhase(phase);

			repo.update(researchStudy);
			return listResource;
		} else {
			MethodOutcome outcome = repo.create(listResource);
			Reference reference;
			if (outcome.getId() != null) {
				reference = new Reference(outcome.getId());
			} else if (outcome.getResource() != null) {
				reference = new Reference(outcome.getResource().getIdElement());
			} else {
				throw new InternalErrorException(String.format("Cannot retrieve ID of created list for ResearchStudy %s",
					researchStudy.getId()));
			}
			researchStudy.getExtensionByUrl(ResearchStudyUtils.EXT_URL).addExtension()
				.setUrl("evaluation")
				.setValue(reference);
			researchStudy.setPhase(phase);

			repo.update(researchStudy);

			if (outcome.getResource() != null) {
				return (ListResource) outcome.getResource();
			} else if (outcome.getId() != null) {
				return (ListResource) listResource.setId(outcome.getId());
			} else {
				throw new InternalErrorException(String.format("Cannot retrieve ID of created list for ResearchStudy %s",
					researchStudy.getId()));
			}
		}
	}
}
