package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.r5.model.*;
import org.hl7.fhir.r5.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r5.model.Bundle.HTTPVerb;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.search.Searches;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;

import java.util.*;

public class StudyInstantiateServiceImpl implements ca.uhn.fhir.jpa.starter.cohort.service.r5.impl.StudyInstantiateService {

	/**
	 * Base repository (typically the local repository) used to obtain {@link FhirContext}
	 */
	private final Repository myRepository;

	/**
	 * Service that handles EvidenceVariable collection and instantiation.
	 */
	private final EvidenceVariableService evidenceVariableService = new EvidenceVariableService();

	/**
	 * Creates a new {@link StudyInstantiateServiceImpl}.
	 *
	 * @param repository base {@link Repository}.
	 */
	public StudyInstantiateServiceImpl(Repository repository) {
		this.myRepository = Objects.requireNonNull(repository);
	}

	/**
	 * Instantiates a {@link ResearchStudy} from its canonical URL against a target
	 * knowledge endpoint.
	 *
	 * @param studyUrl              the canonical URL of the {@link ResearchStudy} definition
	 * @param researchStudyEndpoint the target endpoint where the transaction will be posted
	 * @return {@link Parameters} containing:
	 *         <ul>
	 *           <li><b>studyInstanceUrl</b> – the canonical URL of the created study instance</li>
	 *           <li><b>transactionBundle</b> – the server response bundle</li>
	 *         </ul>
	 * @throws InvalidRequestException       if a required parameter is missing or blank
	 * @throws ResourceNotFoundException     if no {@link ResearchStudy} is found for the given canonical URL
	 * @throws UnprocessableEntityException  if the search or the transaction fails
	 */
	@Override
	public Parameters instantiateStudy(CanonicalType studyUrl, Endpoint researchStudyEndpoint) {
		// 1. Validate inputs
		if (studyUrl == null || studyUrl.getValue() == null || studyUrl.getValue().isBlank()) {
			throw new InvalidRequestException("Missing required parameter 'studyUrl'");
		}
		if (researchStudyEndpoint == null || researchStudyEndpoint.getAddress() == null || researchStudyEndpoint.getAddress().isBlank()) {
			throw new InvalidRequestException("Missing required parameter 'researchStudyEndpoint'");
		}

		// 2. Create a repository proxy to the knowledge endpoint
		Repository researchRepo = Repositories.proxy(myRepository, false, null, researchStudyEndpoint, null);

		// 3. Resolve ResearchStudy definition by canonical search
		Bundle studyBundle;
		try {
			studyBundle = researchRepo.search(Bundle.class, ResearchStudy.class, Searches.byCanonical(studyUrl.getCanonical()), null);
		} catch (Exception e) {
			throw new UnprocessableEntityException("Failed to search ResearchStudy by canonical: " + e.getMessage());
		}
		if (studyBundle.getEntry().isEmpty()) {
			throw new ResourceNotFoundException("ResearchStudy with canonical '" + studyUrl.getValue() + "' not found");
		}
		ResearchStudy definition = (ResearchStudy) studyBundle.getEntry().get(0).getResource();

		// 4. Create instances for each EV definition
		Map<String, EvidenceVariable> variableInstanceMap = new LinkedHashMap<>();
		Map<String, String> variableUrlMap = new LinkedHashMap<>();
		evidenceVariableService.createEvidenceVariableInstances(definition, researchRepo, variableInstanceMap, variableUrlMap);

		// 5. Prepare ResearchStudy instance
		ResearchStudy instance = createResearchStudyInstance(definition, studyUrl, variableUrlMap);

		// 6. Build transaction bundle
		Bundle transaction = buildTransaction(variableInstanceMap, variableUrlMap, instance);

		// 7. Execute transaction
		Bundle transactionResponse;
		try {
			FhirContext context = myRepository.fhirContext();
			IGenericClient client = context.newRestfulGenericClient(researchStudyEndpoint.getAddress());
			transactionResponse = client.transaction().withBundle(transaction).execute();
		} catch (Exception e) {
			throw new UnprocessableEntityException("Failed to post transaction bundle: " + e.getMessage());
		}

		// 8. Assemble output Parameters
		Parameters out = new Parameters();
		out.addParameter().setName("studyInstanceUrl").setValue(new CanonicalType(instance.getUrl()));
		out.addParameter().setName("transactionBundle").setResource(transactionResponse);
		return out;
	}

	/**
	 * Creates a {@link ResearchStudy} instance (clone of the definition) and rewrites
	 * its references to point to EV instances.
	 *
	 * @param studyDefinition the {@link ResearchStudy} definition
	 * @param studyUrl        the canonical URL of the definition
	 * @param evFullUrlMap    map of EV definition keys → instance fullUrl
	 * @return the prepared {@link ResearchStudy} instance
	 */
	private ResearchStudy createResearchStudyInstance(ResearchStudy studyDefinition, CanonicalType studyUrl, Map<String, String> evFullUrlMap) {
		String studyId = UUID.randomUUID().toString();

		ResearchStudy instance = studyDefinition.copy();
		instance.setId(studyId);
		String instanceCanonical = studyUrl.getValue() + "-instance-" + studyId;
		instance.setUrl(instanceCanonical);
		CodeableConcept phase = new CodeableConcept();
		phase.addCoding().setSystem(ResearchStudyUtils.CUSTOM_PHASE_SYSTEM).setCode("initial");
		instance.setPhase(phase);

		RelatedArtifact relatedArtifact = new RelatedArtifact();
		relatedArtifact.setType(RelatedArtifact.RelatedArtifactType.DERIVEDFROM);
		relatedArtifact.setResource(studyUrl.getValue());
		instance.addRelatedArtifact(relatedArtifact);

		updateEligibilityReferences(instance, evFullUrlMap);
		updateExtensionReferences(instance, evFullUrlMap);
		return instance;
	}

	/**
	 * Updates {@code recruitment.eligibility} to reference the EV instance
	 * fullUrl when it points to an EV definition known in {@code variableUrlMap}.
	 *
	 * @param instance        the {@link ResearchStudy} instance to update
	 * @param variableUrlMap  map of EV definition keys → EV instance fullUrl
	 */
	private void updateEligibilityReferences(ResearchStudy instance, Map<String, String> variableUrlMap) {
		if (instance.hasRecruitment() && instance.getRecruitment().hasEligibility()) {
			Reference eligibility = instance.getRecruitment().getEligibility();
			String key = getReferenceKey(eligibility);
			if (key != null && variableUrlMap.containsKey(key)) {
				eligibility.setReference(variableUrlMap.get(key));
			}
		}
	}

	/**
	 * Rewrites extension references on the study instance
	 *
	 * @param instance        the {@link ResearchStudy} instance to update
	 * @param variableUrlMap  map of EV definition keys → EV instance fullUrl (urn:uuid:...)
	 */
	private void updateExtensionReferences(ResearchStudy instance, Map<String, String> variableUrlMap) {
		for (Extension extension : instance.getExtension()) {
			for (Extension subExtension : extension.getExtension()) {
				if ((subExtension.getUrl().equalsIgnoreCase("variable")) && (subExtension.getValue() instanceof Reference ref)) {
					String key = getReferenceKey(ref);
					if (key != null && variableUrlMap.containsKey(key)) {
						ref.setReference(variableUrlMap.get(key));
					}
				} else if ((subExtension.getUrl().equalsIgnoreCase("evaluation")) && (subExtension.getValue() instanceof Reference)) {
					subExtension.setValue(null);
				}
			}
		}
	}

	/**
	 * Builds a transaction {@link Bundle} containing
	 *
	 * @param evidenceVariableMap map of EV definition keys → EV instances to persist
	 * @param evidenceVariableUrl map of EV definition keys → EV instance fullUrl (urn:uuid:...) used as bundle fullUrls
	 * @param studyInstance       the {@link ResearchStudy} instance to persist
	 * @return a transaction {@link Bundle} ready to POST
	 */
	private Bundle buildTransaction(Map<String, EvidenceVariable> evidenceVariableMap, Map<String, String> evidenceVariableUrl, ResearchStudy studyInstance) {

		Bundle transaction = new Bundle();
		transaction.setType(Bundle.BundleType.TRANSACTION);

		// EvidenceVariable instances
		for (Map.Entry<String, EvidenceVariable> entry : evidenceVariableMap.entrySet()) {
			EvidenceVariable instanceVariable = entry.getValue();
			String instanceUrl = evidenceVariableUrl.get(entry.getKey());
			BundleEntryComponent bundleEntry = transaction.addEntry();
			bundleEntry.setFullUrl(instanceUrl);
			bundleEntry.setResource(instanceVariable);
			bundleEntry.getRequest().setMethod(HTTPVerb.POST).setUrl(ResourceType.EvidenceVariable.name());
		}

		// ResearchStudy instance
		BundleEntryComponent studyInstanceEntry = transaction.addEntry();
		studyInstanceEntry.setFullUrl("urn:uuid:" + studyInstance.getIdPart());
		studyInstanceEntry.setResource(studyInstance);
		studyInstanceEntry.getRequest().setMethod(HTTPVerb.POST).setUrl(ResourceType.ResearchStudy.name());

		return transaction;
	}

	/**
	 * Computes a stable lookup key for a {@link Reference}.
	 *
	 * @param reference the {@link Reference} to transform into a key
	 * @return a stable key suitable to match entries in the EV maps, or {@code null} if not resolvable
	 */
	private String getReferenceKey(Reference reference) {
		if (reference == null || !reference.hasReference()) return null;
		String referenceValue = reference.getReference();
		if (referenceValue == null) return null;

		if (referenceValue.startsWith("http://") || referenceValue.startsWith("https://")) {
			String withoutVersion = referenceValue.split("\\|")[0];
			int slash = withoutVersion.lastIndexOf('/');
			return slash >= 0 ? withoutVersion.substring(slash + 1) : withoutVersion;
		}
		IdType id = new IdType(referenceValue);
		return id.toUnqualifiedVersionless().getValue();
	}
}
