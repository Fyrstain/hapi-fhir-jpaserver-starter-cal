package ca.uhn.fhir.jpa.starter.datamart.service.r5.service;

import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import ca.uhn.fhir.model.api.IQueryParameterType;
import ca.uhn.fhir.rest.param.NumberParam;
import ca.uhn.fhir.rest.param.TokenParam;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatamartTransformation {
	private static final Logger logger = LoggerFactory.getLogger(DatamartTransformation.class);
	private final Repository repository;

	public DatamartTransformation(Repository repository) {
		this.repository = repository;
	}

	/**
	 * Transforms a datamart Bundle applying the given StructureMap.
	 *
	 * @param study the ResearchStudy
	 * @param map   the StructureMap to apply
	 * @return a Binary resource containing the transformed data
	 */
	public Binary transform(ResearchStudy study, StructureMap map) {
		String listId = ResearchStudyUtils.getEvaluationListId(study);
		Bundle bundle = fetchDataMartBundle(listId);
		String serialized = repository.fhirContext().newJsonParser().encodeResourceToString(bundle);
		Binary input = new Binary();
		input.setContentType("application/json");
		input.setData(serialized.getBytes());

		Parameters inParams = new Parameters();
		inParams.addParameter().setName("structureMap").setResource(map);
		inParams.addParameter()
			.setName("input")
			.addPart()
			.setName("bundle")
			.setResource(input);

		Parameters outParams = repository.invoke("transform", inParams, Parameters.class, null);
		return (Binary) outParams.getParameterFirstRep().getResource();
	}

	/**
	 * Fetches a Bundle representing the datamart evaluation for a given ListResource ID.
	 *
	 * @param listId the ID of the ListResource containing datamart evaluation results
	 * @return a Bundle with the ListResource and included Parameters resources
	 */
	public Bundle fetchDataMartBundle(String listId) {
		Map<String, List<IQueryParameterType>> params = new HashMap<>();
		params.put("_count", Collections.singletonList(new NumberParam(1000)));
		params.put("_id", Collections.singletonList(new ca.uhn.fhir.rest.param.StringParam(listId)));

		Bundle listBundle = repository.search(Bundle.class, ListResource.class, params, null);

		ListResource listResource = null;
		for (Bundle.BundleEntryComponent e : listBundle.getEntry()) {
			if (e.getResource() instanceof ListResource) {
				listResource = (ListResource) e.getResource();
				break;
			}
		}

		if (listResource == null) {
			return new Bundle();
		}

		Bundle result = new Bundle();
		result.setType(Bundle.BundleType.COLLECTION);
		// ajoute le List au bundle de résultat
		// result.addEntry().setResource(listResource);

		for (ListResource.ListResourceEntryComponent entry : listResource.getEntry()) {
			Reference ref = entry.getItem();
			if (ref == null || ref.getReferenceElement() == null) {
				continue;
			}
			String resourceType = ref.getReferenceElement().getResourceType();
			String idPart = ref.getReferenceElement().getIdPart();
			if (resourceType == null || idPart == null) {
				continue;
			}

			@SuppressWarnings("unchecked")
			Class<? extends Resource> resourceClass =
				(Class<? extends Resource>) repository.fhirContext()
					.getResourceDefinition(resourceType)
					.getImplementingClass();

			Map<String, List<IQueryParameterType>> p2 = new HashMap<>();
			p2.put("_id", Collections.singletonList(new ca.uhn.fhir.rest.param.StringParam(idPart)));

			Bundle resBundle = repository.search(Bundle.class, resourceClass, p2, null);
			for (Bundle.BundleEntryComponent re : resBundle.getEntry()) {
				if (re.getResource() != null) {
					result.addEntry().setResource(re.getResource());
				}
			}
		}
		return result;
	}
}
