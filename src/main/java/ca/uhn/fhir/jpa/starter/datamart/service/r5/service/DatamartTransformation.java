package ca.uhn.fhir.jpa.starter.datamart.service.r5.service;

import ca.uhn.fhir.jpa.starter.datamart.service.r5.utils.ResearchStudyUtils;
import org.hl7.fhir.r5.model.*;
import org.opencds.cqf.fhir.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
		Bundle output = new Bundle();
		output.setType(Bundle.BundleType.COLLECTION);

		ListResource listParam = repository.read(ListResource.class, new IdType(listId));
		if (listParam == null){
			return output;
		}

		for (ListResource.ListResourceEntryComponent entry : listParam.getEntry()) {
			Reference ref = entry.getItem();
			if (ref == null || ref.getReferenceElement() == null) {
				continue;
			}
			String resourceType = ref.getReferenceElement().getResourceType();
			String idPart = ref.getReferenceElement().getIdPart();
			if (!Objects.equals(resourceType, "Parameters") || idPart == null) {
				continue;
			}
			Parameters parameters = repository.read(Parameters.class, new IdType(idPart));
			if (parameters != null) output.addEntry(new Bundle.BundleEntryComponent().setResource(parameters));
		}
		return output;
	}
}
