package ca.uhn.fhir.jpa.starter.datamart.service.r5;

import org.hl7.fhir.r5.model.CanonicalType;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.ListResource;

public interface DatamartServiceImpl {

	ListResource generateDatamart(CanonicalType researchStudyUrl, Endpoint researchStudyEndpoint, Endpoint dataEndpoint, Endpoint terminologyEndpoint, Endpoint cqlEngineEndpoint);
}
