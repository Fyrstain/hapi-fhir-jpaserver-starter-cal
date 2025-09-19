package ca.uhn.fhir.jpa.starter.cohort.service.r5.impl;

import org.hl7.fhir.r5.model.CanonicalType;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.Group;

public interface CohorteServiceImpl {
	Group cohorting(CanonicalType researchStudyUrl, Endpoint researchStudyEndpoint, Endpoint dataEndpoint, Endpoint terminologyEndpoint, Endpoint cqlEngineEndpoint);
}
