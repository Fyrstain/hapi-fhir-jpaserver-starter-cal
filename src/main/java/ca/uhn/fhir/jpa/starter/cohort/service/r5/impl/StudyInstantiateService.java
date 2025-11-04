package ca.uhn.fhir.jpa.starter.cohort.service.r5.impl;

import org.hl7.fhir.r5.model.CanonicalType;
import org.hl7.fhir.r5.model.Endpoint;
import org.hl7.fhir.r5.model.Parameters;

public interface StudyInstantiateService {
	Parameters instantiateStudy(CanonicalType studyUrl, Endpoint researchStudyEndpoint);
}
