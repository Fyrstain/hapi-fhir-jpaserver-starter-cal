package ca.uhn.fhir.jpa.starter.cohort.service.r5.impl;

import ca.uhn.fhir.rest.api.server.RequestDetails;

public interface StudyInstantiateServiceFactory {
	StudyInstantiateServiceImpl create(RequestDetails requestDetails);
}
