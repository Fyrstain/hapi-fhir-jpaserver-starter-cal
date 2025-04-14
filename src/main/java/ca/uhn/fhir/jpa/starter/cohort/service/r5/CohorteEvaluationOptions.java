package ca.uhn.fhir.jpa.starter.cohort.service.r5;

import org.opencds.cqf.fhir.cql.EvaluationSettings;
import org.opencds.cqf.fhir.utility.ValidationProfile;

import java.util.HashMap;
import java.util.Map;

public class CohorteEvaluationOptions {
	private boolean isValidationEnabled = false;
	private Map<String, ValidationProfile> validationProfiles = new HashMap();
	private EvaluationSettings evaluationSettings = null;

	private CohorteEvaluationOptions() {
	}

	public static CohorteEvaluationOptions defaultOptions() {
		CohorteEvaluationOptions options = new CohorteEvaluationOptions();
		options.setEvaluationSettings(EvaluationSettings.getDefault());
		return options;
	}

	public boolean isValidationEnabled() {
		return this.isValidationEnabled;
	}

	public void setValidationEnabled(boolean enableValidation) {
		this.isValidationEnabled = enableValidation;
	}

	public Map<String, ValidationProfile> getValidationProfiles() {
		return this.validationProfiles;
	}

	public void setValidationProfiles(Map<String, ValidationProfile> validationProfiles) {
		this.validationProfiles = validationProfiles;
	}

	public EvaluationSettings getEvaluationSettings() {
		return this.evaluationSettings;
	}

	public void setEvaluationSettings(EvaluationSettings evaluationSettings) {
		this.evaluationSettings = evaluationSettings;
	}
}

