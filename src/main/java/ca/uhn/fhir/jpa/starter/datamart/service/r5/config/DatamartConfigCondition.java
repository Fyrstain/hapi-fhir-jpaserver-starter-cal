package ca.uhn.fhir.jpa.starter.datamart.service.r5.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class DatamartConfigCondition implements Condition {

	@Override
	public boolean matches(ConditionContext theConditionContext, AnnotatedTypeMetadata theAnnotatedTypeMetadata) {
		String property = theConditionContext.getEnvironment().getProperty("hapi.fhir.datamart.enabled");
		return Boolean.parseBoolean(property);
	}
}
