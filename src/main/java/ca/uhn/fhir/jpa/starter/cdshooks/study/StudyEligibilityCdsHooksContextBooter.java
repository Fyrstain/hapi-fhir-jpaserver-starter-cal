package ca.uhn.fhir.jpa.starter.cdshooks.study;

import ca.uhn.fhir.context.ConfigurationException;
import ca.uhn.fhir.i18n.Msg;
import ca.uhn.fhir.rest.api.server.cdshooks.CdsHooksExtension;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ca.uhn.hapi.fhir.cdshooks.api.CdsService;
import ca.uhn.hapi.fhir.cdshooks.api.CdsServiceFeedback;
import ca.uhn.hapi.fhir.cdshooks.api.CdsServicePrefetch;
import ca.uhn.hapi.fhir.cdshooks.api.json.CdsServiceJson;
import ca.uhn.hapi.fhir.cdshooks.svc.CdsHooksContextBooter;
import ca.uhn.hapi.fhir.cdshooks.svc.CdsServiceCache;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StudyEligibilityCdsHooksContextBooter extends CdsHooksContextBooter {
	private static final Logger ourLog = LoggerFactory.getLogger(StudyEligibilityCdsHooksContextBooter.class);

	private final CdsServiceCache studyCdsServiceCache = new CdsServiceCache();

	private List<Object> studyCdsServiceBeans = new ArrayList<>();

	private Class<?> studyDefinitionsClass;

	private AnnotationConfigApplicationContext studyAppCtx;

	private static final String STUDY_CDS_SERVICES_BEAN_NAME = "cdsServices";

	public StudyEligibilityCdsHooksContextBooter() {
		super();
		StudyEligibilityCheckConfig ctx = new StudyEligibilityCheckConfig();
		studyCdsServiceBeans = ctx.cdsServices();
	}

	public void setDefinitionsClass(Class<?> theDefinitionsClass) {
		this.studyDefinitionsClass = theDefinitionsClass;
	}

	public CdsServiceCache buildCdsServiceCache() {
		for (Object serviceBean : studyCdsServiceBeans) {
			extractCdsServices(serviceBean);
		}
		return studyCdsServiceCache;
	}

	@SuppressWarnings("unchecked")
	public void extractCdsServices(Object theServiceBean) {
		Method[] methods = theServiceBean.getClass().getMethods();
		List<Method> sorted = Arrays.stream(methods)
			.sorted(Comparator.comparing(Method::getName))
			.collect(Collectors.toList());

		for (Method method : sorted) {
			if (method.isAnnotationPresent(CdsService.class)) {
				CdsService ann = method.getAnnotation(CdsService.class);
				CdsServiceJson js = new CdsServiceJson();
				js.setId(ann.value());
				js.setHook(ann.hook());
				js.setTitle(ann.title());
				js.setDescription(ann.description());
				js.setExtension(validateJsonExtension(ann.extension()));
				for (CdsServicePrefetch p : ann.prefetch()) {
					js.addPrefetch(p.value(), p.query());
					js.addSource(p.value(), p.source());
				}
				studyCdsServiceCache.registerService(
					js.getId(),
					theServiceBean,
					method,
					js,
					ann.allowAutoFhirClientPrefetch());
			}
			if (method.isAnnotationPresent(CdsServiceFeedback.class)) {
				CdsServiceFeedback ann = method.getAnnotation(CdsServiceFeedback.class);
				studyCdsServiceCache.registerFeedback(
					ann.value(),
					theServiceBean,
					method);
			}
		}
	}

	private CdsHooksExtension validateJsonExtension(String theExtension) {
		if (StringUtils.isEmpty(theExtension)) {
			return null;
		}
		try {
			return new ObjectMapper().readValue(theExtension, CdsHooksExtension.class);
		} catch (JsonProcessingException e) {
			String msg = String.format("Invalid JSON for CDS Hooks extension: %s", e.getMessage());
			ourLog.error(msg, e);
			throw new UnprocessableEntityException(Msg.code(2378) + msg);
		}
	}

	@SuppressWarnings("unchecked")
	public void start() {
		if (studyDefinitionsClass == null) {
			ourLog.info("No definition class provided for StudyEligibility.");
			return;
		}
		ourLog.info("Starting the ApplicationContext for : {}", studyDefinitionsClass.getName());
		studyAppCtx = new AnnotationConfigApplicationContext();
		studyAppCtx.register(studyDefinitionsClass);
		studyAppCtx.refresh();

		try {
			if (studyAppCtx.containsBean(STUDY_CDS_SERVICES_BEAN_NAME)) {
				studyCdsServiceBeans = studyAppCtx.getBean(
					STUDY_CDS_SERVICES_BEAN_NAME, List.class);
			} else {
				ourLog.info("No bean named {}", STUDY_CDS_SERVICES_BEAN_NAME);
			}

			if (studyCdsServiceBeans.isEmpty()) {
				throw new ConfigurationException(Msg.code(2379)
					+ "No CDS Service found (bean named "
					+ STUDY_CDS_SERVICES_BEAN_NAME + ")");
			}
		} catch (Exception e) {
			stop();
			throw e instanceof ConfigurationException
				? (ConfigurationException) e
				: new ConfigurationException(Msg.code(2393) + e.getMessage(), e);
		}
	}
}
