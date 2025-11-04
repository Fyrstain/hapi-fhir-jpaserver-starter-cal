package ca.uhn.fhir.jpa.starter.datamart.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IQueryParameterType;
import ca.uhn.fhir.rest.api.MethodOutcome;
import com.google.common.base.Preconditions;
import org.hl7.fhir.instance.model.api.*;
import org.opencds.cqf.fhir.api.Repository;
import org.opencds.cqf.fhir.utility.repository.FederatedRepository;

import java.util.*;

public class DatamartRepository implements Repository {
    private static final Set<String> terminologyResourceSet = new HashSet(Arrays.asList("ValueSet", "CodeSystem", "ConceptMap", "StructureMap"));
    private static final Set<String> researchResourceSet = new HashSet(Arrays.asList("Library", "EvidenceVariable", "ResearchStudy", "Group", "List"));
    private final Repository data;
    private final Repository research;
    private final Repository terminology;
    private Repository local;

    public DatamartRepository(Repository local, Boolean useLocalData, Repository data, Repository research, Repository terminology) {
        Preconditions.checkNotNull(local);
        this.local = local;
        this.data = data == null ? this.local : (useLocalData ? new FederatedRepository(local, data) : data);
        this.research = research == null ? this.local : research;
        this.terminology = terminology == null ? this.local : terminology;
    }

    public DatamartRepository(Repository data, Repository research, Repository terminology) {
        Preconditions.checkNotNull(data);
        this.data = data;
        this.research = research == null ? this.data : research;
        this.terminology = terminology == null ? this.data : terminology;
    }

    public <T extends IBaseResource, I extends IIdType> T read(Class<T> resourceType, I id, Map<String, String> headers) {
        if (this.isTerminologyResource(resourceType.getSimpleName())) {
            return this.terminology.read(resourceType, id, headers);
        } else {
            return this.isResearchResource(resourceType.getSimpleName()) ? this.research.read(resourceType, id, headers) : this.data.read(resourceType, id, headers);
        }
    }

    public <T extends IBaseResource> MethodOutcome create(T resource, Map<String, String> headers) {
		 if (this.isTerminologyResource(resource.getIdElement().getResourceType())) {
			 return this.terminology.create(resource);
		 } else {
			 return this.isResearchResource(resource.getIdElement().getResourceType()) ? this.research.create(resource) : this.data.create(resource);
		 }
    }

    public <I extends IIdType, P extends IBaseParameters> MethodOutcome patch(I id, P patchParameters, Map<String, String> headers) {
        return null;
    }

    public <T extends IBaseResource> MethodOutcome update(T resource, Map<String, String> headers) {
		 if (this.isTerminologyResource(resource.getIdElement().getResourceType())) {
			 return this.terminology.update(resource);
		 } else {
			 return this.isResearchResource(resource.getIdElement().getResourceType()) ? this.research.update(resource) : this.data.update(resource);
		 }
    }

    public <T extends IBaseResource, I extends IIdType> MethodOutcome delete(Class<T> resourceType, I id, Map<String, String> headers) {
        return null;
    }

    public <B extends IBaseBundle, T extends IBaseResource> B search(Class<B> bundleType, Class<T> resourceType, Map<String, List<IQueryParameterType>> searchParameters, Map<String, String> headers) {
        if (this.isTerminologyResource(resourceType.getSimpleName())) {
            return this.terminology.search(bundleType, resourceType, searchParameters, headers);
        } else {
            return this.isResearchResource(resourceType.getSimpleName()) ? this.research.search(bundleType, resourceType, searchParameters, headers) : this.data.search(bundleType, resourceType, searchParameters, headers);
        }
    }

    public <B extends IBaseBundle> B link(Class<B> bundleType, String url, Map<String, String> headers) {
        return null;
    }

    public <C extends IBaseConformance> C capabilities(Class<C> resourceType, Map<String, String> headers) {
        return null;
    }

    public <B extends IBaseBundle> B transaction(B transaction, Map<String, String> headers) {
        return null;
    }

    public <R extends IBaseResource, P extends IBaseParameters> R invoke(String name, P parameters, Class<R> returnType, Map<String, String> headers) {
        return this.terminology.invoke(name, parameters, returnType, headers);
    }

    public <P extends IBaseParameters> MethodOutcome invoke(String name, P parameters, Map<String, String> headers) {
        return null;
    }

    public <R extends IBaseResource, P extends IBaseParameters, T extends IBaseResource> R invoke(Class<T> resourceType, String name, P parameters, Class<R> returnType, Map<String, String> headers) {
        if (this.isTerminologyResource(resourceType.getSimpleName())) {
            return this.terminology.invoke(resourceType, name, parameters, returnType, headers);
        } else {
            return this.isResearchResource(resourceType.getSimpleName()) ? this.research.invoke(resourceType, name, parameters, returnType, headers) : this.data.invoke(resourceType, name, parameters, returnType, headers);
        }
    }

    public <P extends IBaseParameters, T extends IBaseResource> MethodOutcome invoke(Class<T> resourceType, String name, P parameters, Map<String, String> headers) {
        return null;
    }

    public <R extends IBaseResource, P extends IBaseParameters, I extends IIdType> R invoke(I id, String name, P parameters, Class<R> returnType, Map<String, String> headers) {
        return null;
    }

    public <P extends IBaseParameters, I extends IIdType> MethodOutcome invoke(I id, String name, P parameters, Map<String, String> headers) {
        return null;
    }

    public <B extends IBaseBundle, P extends IBaseParameters> B history(P parameters, Class<B> returnType, Map<String, String> headers) {
        return null;
    }

    public <B extends IBaseBundle, P extends IBaseParameters, T extends IBaseResource> B history(Class<T> resourceType, P parameters, Class<B> returnType, Map<String, String> headers) {
        return null;
    }

    public <B extends IBaseBundle, P extends IBaseParameters, I extends IIdType> B history(I id, P parameters, Class<B> returnType, Map<String, String> headers) {
        return null;
    }

    public FhirContext fhirContext() {
        return this.data.fhirContext();
    }

    private boolean isTerminologyResource(String type) {
        return terminologyResourceSet.contains(type);
    }

    private boolean isResearchResource(String type) {
        return researchResourceSet.contains(type);
    }
}