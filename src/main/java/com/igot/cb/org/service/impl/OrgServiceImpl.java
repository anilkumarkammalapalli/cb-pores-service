package com.igot.cb.org.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.demand.service.DemandService;
import com.igot.cb.demand.service.DemandServiceImpl;
import com.igot.cb.org.service.OrgService;
import com.igot.cb.playlist.util.ProjectUtil;
import com.igot.cb.pores.Service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.*;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.service.RequestHandlerServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class OrgServiceImpl implements OrgService {

    @Autowired
    PayloadValidation payloadValidation;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerServiceImpl;

    @Autowired
    CbServerProperties cbServerProperties;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    DemandService demandService;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    RequestHandlerServiceImpl requestHandlerService;

    @Autowired
    CbServerProperties propertiesConfig;

    @Override
    public ApiResponse readFramework(String frameworkName, String orgId, String termName, String userAuthToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_CREATE);
        try {
            if (StringUtils.isBlank(frameworkName) || StringUtils.isBlank(orgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("OrgID and FrameworkId is Missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String userId = accessTokenValidator.verifyUserToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (!isSpvRequest(userId, Arrays.asList(Constants.MDO_ADMIN,Constants.MDO_LEADER, Constants.SPV_ADMIN, Constants.SPV_PUBLISHER))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("User does not have the required role:");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ID, orgId);
            List<Map<String, Object>> orgDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, propertyMap, null, 1);
            if (CollectionUtils.isEmpty(orgDetails)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("Organization not found");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (StringUtils.isBlank((String) orgDetails.get(0).get(Constants.FRAMEWORK_STATUS))
                || orgDetails.get(0).get(Constants.FRAMEWORK_STATUS).toString()
                .equalsIgnoreCase(Constants.FAILED)) {
                String fwName = (String) orgDetails.get(0).get(Constants.FRAMEWORKID);
                if (StringUtils.isBlank(fwName)) {
                    String name = processFrameworkCreate(frameworkName,orgId,true);
                    log.info("copy framework id : ",name);
                    if (StringUtils.isNotEmpty(name)) {
                        log.info("copy framework id : ",name);
                        createOrgTerm(termName, name, frameworkName, orgId, userId);
                        publishFramework(name,orgId);
                        log.info("copy framework published and term creation also done.");
                        String orgUpdateUrl = cbServerProperties.getLearnerServiceUrl() + cbServerProperties.getOrgUpdateEndpoint();
                        Map<String, Object> orgResponse = outboundRequestHandlerServiceImpl.fetchResultUsingPatch(orgUpdateUrl,createOrgHierarchyRequestMap(orgId, Constants.ORG_HIERARCHY_FRAMEWORK_ID_KEY, Constants.ORG_HIERARCHY_FRAMEWORK_STATUS_KEY, name, Constants.COMPLETED),ProjectUtil.getDefaultHeadrs(userAuthToken));
                        if (MapUtils.isNotEmpty(orgResponse) && Constants.OK.equalsIgnoreCase(
                                (String) orgResponse.get(Constants.RESPONSE_CODE))) {
                            Map<String, Object> result = (Map<String, Object>) orgResponse.get(
                                    Constants.RESULT);
                            String orgResult = (String) result.getOrDefault(Constants.RESPONSE, "");
                            log.info("Organization updated successfully. orgId: {}, result: {}", orgId, orgResult);
                        }
                        response.getResult().put(Constants.FRAMEWORK, name);
                        response.setResponseCode(HttpStatus.OK);
                    } else {
                        log.info("unable to copy a framework");
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams()
                                .setErrMsg(Constants.FRAMEWORK_PROCESS_ALREADY_INITIALISED);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                    }
                } else {
                    response.getResult().put(Constants.FRAMEWORK, fwName);
                    response.setResponseCode(HttpStatus.OK);
                }
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams()
                    .setErrMsg(Constants.FRAMEWORK_PROCESS_ALREADY_INITIALISED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

        } catch (CustomException e) {
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
            log.error("Payload validation failed: " + e.getMessage());
        } catch (Exception e) {
            response.getParams().setErr("Failed to read framework: " + e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private ApiResponse frameworkRead(String frameworkId) {
        ApiResponse response = ProjectUtil.createDefaultResponse("");
        try {
            StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
            strUrl.append(cbServerProperties.getOdcsFrameworkRead()).append("/").append(frameworkId);
            Map<String, Object> framworkResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResult(strUrl.toString());
            if (null != framworkResponse) {
                if (Constants.OK.equalsIgnoreCase((String) framworkResponse.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> resultMap = (Map<String, Object>) framworkResponse.get(Constants.RESULT);
                    Map<String, Object> framework = (Map<String, Object>) resultMap.get(Constants.FRAMEWORK);
                    response.getResult().put(Constants.FRAMEWORK, framework);
                } else {
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    response.getParams().setErr("Data not found with id : " + frameworkId);
                }
            } else {
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                response.getParams().setErr("Failed to read the framework details for Id : " + frameworkId);
            }
        } catch (Exception e) {
            log.error("Failed to read framework with Id: " + frameworkId, e);
            response.getParams().setErr("Failed to read framework: " + e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private static List<Map<String, String>> createChannels(String channelId) {
        Map<String, String> channel = createChannel(channelId);
        List<Map<String, String>> channels = new ArrayList<>();
        channels.add(channel);
        return channels;
    }

    private static Map<String, String> createChannel(String channelId) {
        Map<String, String> channel = new HashMap<>();
        channel.put(Constants.IDENTIFIER, channelId);
        return channel;
    }
    public boolean isSpvRequest(String userId, List<String> requiredRoles) {
        Map<String, String> header = new HashMap<>();
        log.info("isSpvRequest started for userId: {}", userId);
        Map<String, Object> readData = (Map<String, Object>) requestHandlerService
                .fetchUsingGetWithHeadersProfile(propertiesConfig.getSbUrl() + propertiesConfig.getUserReadEndPoint() + userId,
                        header);
        Map<String, Object> result = (Map<String, Object>) readData.get(Constants.RESULT);
        Map<String, Object> responseMap = (Map<String, Object>) result.get(Constants.RESPONSE);
        List roles = (List) responseMap.get(Constants.ROLES);

        if (CollectionUtils.isNotEmpty(requiredRoles)) {
            for (String requiredRole : requiredRoles) {
                if (roles.contains(requiredRole)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String processFrameworkCreate(String masterFramework, String orgId, boolean includeOrgId) {
        String fwName = "";
        try {
            log.info("processFrameworkCreate started");
            Map<String, Object> createReq = createFrameworkRequest(orgId, masterFramework);
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.REQUEST, createReq);
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.X_CHANNEL_ID, orgId);
            StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
            strUrl.append(cbServerProperties.getFrameworkCopy()).append("/");
            strUrl.append(masterFramework);
            log.info("Printing URL for copy: {}", strUrl);
            log.info("Printing request: {}", request);
            Map<String, Object> frameworkResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResultUsingPost(
                    strUrl.toString(),
                    request, headers);
            if (MapUtils.isNotEmpty(frameworkResponse) && Constants.OK.equalsIgnoreCase(
                    (String) frameworkResponse.get(Constants.RESPONSE_CODE))) {
                Map<String, Object> result = (Map<String, Object>) frameworkResponse.get(
                        Constants.RESULT);
               fwName = (String) result.getOrDefault(Constants.NODE_ID, "");
               log.info("copy framework node id: {}", fwName);
            } else {
                log.error("Failed to copy the framework: {}",
                        frameworkResponse.get(Constants.RESPONSE_CODE));
            }

        } catch (Exception e) {
            log.error("Unexpected error occurred in processFrameworkCreate", e);
        }
        return fwName;
    }

    public static Map<String, Object> createFrameworkRequest(String channelId, String frameworkName) {
        Map<String, Object> framework = createFramework(channelId, frameworkName);
        Map<String, Object> request = new HashMap<>();
        request.put("framework", framework);
        return request;
    }

    private static Map<String, Object> createFramework(String channelId, String frameworkName) {
        Map<String, Object> framework = new HashMap<>();
        StringBuilder name = new StringBuilder(channelId).append("_").append(frameworkName);
        framework.put(Constants.NAME, name);
        framework.put(Constants.DESCRIPTION, "Framework for Channel " + channelId + ". This framework is a customized copy derived from the Master Framework");
        framework.put(Constants.CODE, name);
        framework.put(Constants.OWNER, channelId);
        return framework;
    }

    private void updateStatusToFailed(String orgId) {
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.ID, orgId);
        map.put(Constants.FRAMEWORK_STATUS, Constants.FAILED);
        Map<String, Object> updateOrgDetails = cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, map);
    }


    private String frameworkReadV1(String frameworkId) {
        String code = null;
        try {
            StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
            strUrl.append(cbServerProperties.getOdcsFrameworkRead()).append("/").append(frameworkId);
            log.info("prinitng framework read url "+strUrl.toString());
            Map<String, Object> framworkResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResult(strUrl.toString());
            if (null != framworkResponse) {
                if (Constants.OK.equalsIgnoreCase((String) framworkResponse.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> resultMap = (Map<String, Object>) framworkResponse.get(Constants.RESULT);
                    Map<String, Object> framework = (Map<String, Object>) resultMap.get(Constants.FRAMEWORK);
                    List categoriesList = (List<Map<String,Object>>) framework.get("categories");
                    Map<String,Object> map = (Map<String,Object>) categoriesList.get(0);
                    code = (String) map.get(Constants.CODE);
                    return code;
                } else {
                    log.info("Data not found with id : " + frameworkId);
                }
            } else {
                log.info("Data not found with ID: {}", frameworkId);
            }
        } catch (Exception e) {
            log.error("Failed to read framework with ID: {}", frameworkId, e);
        }
        return code;
    }

    private void createOrgTerm(String termName, String copyfw, String masterFramework, String orgId, String createdBy) {
        try {
            String category = frameworkReadV1(masterFramework);
            log.info("category first "+category);
            if (StringUtils.isNotEmpty(category)) {
                Map<String, Object> termMap = createTermMap(termName, category, createdBy);
                Map<String, Object> requestMap = createRequestMap(termMap);
                Map<String, Object> outerMap = createOuterMap(requestMap);
                StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
                strUrl.append(cbServerProperties.getOdcsTermCrete()).append("?framework=")
                        .append(copyfw).append("&category=")
                        .append(category);
                log.info("Created term strUrl {}", strUrl);
                Map<String, Object> termResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResultUsingPost(strUrl.toString(),
                        outerMap);
                if (termResponse != null
                        && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> resultMap = (Map<String, Object>) termResponse.get(Constants.RESULT);
                    List<String> termIdentifier = (List<String>) resultMap.getOrDefault(Constants.NODE_ID, "");
                    log.info("Created term successfully with name: {}", termName);
                    log.info("Term identifier: {}", termIdentifier);
                } else {
                    log.info("Unable to create term with name: {}", termName);
                    updateStatusToFailed(orgId);
                }
            }
        } catch (Exception e) {
            log.error("Unexpected error occurred while creating term", e);
        }
    }

    private void publishFramework(String fwName, String orgId) {
        try {
            log.info("publishFramework started: {}", fwName);
            StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
            strUrl.append(cbServerProperties.getFrameworkPublish()).append("/").append(fwName);
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.X_CHANNEL_ID, orgId);
            Map<String, Object> response = outboundRequestHandlerServiceImpl.fetchResultUsingPost(
                    strUrl.toString(), "", headers);
            log.info("framework publish url: {}", strUrl.toString());
            if (response != null
                    && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
                log.info("Published the framework: {}", fwName);
            } else {
                log.info("Unable to publish the framework with name: {}", fwName);
                updateStatusToFailed(orgId);
            }
        } catch (Exception e) {
            log.error("Unexpected error occurred while publishing the framework", e);
        }
    }
    public static Map<String, Object> createRequestMap(Map<String, Object> termMap) {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("term", termMap);
        return requestMap;
    }

    public static Map<String, Object> createOuterMap(Map<String, Object> requestMap) {
        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put("request", requestMap);
        return outerMap;
    }

    public static Map<String, Object> createTermMap(String termName, String category, String createdBy) {
        Map<String, Object> termMap = new HashMap<>();
        termMap.put(Constants.NAME, termName);
        termMap.put(Constants.DESCRIPTION, termName);
        termMap.put(Constants.CODE, UUID.randomUUID());
        termMap.put(Constants.REF_TYPE, "");
        termMap.put(Constants.REF_ID, "");
        termMap.put(Constants.CATEGORY, category);
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(Constants.TIMESTAMP, System.currentTimeMillis());
        additionalProperties.put(Constants.CREATED_BY, createdBy);
        termMap.put(Constants.ADDITIONAL_PROPERTIES, additionalProperties);
        return termMap;
    }

    public void updateOrganizationFramework(String frameworkId, String orgId, String frameworkIdKey, String frameworkStatusKey) {
        log.info("updateOrganizationFramework function started : {}");
        Map<String, Object> updateFields = new HashMap<>();
        updateFields.put(frameworkIdKey, frameworkId);
        updateFields.put(Constants.ID, orgId);
        updateFields.put(frameworkStatusKey, Constants.COMPLETED);
        log.info("updateOrganizationFramework map : {}", updateFields);
        try {
            Map<String, Object> updateOrgDetails = cassandraOperation.updateRecord(
                    Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, updateFields);

            String updateResponse = (String) updateOrgDetails.get(Constants.RESPONSE);
            if (StringUtils.isNotEmpty(updateResponse) && updateResponse.equalsIgnoreCase(Constants.SUCCESS)) {
                log.info("Updated framework_id in organization table successfully with name: {}", frameworkId);
            } else {
                log.error("Failed to update organization details with the new framework ID");
            }
        } catch (Exception e) {
            log.error("An error occurred while updating organization details", e);
        }

    }

    @Override
    public ApiResponse createOrgHierarchyFramework(String masterFramework, String orgId, String userAuthToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_HIERARCHY_FRAMEWORK_CREATE);
        try {
            if (StringUtils.isBlank(masterFramework) || StringUtils.isBlank(orgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("OrgID and FrameworkId is Missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String userId = accessTokenValidator.verifyUserToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (!isSpvRequest(userId, Arrays.asList(Constants.MDO_ADMIN,Constants.MDO_LEADER, Constants.SPV_ADMIN, Constants.SPV_PUBLISHER, Constants.STATE_ADMIN))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("User does not have the required role:");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> orgDetail = getOrgDetailsById(orgId);
            if (orgDetail == null) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("Organization not found");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (StringUtils.isBlank((String) orgDetail.get(Constants.ORG_HIERARCHY_FRAMEWORK_STATUS))
                    || orgDetail.get(Constants.ORG_HIERARCHY_FRAMEWORK_STATUS).toString()
                    .equalsIgnoreCase(Constants.FAILED)) {
                String fwName = (String) orgDetail.get(Constants.ORG_HIERARCHY_FRAMEWORK_ID);
                if (StringUtils.isBlank(fwName)) {
                    String name = processFrameworkCreate(masterFramework,orgId, false);
                    log.info("copy framework id : ",name);
                    if (StringUtils.isNotEmpty(name)) {
                        log.info("copy framework id : ",name);
                        publishFramework(name,orgId);
                        String orgUpdateUrl = cbServerProperties.getLearnerServiceUrl() + cbServerProperties.getOrgUpdateEndpoint();
                        Map<String, Object> orgResponse = outboundRequestHandlerServiceImpl.fetchResultUsingPatch(orgUpdateUrl,createOrgHierarchyRequestMap(orgId, Constants.ORG_HIERARCHY_FRAMEWORK_ID_KEY, Constants.ORG_HIERARCHY_FRAMEWORK_STATUS_KEY, name, Constants.COMPLETED),ProjectUtil.getDefaultHeadrs(userAuthToken));
                        if (MapUtils.isNotEmpty(orgResponse) && Constants.OK.equalsIgnoreCase(
                                (String) orgResponse.get(Constants.RESPONSE_CODE))) {
                            Map<String, Object> result = (Map<String, Object>) orgResponse.get(
                                    Constants.RESULT);
                            String orgResult = (String) result.getOrDefault(Constants.RESPONSE, "");
                            log.info("Organization updated successfully. orgId: {}, result: {}", orgId, orgResult);
                        }
                        response.getResult().put(Constants.FRAMEWORK, name);
                        response.setResponseCode(HttpStatus.OK);
                    } else {
                        log.info("unable to copy a framework");
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams()
                                .setErrMsg(Constants.FRAMEWORK_PROCESS_ALREADY_INITIALISED);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                    }
                } else {
                    response.getResult().put(Constants.FRAMEWORK, fwName);
                    response.setResponseCode(HttpStatus.OK);
                }
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams()
                        .setErrMsg(Constants.FRAMEWORK_PROCESS_ALREADY_INITIALISED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

        } catch (CustomException e) {
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
            log.error("Payload validation failed: " + e.getMessage());
        } catch (Exception e) {
            log.error("An error occurred while creating organization hierarchy framework", e);
            response.getParams().setErr("Failed to create organization hierarchy framework: " + e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private Map<String, Object> getOrgDetailsById(String orgId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, orgId);

        List<Map<String, Object>> orgDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD,
                Constants.ORG_TABLE,
                propertyMap,
                null,
                1
        );

        if (CollectionUtils.isNotEmpty(orgDetails)) {
            return orgDetails.get(0);
        } else {
            return null;
        }
    }

    public static Map<String, Object> createOrgHierarchyRequestMap(String organisationId, String frameworkIdKey, String frameworkStatusKey, String frameworkId, String frameworkStatus) {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ORGANISATION_ID, organisationId);
        requestMap.put(frameworkIdKey, frameworkId);
        requestMap.put(frameworkStatusKey, frameworkStatus);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put(Constants.REQUEST, requestMap);
        return outerMap;
    }

}