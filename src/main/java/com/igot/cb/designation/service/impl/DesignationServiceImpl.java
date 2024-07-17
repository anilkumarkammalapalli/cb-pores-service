package com.igot.cb.designation.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.designation.entity.DesignationEntity;
import com.igot.cb.designation.repository.DesignationRepository;
import com.igot.cb.designation.service.DesignationService;
import com.igot.cb.playlist.util.ProjectUtil;
import com.igot.cb.pores.Service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class DesignationServiceImpl implements DesignationService {

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  private DesignationRepository designationRepository;

  @Autowired
  private PayloadValidation payloadValidation;

  @Autowired
  private EsUtilService esUtilService;

  @Autowired
  private CacheService cacheService;

  @Autowired
  private CbServerProperties cbServerProperties;

  @Autowired
  private OutboundRequestHandlerServiceImpl outboundRequestHandlerServiceImpl;

  @Override
  public void loadDesignationFromExcel(MultipartFile file) {
    log.info("DesignationServiceImpl::loadDesignationFromExcel");
    List<Map<String, String>> processedData = processExcelFile(file);
    log.info("No.of processedData from excel: " + processedData.size());
    JsonNode designationJson = objectMapper.valueToTree(processedData);
    AtomicLong startingId = new AtomicLong(designationRepository.count());
    DesignationEntity designationEntity = new DesignationEntity();
    designationJson.forEach(
        eachDesignation -> {
          String formattedId = String.format("DESG-%06d", startingId.incrementAndGet());
          if (!eachDesignation.isNull()) {
            ((ObjectNode) eachDesignation).put(Constants.ID, formattedId);
            if (eachDesignation.has(Constants.UPDATED_DESIGNATION) && !eachDesignation.get(
                Constants.UPDATED_DESIGNATION).isNull()) {
              ((ObjectNode) eachDesignation).put(Constants.DESIGNATION,
                  eachDesignation.get(Constants.UPDATED_DESIGNATION));
            }
            String descriptionValue =
                (eachDesignation.has(Constants.DESCRIPTION_PAYLOAD) && !eachDesignation.get(
                    Constants.DESCRIPTION_PAYLOAD).isNull())
                    ? eachDesignation.get(Constants.UPDATED_DESIGNATION).asText("")
                    : "";
            ((ObjectNode) eachDesignation).put(Constants.DESCRIPTION, descriptionValue);
            payloadValidation.validatePayload(Constants.DESIGNATION_PAYLOAD_VALIDATION,
                eachDesignation);
            ((ObjectNode) eachDesignation).put(Constants.STATUS, Constants.ACTIVE);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) eachDesignation).put(Constants.CREATED_ON, String.valueOf(currentTime));
            ((ObjectNode) eachDesignation).put(Constants.UPDATED_ON, String.valueOf(currentTime));
            designationEntity.setId(formattedId);
            designationEntity.setData(eachDesignation);
            designationEntity.setIsActive(true);
            designationEntity.setCreatedOn(currentTime);
            designationEntity.setUpdatedOn(currentTime);
            designationRepository.save(designationEntity);
            log.info(
                "DesignationServiceImpl::loadDesignationFromExcel::persited designation in postgres with id: "
                    + formattedId);
            Map<String, Object> map = objectMapper.convertValue(eachDesignation, Map.class);
            esUtilService.addDocument(Constants.DESIGNATION_INDEX_NAME, Constants.INDEX_TYPE,
                formattedId, map, cbServerProperties.getElasticDesignationJsonPath());
            cacheService.putCache(formattedId, eachDesignation);
            log.info(
                "DesignationServiceImpl::loadDesignationFromExcel::created the designation with: "
                    + formattedId);
          }

        });
    log.info("DesignationServiceImpl::loadDesignationFromExcel::created the designations");
  }
  @Override
  public ApiResponse createDesignation(JsonNode request) {
    ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_DESIGNATION_CREATE);
    try {
    payloadValidation.validatePayload(Constants.DESIGNATION_CREATE_PAYLOAD_VALIDATION, request);
    String name = request.get(Constants.NAME).asText();
    String ref_Id = request.get(Constants.REF_ID).asText();
    Optional<DesignationEntity> designationEntity = designationRepository.findByIdAndIsActive(ref_Id, Boolean.TRUE);
    if (designationEntity.isPresent()) {
      DesignationEntity designation = designationEntity.get();
      if (designation.getIsActive()) {
        ApiResponse readResponse = readDesignation(ref_Id);
        if (readResponse == null) {
          response.getParams().setErr("Failed to validate sector exists or not.");
          response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
          response.getParams().setStatus(Constants.FAILED);
        } else if (HttpStatus.NOT_FOUND.equals(readResponse.getResponseCode())) {
          Map<String, Object> reqBody = new HashMap<>();
          request.fields().forEachRemaining(entry -> reqBody.put(entry.getKey(), entry.getValue().asText()));
          Map<String, Object> parentObj = new HashMap<>();
          parentObj.put(Constants.IDENTIFIER,
                  cbServerProperties.getOdcsDesignationFramework() + "_" + cbServerProperties.getOdcsDesignationCategory());
          reqBody.put(Constants.PARENTS, Arrays.asList(parentObj));
          Map<String, Object> termReq = new HashMap<String, Object>();
          termReq.put(Constants.TERM, reqBody);
          Map<String, Object> createReq = new HashMap<String, Object>();
          createReq.put(Constants.REQUEST, termReq);
          StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
          strUrl.append(cbServerProperties.getOdcsTermCrete()).append("?framework=")
                  .append(cbServerProperties.getOdcsDesignationFramework()).append("&category=")
                  .append(cbServerProperties.getOdcsDesignationCategory());
          Map<String, Object> termResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResultUsingPost(strUrl.toString(),
                  createReq);
          if (termResponse != null
                  && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> resultMap = (Map<String, Object>) termResponse.get(Constants.RESULT);
            List<String> termIdentifier = (List<String>) resultMap.getOrDefault(Constants.NODE_ID, "");
            log.info("Created Designation successfully with name: " + ref_Id);
            log.info("termIdentifier : " + termIdentifier);
            Map<String, Object> reqBodyMap = new HashMap<>();
            reqBodyMap.put(Constants.ID, ref_Id);
            reqBodyMap.put(Constants.DESIGNATION, name);
            reqBodyMap.put(Constants.REF_NODES, termIdentifier);
            CustomResponse desgResponse = updateDesignation(objectMapper.valueToTree(reqBodyMap));
            if (desgResponse.getResponseCode() != HttpStatus.OK) {
              log.error("Failed to update designation: " + response.getParams().getErr());
              response.getParams().setErr("Failed to update designation.");
              response.setResult(desgResponse.getResult());
              response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
              response.getParams().setStatus(Constants.FAILED);
            }
          } else {
            log.error("Failed to create the Designation with name: " + ref_Id);
            response.getParams().setErr("Failed to create the Designation");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
          }
        } else if (HttpStatus.OK.equals(readResponse.getResponseCode())) {
          String errMsg = "Designation already exists with name: " + ref_Id;
          log.error(errMsg);
          response.getParams().setErr(errMsg);
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          response.getParams().setStatus(Constants.FAILED);
        } else {
          log.error("Failed to create the Designation with name: " + ref_Id);
          response.getParams().setErr("Failed to create.");
          response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
          response.getParams().setStatus(Constants.FAILED);
        }
      } else {
        //if desg. is not active.
        log.error("Failed to create Designation exists with name: " + ref_Id);
        response.getParams().setErr("Failed to create Designation.");
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        response.getParams().setStatus(Constants.FAILED);
      }
    } else {
      log.error("Failed to validate Designation exists with name: " + ref_Id);
      response.getParams().setErr("Designation Not Exist.");
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      response.getParams().setStatus(Constants.FAILED);
    }
  } catch (CustomException e) {
      response.getParams().setErr(e.getMessage());
      response.setResponseCode(HttpStatus.BAD_REQUEST);
      response.getParams().setStatus(Constants.FAILED);
      log.error("Payload validation failed: " + e.getMessage());
    } catch (Exception e) {
      response.getParams().setErr("Unexpected error occurred while processing the request.");
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      response.getParams().setStatus(Constants.FAILED);
      log.error("Unexpected error occurred: " + e.getMessage(), e);
    }
    return response;
  }

  private List<Map<String, String>> processExcelFile(MultipartFile incomingFile) {
    log.info("DesignationServiceImpl::processExcelFile");
    try {
      return validateFileAndProcessRows(incomingFile);
    } catch (Exception e) {
      log.error("Error occurred during file processing: {}", e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> validateFileAndProcessRows(MultipartFile file) {
    log.info("DesignationServiceImpl::validateFileAndProcessRows");
    log.info("DesignationServiceImpl::validateFileAndProcessRows");
    String fileName = file.getOriginalFilename();
    if (fileName == null) {
      throw new RuntimeException("File name is null");
    }

    try (InputStream inputStream = file.getInputStream()) {
      if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
        Workbook workbook = WorkbookFactory.create(inputStream);
        Sheet sheet = workbook.getSheetAt(0);
        return processSheetAndSendMessage(sheet);
      } else if (fileName.endsWith(".csv")) {
        return processCsvAndSendMessage(inputStream);
      } else {
        throw new RuntimeException("Unsupported file type: " + fileName);
      }
    } catch (IOException e) {
      log.error("Error while processing file: {}", e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> processSheetAndSendMessage(Sheet sheet) {
    log.info("DesignationServiceImpl::processSheetAndSendMessage");
    try {
      DataFormatter formatter = new DataFormatter();
      Row headerRow = sheet.getRow(0);
      List<Map<String, String>> dataRows = new ArrayList<>();
      for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
        Row dataRow = sheet.getRow(rowIndex);
        if (dataRow == null) {
          break; // No more data rows, exit the loop
        }
        boolean allBlank = true;
        Map<String, String> rowData = new HashMap<>();
        for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
          Cell headerCell = headerRow.getCell(colIndex);
          Cell valueCell = dataRow.getCell(colIndex);
          if (headerCell != null && headerCell.getCellType() != CellType.BLANK) {
            String excelHeader =
                formatter.formatCellValue(headerCell).replaceAll("[\\n*]", "").trim();
            String cellValue = "";
            if (valueCell != null && valueCell.getCellType() != CellType.BLANK) {
              if (valueCell.getCellType() == CellType.NUMERIC
                  && DateUtil.isCellDateFormatted(valueCell)) {
                // Handle date format
                Date date = valueCell.getDateCellValue();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                cellValue = dateFormat.format(date);
              } else {
                cellValue = formatter.formatCellValue(valueCell).replace("\n", ",").trim();
              }
              allBlank = false;
            }
            rowData.put(excelHeader, cellValue);
          }
        }
        log.info("Data Rows: " + rowData);
        if (allBlank) {
          break; // If all cells are blank in the current row, stop processing
        }
        dataRows.add(rowData);
      }
      log.info("Number of Data Rows Processed: " + dataRows.size());
      return dataRows;
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> processCsvAndSendMessage(InputStream inputStream) throws IOException {
    log.info("DesignationServiceImpl::processCsvAndSendMessage");
    List<Map<String, String>> dataRows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

      List<String> headers = csvParser.getHeaderNames();

      for (CSVRecord csvRecord : csvParser) {
        boolean allBlank = true;
        Map<String, String> rowData = new HashMap<>();
        for (String header : headers) {
          String cellValue = csvRecord.get(header);
          if (cellValue != null && !cellValue.trim().isEmpty()) {
            // Handle date format (assuming date is in a specific format)
            if (isDate(cellValue)) {
              SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
              cellValue = dateFormat.format(parseDate(cellValue));
            } else {
              cellValue = cellValue.replace("\n", ",").trim();
            }
            allBlank = false;
          }
          rowData.put(header, cellValue);
        }
        log.info("Data Rows: " + rowData);
        if (allBlank) {
          break; // If all cells are blank in the current row, stop processing
        }
        dataRows.add(rowData);
      }
      log.info("Number of Data Rows Processed: " + dataRows.size());
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
    return dataRows;
  }

  private boolean isDate(String value) {
    try {
      parseDate(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private Date parseDate(String value) throws Exception {
    // Customize this date parsing logic based on the expected date format in your CSV
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    return dateFormat.parse(value);
  }
  public ApiResponse readDesignation(String Id) {
    ApiResponse response = new ApiResponse();
    try {
      StringBuilder strUrl = new StringBuilder(cbServerProperties.getKnowledgeMS());
      strUrl.append(cbServerProperties.getOdcsDesignationTermRead()).append("/").append(Id).append("?framework=")
              .append(cbServerProperties.getOdcsDesignationFramework()).append("&category=")
              .append(cbServerProperties.getOdcsDesignationCategory());

      Map<String, Object> map = new HashMap<String, Object>();
      Map<String, Object> desgResponse = (Map<String, Object>) outboundRequestHandlerServiceImpl.fetchResult(strUrl.toString());
      if (null != desgResponse) {
        if (Constants.OK.equalsIgnoreCase((String) desgResponse.get(Constants.RESPONSE_CODE))) {
          Map<String, Object> resultMap = (Map<String, Object>) desgResponse.get(Constants.RESULT);
          Map<String, Object> input = (Map<String, Object>) resultMap.get(Constants.TERM);
          processDesignation(input, map);
          response.getResult().put(Constants.DESIGNATION, map);
        } else {
          response.setResponseCode(HttpStatus.NOT_FOUND);
          response.getParams().setErr("Data not found with id : " + Id);
        }
      } else {
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        response.getParams().setErr("Failed to read the des details for Id : " + Id);
      }
    } catch (Exception e) {
      log.error("Failed to read Designation with Id: " + Id, e);
      response.getParams().setErr("Failed to read Designation: " + e.getMessage());
      response.getParams().setStatus(Constants.FAILED);
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  private void processDesignation(Map<String, Object> designationInput, Map<String, Object> designationMap) {
    for (String field : cbServerProperties.getOdcsFields()) {
      if (designationInput.containsKey(field)) {
        designationMap.put(field, designationInput.get(field));
      }
    }
    if (designationInput.containsKey(Constants.CHILDREN)) {
      designationMap.put(Constants.CHILDREN, new ArrayList<Map<String, Object>>());
      processSubDesignation(designationInput, designationMap);
    }
  }

  private void processSubDesignation(Map<String, Object> designation, Map<String, Object> newDesignation) {
    List<Map<String, Object>> designationList = (List<Map<String, Object>>) designation.get(Constants.CHILDREN);
    Set<String> uniqueDesg = new HashSet<String>();
    for (Map<String, Object> desig : designationList) {
      if (uniqueDesg.contains((String) desig.get(Constants.IDENTIFIER))) {
        continue;
      } else {
        uniqueDesg.add((String) desig.get(Constants.IDENTIFIER));
      }
      Map<String, Object> newSubDesignation = new HashMap<String, Object>();
      for (String field : cbServerProperties.getOdcsFields()) {
        if (desig.containsKey(field)) {
          newSubDesignation.put(field, desig.get(field));
        }
      }
      ((List) newDesignation.get(Constants.CHILDREN)).add(newSubDesignation);
    }
  }

  @Override
  public CustomResponse updateDesignation(JsonNode updateDesignationDetails) {
    log.info("DesignationServiceImpl::updateDesignation::inside the method");
    payloadValidation.validatePayload(Constants.DESIGNATION_PAYLOAD_VALIDATION,
            updateDesignationDetails);
    CustomResponse response = new CustomResponse();
    try {
      if (updateDesignationDetails.has(Constants.ID) && !updateDesignationDetails.get(Constants.ID)
              .isNull() && updateDesignationDetails.has(Constants.REF_NODES)
              && !updateDesignationDetails.get(Constants.REF_NODES).isNull()) {
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String id = updateDesignationDetails.get(Constants.ID).asText();
        Optional<DesignationEntity> designationEntiy = designationRepository.findById(
                id);
        DesignationEntity designationEntityUpdated = null;
        if (designationEntiy.isPresent()) {
          JsonNode dataNode = designationEntiy.get().getData();
          Iterator<Map.Entry<String, JsonNode>> fields = updateDesignationDetails.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            // Check if the field is present in the update JsonNode
            if (dataNode.has(fieldName)) {
              // Update the main JsonNode with the value from the update JsonNode
              ((ObjectNode) dataNode).set(fieldName, updateDesignationDetails.get(fieldName));
            } else {
              ((ObjectNode) dataNode).put(fieldName, updateDesignationDetails.get(fieldName));
            }
          }
          designationEntiy.get().setUpdatedOn(currentTime);
          ((ObjectNode) dataNode).put(Constants.UPDATED_ON, new TextNode(
                  convertTimeStampToDate(designationEntiy.get().getUpdatedOn().getTime())));
          designationEntityUpdated = designationRepository.save(designationEntiy.get());
          ObjectNode jsonNode = objectMapper.createObjectNode();
          jsonNode.set(Constants.ID,
                  new TextNode(updateDesignationDetails.get(Constants.ID).asText()));
          jsonNode.setAll((ObjectNode) designationEntityUpdated.getData());
          jsonNode.set(Constants.UPDATED_ON, new TextNode(
                  convertTimeStampToDate(designationEntityUpdated.getUpdatedOn().getTime())));
          Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
          esUtilService.updateDocument(Constants.INDEX_NAME_FOR_ORG_BOOKMARK, Constants.INDEX_TYPE,
                  designationEntityUpdated.getId(), map,
                  cbServerProperties.getElasticBookmarkJsonPath());
          cacheService.putCache(designationEntityUpdated.getId(),
                  designationEntityUpdated.getData());
          log.info("updated the Designation");
          response.setMessage(Constants.SUCCESSFULLY_CREATED);
          map.put(Constants.ID, designationEntityUpdated.getId());
          response.setResult(map);
          response.setResponseCode(HttpStatus.OK);
          log.info("InterestServiceImpl::createInterest::persited interest in Pores");
          return response;
        }
      }
    } catch (Exception e) {
      log.error("Error while processing file: {}", e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
    return null;
  }

  private String convertTimeStampToDate(long timeStamp) {
    Instant instant = Instant.ofEpochMilli(timeStamp);
    OffsetDateTime dateTime = instant.atOffset(ZoneOffset.UTC);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss.SSS'Z'");
    return dateTime.format(formatter);
  }

}
