package com.example.webhook.service;

import com.example.webhook.api.GenerateWebhookRequest;
import com.example.webhook.api.GenerateWebhookResponse;
import com.example.webhook.api.QueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class WebhookService {

    private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

    private static final String FINAL_SQL_QUERY =
        "WITH filtered_payments AS (\n" +
        "    SELECT \n" +
        "        p.EMP_ID,\n" +
        "        SUM(p.AMOUNT) AS total_salary\n" +
        "    FROM PAYMENTS p\n" +
        "    WHERE EXTRACT(DAY FROM p.PAYMENT_TIME) <> 1\n" +
        "    GROUP BY p.EMP_ID\n" +
        "),\n" +
        "ranked AS (\n" +
        "    SELECT \n" +
        "        d.DEPARTMENT_NAME,\n" +
        "        fp.total_salary AS SALARY,\n" +
        "        CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME,\n" +
        "        FLOOR(DATEDIFF(CURDATE(), e.DOB) / 365) AS AGE,\n" +
        "        ROW_NUMBER() OVER (\n" +
        "            PARTITION BY d.DEPARTMENT_ID \n" +
        "            ORDER BY fp.total_salary DESC\n" +
        "        ) AS rn\n" +
        "    FROM filtered_payments fp\n" +
        "    JOIN EMPLOYEE e ON fp.EMP_ID = e.EMP_ID\n" +
        "    JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID\n" +
        ")\n" +
        "SELECT \n" +
        "    DEPARTMENT_NAME,\n" +
        "    SALARY,\n" +
        "    EMPLOYEE_NAME,\n" +
        "    AGE\n" +
        "FROM ranked\n" +
        "WHERE rn = 1;";

    private final RestTemplate restTemplate;
    private final String generateWebhookUrl;
    private final String candidateName;
    private final String candidateRegNo;
    private final String candidateEmail;

    public WebhookService(
            RestTemplate restTemplate,
            @Value("${app.generateWebhook.url}") String generateWebhookUrl,
            @Value("${app.candidate.name}") String candidateName,
            @Value("${app.candidate.regNo}") String candidateRegNo,
            @Value("${app.candidate.email}") String candidateEmail) {

        this.restTemplate = restTemplate;
        this.generateWebhookUrl = generateWebhookUrl;
        this.candidateName = candidateName;
        this.candidateRegNo = candidateRegNo;
        this.candidateEmail = candidateEmail;
    }

    public void executeFlowOnStartup() {
        log.info("Starting webhook flow...");

        GenerateWebhookResponse response = generateWebhook();
        if (response == null || response.getWebhook() == null || response.getAccessToken() == null) {
            log.error("Failed to get webhook URL or access token.");
            return;
        }

        log.info("Webhook URL received: {}", response.getWebhook());
        log.info("AccessToken length: {}", response.getAccessToken().length());

        String finalSqlQuery = FINAL_SQL_QUERY;

        submitFinalQuery(response.getWebhook(), response.getAccessToken(), finalSqlQuery);
    }

    private GenerateWebhookResponse generateWebhook() {
        try {
            GenerateWebhookRequest requestBody =
                    new GenerateWebhookRequest(candidateName, candidateRegNo, candidateEmail);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<GenerateWebhookRequest> entity = new HttpEntity<>(requestBody, headers);

            ResponseEntity<GenerateWebhookResponse> response =
                    restTemplate.postForEntity(generateWebhookUrl, entity, GenerateWebhookResponse.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                return response.getBody();
            } else {
                log.error("generateWebhook failed with status: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error during generateWebhook call", e);
        }
        return null;
    }

    private void submitFinalQuery(String webhookUrl, String accessToken, String finalQuery) {
    try {
        QueryRequest body = new QueryRequest(finalQuery);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String authHeader;
        if (accessToken.startsWith("Bearer ")) {
            authHeader = accessToken;
        } else {
            authHeader = accessToken; 
        }

        log.info("Using Authorization header prefix: {}",
                authHeader.length() > 10 ? authHeader.substring(0, 10) + "..." : authHeader);

        headers.set(HttpHeaders.AUTHORIZATION, authHeader);

        HttpEntity<QueryRequest> entity = new HttpEntity<>(body, headers);

        ResponseEntity<String> response =
                restTemplate.postForEntity(webhookUrl, entity, String.class);

        log.info("Submitted final query. Status: {}", response.getStatusCode());
        log.info("Response from webhook: {}", response.getBody());

    } catch (Exception e) {
        log.error("Error submitting final query", e);
    }
  }
  
}

