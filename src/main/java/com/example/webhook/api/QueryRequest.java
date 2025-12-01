package com.example.webhook.api;

public class QueryRequest {

    private String finalQuery;

    public QueryRequest() {
    }

    public QueryRequest(String finalQuery) {
        this.finalQuery = finalQuery;
    }

    public String getFinalQuery() {
        return finalQuery;
    }

    public void setFinalQuery(String finalQuery) {
        this.finalQuery = finalQuery;
    }
}

