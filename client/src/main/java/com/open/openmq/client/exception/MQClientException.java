package com.open.openmq.client.exception;

import com.open.openmq.common.UtilAll;

/**
 * @Description TODO
 * @Date 2023/1/30 23:56
 * @Author jack wu
 */
public class MQClientException extends Exception {
    private static final long serialVersionUID = -5758410930844185841L;
    private int responseCode;
    private String errorMessage;

    public MQClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage) {
        super("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage, Throwable cause) {
        super("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage, cause);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public MQClientException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
