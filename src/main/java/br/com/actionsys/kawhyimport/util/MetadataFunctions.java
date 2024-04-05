package br.com.actionsys.kawhyimport.util;

public class MetadataFunctions {

    public static final String FUNCTION_PREFIX = "${";
    public static final String GENERATE_NUMBER_NFSE = "${generateNfseNumber}";
    public static final String COMP_WHERE = "${compWhere}";
    public static final String PARENT_SEQUENCE = "${parentSequence}";
    public static final String SEQUENCE = "${sequence}";
    public static final String DOCUMENT_ID = "${chaveDeAcesso}";
    public static final String GET_CNPJ_TOMADOR = "${getCnpjTomador}"; //TODO IMPLE
    public static final String FIXED_VALUE_PREFIX = FUNCTION_PREFIX + "'";
    public static final String AUDIT_HOST = "${auditHost}";
    public static final String AUDIT_DATE = "${auditDate}";
    public static final String AUDIT_TIME = "${auditTime}";
    public static final String AUDIT_SERVICE_NAME = "${auditServiceName}";
    public static final String AUDIT_USER = "${auditUser}";
}
