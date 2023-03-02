package com.microsoft.solutions.keyvaultcrypto;

import org.apache.spark.sql.api.java.UDF2;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.core.exception.HttpResponseException;
import com.azure.identity.DefaultAzureCredential;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import java.util.Base64;
import java.lang.Math;

public class doDecryption implements UDF2<String, String, String>{

    private DefaultAzureCredential credential;
    private CryptographyClient cryptoClient;

    private Integer maxDecryptionAttempts = 4;

    @Override
    public String call(String encryptedText, String keyURI) throws Exception
    {
        Double attempts = 0.0;
        Double base = 2.0;
        Boolean ok = false;
        String plaintext = null;
        if (credential == null) {
            credential = new DefaultAzureCredentialBuilder().build();
        }
        if (cryptoClient == null) {
            cryptoClient =  new CryptographyClientBuilder().keyIdentifier(keyURI).credential(credential).buildClient();
            
        }
        byte[] cipherText = Base64.getDecoder().decode(encryptedText);
        do {
            try {
                plaintext = new String(cryptoClient.decrypt(EncryptionAlgorithm.fromString("RSA1_5"), cipherText).getPlainText());
                
                ok = true;
            } catch(HttpResponseException e) {
                System.out.println("Caught HttpResponseException!");
                attempts++;
                Double milliseconds = Math.pow(base, attempts) * 1000;
                Thread.sleep(milliseconds.longValue());
                System.out.println(String.format("Waiting %f ms before tyring again...",milliseconds));
            }
        } while (ok == false && attempts <= maxDecryptionAttempts);
        return plaintext;
    }
}