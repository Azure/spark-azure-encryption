package com.microsoft.solutions.keyvaultcrypto;

import org.apache.spark.sql.api.java.UDF2;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import java.util.Base64;

public class doEncryption implements UDF2<String, String, String>{

    private DefaultAzureCredential credential;
    private CryptographyClient cryptoClient;

    @Override
    public String call(String plaintext, String keyURI) throws Exception
    {
        if (credential == null) {
            credential = new DefaultAzureCredentialBuilder().build();
        }
        if (cryptoClient == null) {
            cryptoClient =  new CryptographyClientBuilder().keyIdentifier(keyURI).credential(credential).buildClient();
        }
        byte[] cipherText = cryptoClient.encrypt(EncryptionAlgorithm.fromString("RSA1_5"), plaintext.getBytes()).getCipherText();
        String base64Text = Base64.getEncoder().encodeToString(cipherText);
        return base64Text;
    }

}