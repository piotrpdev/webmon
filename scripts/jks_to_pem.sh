#!/bin/bash
# NOT MADE BY ME: https://dev.to/adityakanekar/connecting-to-kafka-cluster-using-ssl-with-python-k2e
srcFolder=$1
keyStore=$1/$2
password=$3
alias=$4
outputFolder=$5

echo $keyStore
echo "Generating certificate.pem"
keytool -exportcert -alias $alias -keystore $keyStore -rfc -file $outputFolder/certificate.pem -storepass $password

echo "Generating key.pem"
keytool -v -importkeystore -srckeystore $keyStore -srcalias $alias -destkeystore $outputFolder/cert_and_key.p12 -deststoretype PKCS12 -storepass $password -srcstorepass $password
openssl pkcs12 -in $outputFolder/cert_and_key.p12 -nodes -nocerts -out $outputFolder/key.pem -passin pass:$password

echo "Generating CARoot.pem"
keytool -exportcert -alias $alias -keystore $keyStore -rfc -file $outputFolder/CARoot.pem -storepass $password