#################################
# SECURITY.PROPERTIES           #
#################################

# Location of files used for security. 
security.dir=@{SECURITY_DIRECTORY}

# RMI + JMX authentication and encryption parameters
security.rmi.authentication=@{ENABLE_RMI_AUTHENTICATION}
security.rmi.tungsten.authenticationRealm=true
security.rmi.tungsten.authenticationRealm.encrypted.password=true
security.rmi.encryption=@{ENABLE_RMI_SSL}
security.rmi.authentication.username=@{RMI_USER}

# Password and access file
security.password_file.location=@{JAVA_PASSWORDSTORE_PATH}
security.access_file.location=@{JAVA_JMXREMOTE_ACCESS_PATH}

# Keystore and Truststore for SSL and encryption
security.keystore.location=@{JAVA_KEYSTORE_PATH}
security.keystore.password=@{JAVA_KEYSTORE_PASSWORD}
security.truststore.location=@{JAVA_TRUSTSTORE_PATH}
security.truststore.password=@{JAVA_TRUSTSTORE_PASSWORD}

# Keystore and Truststore for the Tungsten Connector
connector.security.use.ssl=@{ENABLE_CONNECTOR_CLIENT_SSL}
connector.security.keystore.location=@{JAVA_CONNECTOR_KEYSTORE_PATH}
connector.security.keystore.password=@{JAVA_CONNECTOR_KEYSTORE_PASSWORD}
connector.security.truststore.location=@{JAVA_CONNECTOR_TRUSTSTORE_PATH}
connector.security.truststore.password=@{JAVA_CONNECTOR_TRUSTSTORE_PASSWORD}
