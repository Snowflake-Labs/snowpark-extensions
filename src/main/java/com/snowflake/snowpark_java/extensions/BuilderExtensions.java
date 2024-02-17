package com.snowflake.snowpark_java.extensions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BuilderExtensions {
public static Map<String, String> read_snowsql_config(String sectionName) throws IOException {
    String homeDir = System.getProperty("user.home");
    String filePath = homeDir + "/.snowsql/config";
    
    Map<String, String> properties = new HashMap<>();

    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
        String line;
        String currentSection = null;

        while ((line = br.readLine()) != null) {
            line = line.trim();

            if (line.startsWith("[") && line.endsWith("]")) {
                currentSection = line.substring(1, line.length() - 1);
            } else if (currentSection != null && currentSection.equals(sectionName) && line.contains("=")) {
                String[] parts = line.split("=");
                String key = parts[0].trim();
                String value = parts[1].trim();
                properties.put(key, value);
            }
        }
    }

    return properties;
}
}