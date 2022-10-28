package com.meta.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;


/**
 * FileUtils
 *
 * @author weitaoliang
 * @version V1.0
 */
public class FileUtils {

    /**
     * Loads the configuration file for the application
     */
    private Properties loadPropertiesFile(String propertiesFile) throws IOException {
        Properties props = new Properties();
        File propsFile;
        propsFile = new File(propertiesFile);
        if (propsFile.isFile()) {
            try (InputStreamReader isr = new InputStreamReader(
                    new FileInputStream(propsFile), StandardCharsets.UTF_8)) {
                props.load(isr);
                for (Map.Entry<Object, Object> e : props.entrySet()) {
                    e.setValue(e.getValue().toString().trim());
                }
            }
        }
        return props;
    }
}
