package org.apache.rocketmq.common.logging;

import com.google.common.io.CharStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.logging.ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.rocketmq.logging.ch.qos.logback.core.joran.spi.JoranException;

public class JoranConfiguratorExt extends JoranConfigurator {
    private InputStream transformXml(InputStream in) throws IOException {
        try {
            String str = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
            str = str.replace("\"ch.qos.logback", "\"org.apache.rocketmq.logging.ch.qos.logback");
            return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
        } finally {
            if (null != in) {
                in.close();
            }
        }
    }

    public final void doConfigure0(URL url) throws JoranException {
        InputStream in = null;
        try {
            informContextOfURLUsedForConfiguration(getContext(), url);
            URLConnection urlConnection = url.openConnection();
            // per http://jira.qos.ch/browse/LBCORE-105
            // per http://jira.qos.ch/browse/LBCORE-127
            urlConnection.setUseCaches(false);

            InputStream temp = urlConnection.getInputStream();
            in = transformXml(temp);

            doConfigure(in, url.toExternalForm());
        } catch (IOException ioe) {
            String errMsg = "Could not open URL [" + url + "].";
            addError(errMsg, ioe);
            throw new JoranException(errMsg, ioe);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {
                    String errMsg = "Could not close input stream";
                    addError(errMsg, ioe);
                    throw new JoranException(errMsg, ioe);
                }
            }
        }
    }
}
