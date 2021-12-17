package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;
import sun.net.www.http.HttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

// class name must match plugin name
@LogstashPlugin(name = "drain3_log_filter")
public class Drain3LogFilter implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "message");

    private String id;
    private String sourceField;

    public Drain3LogFilter(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        String host = System.getenv("DRAIN3_SERVER");
        if (host == null) {
            return events;
        }
        for (Event e : events) {
            // check if msg is available
            // else use message
            // call python webserver
            // set new field 'category'

            Object msg = e.getField("msg");
            if (msg == null) {
                msg = e.getField("message");
            }
            if (msg instanceof String) {
                try {
                    URL url = new URL(host + "?message="+ URLEncoder.encode((String)msg, StandardCharsets.UTF_8.name()));
                    HttpURLConnection con = (HttpURLConnection) url.openConnection();
                    con.setRequestMethod("GET");
                    con.setDoInput(true);

                    BufferedReader in = new BufferedReader(new InputStreamReader(
                            con.getInputStream()));
                    String template = in.readLine();
                    in.close();
                    con.disconnect();
                    e.setField("template", template);
                    matchListener.filterMatched(e);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

            }
        }
        return events;
    }

    //  elasticsearch {
    //    hosts => ["http://localhost:9200"]
    //    index => "%{[@metadata][beat]}-%{[@metadata][version]}"
    //  }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Collections.singletonList(SOURCE_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
