package site.ycsb.db.couchbase3;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

class RESTException extends Exception {
  private Integer code = 0;

  public RESTException(Integer code, String message) {
    super(message);
    this.code = code;
  }

  public RESTException(String message) {
    super(message);
  }
}

/**
 * Connect To REST Interface.
 */
public class RESTInterface {
  private String hostname;
  private String username;
  private String password;
  private String token = null;
  private Boolean useSsl;
  private Integer port;
  private String urlPrefix;
  private OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
  private OkHttpClient client;
  private Authenticator authenticator;
  private SSLContext sslContext;
  public static final String DEFAULT_HTTP_PREFIX = "http://";
  public static final String DEFAULT_HTTPS_PREFIX = "https://";

  public RESTInterface(String hostname, String username, String password, Boolean useSsl, Integer port) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = port;
    this.init();
  }

  public RESTInterface(String hostname, String username, String password, Boolean useSsl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    if (useSsl) {
      this.port = 443;
    } else {
      this.port = 80;
    }
    this.init();
  }

  public RESTInterface(String hostname, String token, Boolean useSsl) {
    this.hostname = hostname;
    this.token = token;
    this.useSsl = useSsl;
    if (useSsl) {
      this.port = 443;
    } else {
      this.port = 80;
    }
    this.init();
  }

  public void init() {
    StringBuilder connectBuilder = new StringBuilder();
    String urlProtocol;
    if (useSsl) {
      urlProtocol = DEFAULT_HTTPS_PREFIX;
    } else {
      urlProtocol = DEFAULT_HTTP_PREFIX;
    }
    connectBuilder.append(urlProtocol);
    connectBuilder.append(hostname);
    connectBuilder.append(":");
    connectBuilder.append(port.toString());
    urlPrefix = connectBuilder.toString();

    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          @Override
          public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
          }

          @Override
          public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
          }

          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[]{};
          }
        }
    };

    try {
      sslContext = SSLContext.getInstance("SSL");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    try {
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (KeyManagementException e) {
      throw new RuntimeException(e);
    }

    clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
    clientBuilder.hostnameVerifier((hostname, session) -> true);

    authenticator = (route, response) -> {
      if (response.request().header("Authorization") != null) {
        return null;
      }
      if (token != null ) {
        return response.request().newBuilder()
            .header("Authorization", "Bearer " + token)
            .build();
      } else {
        String credential = Credentials.basic(username, password);
        return response.request().newBuilder()
            .header("Authorization", credential)
            .build();
      }
    };

    clientBuilder.authenticator(authenticator);

    client = clientBuilder.build();
  }

  public JsonObject getJSON(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    Request request = new Request.Builder()
        .url(url)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new RESTException(response.code(), response.toString());
      if (response.body() != null) {
        String responseText = response.body().string();
        return JsonParser.parseString(responseText).getAsJsonObject();
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
    return new JsonObject();
  }

  public List<JsonElement> getCapella(String endpoint) {
    JsonObject response;
    List<JsonElement> data = new ArrayList<>();
    try {
      response = getJSON(endpoint);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
    if (response.has("cursor")) {
      if (response.getAsJsonObject("cursor").has("pages")) {
        if (response.getAsJsonObject("cursor")
            .getAsJsonObject("pages")
            .has("next")) {
          String nextPage = response.getAsJsonObject("cursor")
              .getAsJsonObject("pages").get("next").getAsString();
          String perPage = response.getAsJsonObject("cursor")
              .getAsJsonObject("pages").get("perPage").getAsString();
          if (Integer.parseInt(nextPage) > 0) {
            URL url;
            try {
              url = new URL(urlPrefix + endpoint);
            } catch (MalformedURLException e) {
              throw new RuntimeException(e);
            }
            String newEndpoint = url.getPath() + "?page=" + nextPage + "&perPage=" + perPage;
            List<JsonElement> nextData = getCapella(newEndpoint);
            data.addAll(nextData);
          }
        }
      }
    }
    data.addAll(response.get("data").getAsJsonArray().asList());
    return data;
  }
}
