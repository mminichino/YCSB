package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

class RESTException extends Exception {
  private Integer code = 0;

  public RESTException(Integer code, String message) {
    super(message);
    this.code = code;
  }

  public RESTException(String message) {
    super(message);
  }

  public Integer getCode() {
    return code;
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
  private String credential;
  private boolean enableDebug = false;
  public static final String DEFAULT_HTTP_PREFIX = "http://";
  public static final String DEFAULT_HTTPS_PREFIX = "https://";
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

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

    if (token != null) {
      credential = "Bearer " + token;
    } else {
      credential = Credentials.basic(username, password);
    }

    if (enableDebug) {
      HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
      logging.setLevel(HttpLoggingInterceptor.Level.HEADERS);
      clientBuilder.addInterceptor(logging);
    }

    client = clientBuilder.build();
  }

  public JsonObject getJSON(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    Request request = new Request.Builder()
        .url(url)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
      if (response.body() != null) {
        String responseText = response.body().string();
        Gson gson = new Gson();
        return gson.fromJson(responseText, JsonObject.class);
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
    return new JsonObject();
  }

  public JsonArray getJSONArray(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    Request request = new Request.Builder()
        .url(url)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
      if (response.body() != null) {
        String responseText = response.body().string();
        Gson gson = new Gson();
        return gson.fromJson(responseText, JsonArray.class);
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
    return new JsonArray();
  }

  public HashMap<?, ?> getMap(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    Request request = new Request.Builder()
        .url(url)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
      if (response.body() != null) {
        String responseText = response.body().string();
        return new ObjectMapper().readValue(responseText, HashMap.class);
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
    return new HashMap<>();
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

  public String postJSON(String endpoint, JsonObject json) throws RESTException {
    String url = urlPrefix + endpoint;
    RequestBody body = RequestBody.create(json.toString(), JSON);
    Request request = new Request.Builder()
        .url(url)
        .post(body)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
      return response.body() != null ? response.body().string() : null;
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
  }

  public JsonArray postJSONArray(String endpoint, JsonArray json) throws RESTException {
    return postJSONString(endpoint, json.toString());
  }

  public JsonArray postJSONString(String endpoint, String json) throws RESTException {
    String url = urlPrefix + endpoint;
    RequestBody body = RequestBody.create(json, JSON);
    Request request = new Request.Builder()
        .url(url)
        .post(body)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
      Gson gson = new Gson();
      return response.body() != null ? gson.fromJson(response.body().string(), JsonArray.class) : new JsonArray();
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
  }

  public void postEndpoint(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    RequestBody body = RequestBody.create(new JsonObject().toString(), JSON);
    Request request = new Request.Builder()
        .url(url)
        .post(body)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
  }

  public void postParameters(String endpoint, Map<String, String> params) throws RESTException {
    String url = urlPrefix + endpoint;
    RequestBody body = RequestBody.create(new JsonObject().toString(), JSON);

    HttpUrl.Builder httpBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();
    for(Map.Entry<String, String> param : params.entrySet()) {
      httpBuilder.addQueryParameter(param.getKey(), param.getValue());
    }

    Request request = new Request.Builder()
        .url(httpBuilder.build())
        .post(body)
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
  }

  public void deleteEndpoint(String endpoint) throws RESTException {
    String url = urlPrefix + endpoint;
    Request request = new Request.Builder()
        .url(url)
        .delete()
        .header("Authorization", credential)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RESTException(response.code(), response.toString());
      }
    } catch (IOException e) {
      throw new RESTException(e.getMessage());
    }
  }
}
