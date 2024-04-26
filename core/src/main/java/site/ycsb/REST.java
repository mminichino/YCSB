package site.ycsb;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;

/**
 * Connect To REST Interface.
 */
public class REST {
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("site.ycsb.REST");
  private final String hostname;
  private String username;
  private String password;
  private String token = null;
  private final Boolean useSsl;
  private final Integer port;
  private final OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
  private OkHttpClient client;
  private String credential;
  private final boolean enableDebug;
  public int responseCode;
  public byte[] responseBody;
  public RequestBody requestBody;
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  public REST(String hostname, String username, String password, Boolean useSsl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = useSsl ? 443 : 80;
    this.enableDebug = false;
    this.init();
  }

  public REST(String hostname, String username, String password, Boolean useSsl, Integer port) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = port;
    this.enableDebug = false;
    this.init();
  }

  public REST(String hostname, String token, Boolean useSsl) {
    this.hostname = hostname;
    this.token = token;
    this.useSsl = useSsl;
    this.enableDebug = false;
    this.port = useSsl ? 443 : 80;
    this.init();
  }

  public void init() {
    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType) {
          }

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType) {
          }

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
          }
        }
    };

    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance("SSL");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    try {
      sslContext.init(null, trustAllCerts, new SecureRandom());
    } catch (KeyManagementException e) {
      throw new RuntimeException(e);
    }

    clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
    clientBuilder.hostnameVerifier((hostname, session) -> true);
    clientBuilder.connectTimeout(Duration.ofSeconds(20));
    clientBuilder.readTimeout(Duration.ofSeconds(20));
    clientBuilder.writeTimeout(Duration.ofSeconds(20));

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

  private void execHttpCall(Request request) {
    try {
      try (Response response = client.newCall(request).execute()) {
        responseCode = response.code();
        responseBody = response.body() != null ? response.body().bytes() : new byte[0];
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public REST get(String endpoint) {
    execHttpCall(buildGetRequest(endpoint));
    return this;
  }

  public REST post(String endpoint) {
    execHttpCall(buildPostRequest(endpoint, requestBody));
    return this;
  }

  public REST delete(String endpoint) {
    execHttpCall(buildDeleteRequest(endpoint));
    return this;
  }

  public JsonObject json() {
    Gson gson = new Gson();
    return gson.fromJson(new String(responseBody), JsonObject.class);
  }

  public JsonArray jsonArray() {
    Gson gson = new Gson();
    return gson.fromJson(new String(responseBody), JsonArray.class);
  }

  public REST jsonBody(JsonObject json) {
    requestBody = RequestBody.create(json.toString(), JSON);
    return this;
  }

  public int code() {
    return responseCode;
  }

  public REST validate() {
    if (responseCode >= 300) {
      String response = new String(responseBody);
      throw new RuntimeException(
          "Invalid response from API endpoint: response code: " + responseCode + " Response: " + response
      );
    }
    return this;
  }

  public boolean waitForJsonValue(String endpoint, String key, String value, int retryCount) {
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      JsonObject response = get(endpoint).validate().json();
      String result = response.get(key).getAsString();
      if (result.equals(value)) {
        return true;
      }
      try {
        Thread.sleep(waitFactor);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  public boolean waitForCode(String endpoint, int code, int retryCount) {
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      int result = get(endpoint).code();
      if (result == code) {
        return true;
      }
      try {
        Thread.sleep(waitFactor);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  public HttpUrl buildUrl(String endpoint) {
    HttpUrl.Builder builder = new HttpUrl.Builder();
    return builder.scheme(useSsl ? "https" : "http")
        .host(hostname)
        .port(port)
        .addPathSegment(endpoint)
        .build();
  }

  public Request buildGetRequest(String endpoint) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .header("Authorization", credential)
        .build();
  }

  public Request buildPostRequest(String endpoint, RequestBody body) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .post(body)
        .header("Authorization", credential)
        .build();
  }

  public Request buildDeleteRequest(String endpoint) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .delete()
        .header("Authorization", credential)
        .build();
  }

}
