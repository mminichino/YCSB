package site.ycsb.db.couchbase3;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.jetbrains.annotations.NotNull;
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
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Connect To REST Interface.
 */
public class RESTInterface {
  protected static final ch.qos.logback.classic.Logger LOGGER =
          (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("site.ycsb.db.couchbase3.RESTInterface");
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

  public RESTInterface(String hostname, String username, String password, Boolean useSsl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = useSsl ? 443 : 80;
    this.enableDebug = false;
    this.init();
  }

  public RESTInterface(String hostname, String token, Boolean useSsl) {
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

  public RESTInterface get(HttpUrl url) {
      execHttpCall(buildGetRequest(url));
      return this;
  }

  public RESTInterface post(String endpoint) {
    execHttpCall(buildPostRequest(endpoint, requestBody));
    return this;
  }

  public RESTInterface delete(String endpoint) {
    execHttpCall(buildDeleteRequest(endpoint));
    return this;
  }

  public JsonObject json() {
    Gson gson = new Gson();
    return gson.fromJson(new String(responseBody), JsonObject.class);
  }

  public RESTInterface jsonBody(JsonObject json) {
    requestBody = RequestBody.create(json.toString(), JSON);
    return this;
  }

  public int code() {
    return responseCode;
  }

  public RESTInterface validate() {
    if (responseCode >= 300 ) {
      try {
        Gson gson = new Gson();
        JsonObject response = gson.fromJson(new String(responseBody), JsonObject.class);
        String message = "Can not access Capella API: Response Code: " + responseCode;
        if (response.has("hint")) {
          message = message + " Hint: " + response.get("hint").getAsString();
        }
        throw new RuntimeException(message);
      } catch (JsonSyntaxException e) {
        throw new RuntimeException("Invalid response from API endpoint: response code: " + responseCode);
      }
    }
    return this;
  }

  public List<JsonElement> getCapella(String endpoint) {
    HttpUrl url = buildUrl(endpoint);
    JsonObject response = get(url).validate().json();
    return new ArrayList<>(response.get("data").getAsJsonArray().asList());
  }

  public HttpUrl buildUrl(String endpoint) {
    HttpUrl.Builder builder = new HttpUrl.Builder();
    return builder.scheme(useSsl ? "https" : "http")
            .host(hostname)
            .port(port)
            .addPathSegment(endpoint)
            .build();
  }

  public Request buildGetRequest(HttpUrl url) {
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

  public HttpUrl buildPageUrl(String endpoint, int page, int perPage) {
    HttpUrl.Builder builder = new HttpUrl.Builder();
    return builder.scheme(useSsl ? "https" : "http")
            .host(hostname)
            .port(port)
            .addPathSegment(endpoint)
            .addQueryParameter("page", String.valueOf(page))
            .addQueryParameter("perPage", String.valueOf(perPage))
            .build();
  }

  public Request buildPageRequest(String endpoint, int page, int perPage) {
    HttpUrl url = buildPageUrl(endpoint, page, perPage);
    return new Request.Builder()
            .url(url)
            .header("Authorization", credential)
            .build();
  }

  public List<JsonElement> getCapellaList(String endpoint) {
    HttpUrl url = buildPageUrl(endpoint, 1, 10);
    JsonObject cursor = get(url).validate().json();

    int totalItems = cursor.get("cursor").getAsJsonObject().get("pages").getAsJsonObject().get("totalItems").getAsInt();
    int pages = (int) Math.ceil(totalItems / 10.0);
    List<JsonElement> data = new ArrayList<>(cursor.get("data").getAsJsonArray().asList());

    CountDownLatch countDownLatch = new CountDownLatch(pages);
    for (int i=2; i <= pages; i++) {
      Request request = buildPageRequest(endpoint, i, 10);
      client.newCall(request).enqueue(new Callback() {

        @Override
        public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
          if (!response.isSuccessful()) {
            LOGGER.error("Error: " + response.code() + ": " + response);
          } else if (response.body() != null) {
            String responseText = response.body().string();
            Gson gson = new Gson();
            JsonObject part =  gson.fromJson(responseText, JsonObject.class);
            data.addAll(part.get("data").getAsJsonArray().asList());
          }
          countDownLatch.countDown();
        }

        @Override
        public void onFailure(@NotNull Call call, @NotNull IOException e) {
          LOGGER.error(e.getMessage(), e);
          countDownLatch.countDown();
        }
      });
    }
    countDownLatch.countDown();

    try {
      countDownLatch.await();
      client.dispatcher().executorService().shutdown();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return data;
  }
}
