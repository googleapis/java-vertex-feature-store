# Vertex AI Feature Store Bigtable Gateway

Custom Java library for accessing directly online store(Bigtable).

By Vertex AI Feature Store

## Getting Started

This feature is disabled by default. To enable it for your project,
please contact vertex-feature-store-feedback@google.com.

## Add dependency

If you're using Maven, add the following to your dependencies:

[//]: # ({x-version-update-start:google-genai:released})
```xml
<dependency>
  <groupId>com.google.cloud.aiplatform.fs</groupId>
  <artifactId>google-cloud-aiplatform-fs</artifactId>
  <version>3.61.0</version>
  <exclusions>
    <exclusion>
      <groupId>com.google.api.grpc</groupId>
      <artifactId>proto-google-cloud-aiplatform-v1</artifactId>
    </exclusion>
    <exclusion>
      <groupId>com.google.api.grpc</groupId>
      <artifactId>grpc-google-cloud-aiplatform-v1</artifactId>
    </exclusion>
    <exclusion>
      <groupId>com.google.cloud</groupId>
      <artifactId>gapic-google-cloud-aiplatform-v1</artifactId>
    </exclusion>
  </exclusions>
</dependency>

```

### Create a client and call FetchFeatureValues APIs

```java
import com.google.cloud.aiplatform.fs.FeatureOnlineStoreDirectClient;
import com.google.cloud.aiplatform.v1.FeatureValue;
import com.google.cloud.aiplatform.v1.FeatureView;
import com.google.cloud.aiplatform.v1.FeatureViewDataFormat;
import com.google.cloud.aiplatform.v1.FeatureViewDataKey;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesRequest;
import com.google.cloud.aiplatform.v1.FetchFeatureValuesResponse;

public class DirectReadExample {

  private static final String FEATURE_VIEW_NAME =
      "projects/<PROJECT>/locations/<LOCATION>/featureOnlineStores/<FOS>/featureViews/<FV>";
  private FeatureOnlineStoreDirectClient client;

  DirectReadExample() {
    try {
      client = FeatureOnlineStoreDirectClient.create(FEATURE_VIEW_NAME);
    } catch (Exception e) {
      System.out.println("Could not create client" + e);
    }
  }

  public void fetchFeatureValuesSync(String key) {
    FetchFeatureValuesRequest request =
        FetchFeatureValuesRequest.newBuilder()
            .setFeatureView(FEATURE_VIEW_NAME)
            .setDataFormat(FeatureViewDataFormat.KEY_VALUE)
            .setDataKey(FeatureViewDataKey.newBuilder().setKey(key).build())
            .build();
    try {
      FetchFeatureValuesResponse response = client.fetchFeatureValues(request);
      System.out.println(response.toString());
    } catch (Exception e) {
      System.out.println("Error fetching features");
    }
  }

  public static void main(String[] args) {
    DirectReadExample ex = new DirectReadExample();
    String key = "your data key to read from FeatureView";
    ex.fetchFeatureValuesSync(key);
    System.out.println("Test done");
  }
}
```

### Configure Bigtable connection

Below code is an example for configuring Bigtable connection.

```java
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.aiplatform.fs.DirectClientSettings;
import com.google.cloud.aiplatform.fs.FeatureOnlineStoreDirectClient;

public class DirectReadExample {

  private static final String FEATURE_VIEW_NAME =
      "projects/<PROJECT>/locations/<LOCATION>/featureOnlineStores/<FOS>/featureViews/<FV>";
  private FeatureOnlineStoreDirectClient client;

  DirectReadExample() {
    // Add configuration for the channel pool.
    ChannelPoolSettings channelPoolSettings =
        ChannelPoolSettings.builder()
            .setInitialChannelCount(3)
            .setMaxChannelCount(10)
            .setMinChannelCount(3)
            .build();
    // Add configuration for the retry behavior.
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelayDuration(Duration.ofSeconds(1))
            .setMaxAttempts(10)
            .setMaxRetryDelayDuration(Duration.ofSeconds(5))
            .build();

    DirectClientSettings settings =
        new DirectClientSettings.Builder()
            .setChannelPoolSettings(channelPoolSettings)
            .setRetrySettings(retrySettings)
            // Configure which error codes to enable retries.
            .setRetryableCodes(Code.ABORTED, Code.INTERNAL, Code.CANCELLED)
            .build();

    try {
      client = FeatureOnlineStoreDirectClient.create(FEATURE_VIEW_NAME, settings);
    } catch (Exception e) {
      System.out.println("Could not create client" + e);
    }
  }
}

```

## Contribute to this library

This Vertex AI Feature Store Java SDK will not accept contributions.



## License

Apache 2.0 - See [LICENSE][license] for more information.
