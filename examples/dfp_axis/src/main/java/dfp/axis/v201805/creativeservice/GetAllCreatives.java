// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dfp.axis.v201805.creativeservice;

import static com.google.api.ads.common.lib.utils.Builder.DEFAULT_CONFIGURATION_FILENAME;

// basic shit
import java.util.ArrayList;
import java.util.List;

import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.conf.ConfigurationLoadException;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.ads.dfp.axis.factory.DfpServices;
import com.google.api.ads.dfp.axis.utils.v201805.StatementBuilder;
import com.google.api.ads.dfp.axis.v201805.ApiError;
import com.google.api.ads.dfp.axis.v201805.ApiException;
import com.google.api.ads.dfp.axis.v201805.Creative;
import com.google.api.ads.dfp.axis.v201805.CreativePage;
import com.google.api.ads.dfp.axis.v201805.CreativeServiceInterface;
import com.google.api.ads.dfp.lib.client.DfpSession;
import com.google.api.client.auth.oauth2.Credential;
import java.rmi.RemoteException;

// beam sdk libs
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.testing.PAssert;


/**
 * This example gets all creatives. To create creatives, run
 * CreateCreatives.java.
 *
 * Credentials and properties in {@code fromFile()} are pulled from the
 * "ads.properties" file. See README for more info.
 */
public class GetAllCreatives {

  /**
   * Runs the example.
   *
   * @param dfpServices the services factory.
   * @param session the session.
   * @throws ApiException if the API request failed with one or more service errors.
   * @throws RemoteException if the API request failed due to other errors.
   */
  public static List<Creative> GetCreatives(DfpServices dfpServices, DfpSession session)
  throws RemoteException {
    // Get the CreativeService.
    CreativeServiceInterface creativeService =
    dfpServices.get(session, CreativeServiceInterface.class);

    String creativeTypeSubFilter = String.format("creativeType = %s", "ImageCreative");

    // advertiserID is the platform UID of the datasource
    String advertiserIdSubFilter = String.format("advertiserId = %s", "751815071");

    // This needs to be retrieved from the
    String[] creativeIdsArray = {"138230331327","138230331330","138230332206","138230332590","138230379977","138230380889","138230385146","138230385368","138230385371","138230386025","138230714167","138230714170","138230714173","138231410126","138231410165","138231731145","138231731877","138231810821","138231811019","138231811022","138231855280"};
    String creativeIdSubFilter = "id = " + String.join(" or id = ", creativeIdsArray);
    System.out.printf("sub filter: %s\n", creativeIdSubFilter);


    // Create a statement to get all creatives.
    StatementBuilder statementBuilder = new StatementBuilder()
    .orderBy("id ASC")
    .where(creativeTypeSubFilter)
    .where(advertiserIdSubFilter)
    .where(creativeIdSubFilter) // without this filter, we get all creatives under this advertiser
    .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

    System.out.printf("Query statement: %s", statementBuilder.toStatement());

    // Default for total result set size.
    int totalResultSetSize = 0;

    List<Creative> results = new ArrayList<Creative>();

    do {
      // Get creatives by statement.
      CreativePage page = creativeService.getCreativesByStatement(statementBuilder.toStatement());

      if (page.getResults() != null) {
        totalResultSetSize = page.getTotalResultSetSize();
        int i = page.getStartIndex();
        for (Creative creative : page.getResults()) {
          System.out.printf(
          "%d) Creative with ID %d and name '%s' and url '%s' was found.%n", i++,
          creative.getId(), creative.getName(), creative.getPreviewUrl());
          results.add(creative);
        }
      }

      statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
    } while (statementBuilder.getOffset() < totalResultSetSize);

    System.out.printf("Number of results found: %d%n", totalResultSetSize);
    return results;
  }


  public interface MyOptions extends PipelineOptions {
    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    @Required
    String getInputFile();
    void setInputFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  // The DoFn to perform on each element in the input PCollection.
  static class DoAuthAndExtract extends DoFn<String, String>
  {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // Get the input element from ProcessContext.
      String word = c.element();
      String[] parts = word.split(",");
      String clientId = parts[0];
      String clientSecret = parts[1];
      String refreshToken = parts[2];
      String applicationName = parts[3];
      String networkCode = parts[4];

      System.out.printf("clientId: %s", clientId);
      System.out.printf("clientSecret: %s", clientSecret);
      System.out.printf("refreshToken: %s", refreshToken);
      System.out.printf("applicationName: %s", applicationName);
      System.out.printf("networkCode: %s", networkCode);

      // Instantiate DfpSession, DfpServices. Then make the query
      DfpSession session = null;
      try {
        System.out.printf("Loading credentials...\n");
        // Generate a refreshable OAuth2 credential.
        Credential oAuth2Credential =
        new OfflineCredentials.Builder()
        .forApi(Api.DFP)
        .withClientSecrets(clientId, clientSecret)
        .withRefreshToken(refreshToken)
        .build()
        .generateCredential();
        System.out.printf("Creating session...\n");
        // Construct a DfpSession.
        session = new DfpSession.Builder()
        .withApplicationName(applicationName)
        .withNetworkCode(networkCode)
        .withOAuth2Credential(oAuth2Credential).build();
      } catch (ValidationException ve) {
        System.err.printf(
        "Invalid configuration in the %s file. Exception: %s%n",
        DEFAULT_CONFIGURATION_FILENAME, ve);
      } catch (OAuthException oe) {
        System.err.printf(
        "Failed to create OAuth credentials. Check OAuth settings in the %s file. "
        + "Exception: %s%n",
        DEFAULT_CONFIGURATION_FILENAME, oe);
      }

      DfpServices service = new DfpServices();

      try {
        List<Creative> creatives = GetCreatives(service, session);
        c.output(String.valueOf(creatives.size()));
      } catch (ApiException apiException) {
        // ApiException is the base class for most exceptions thrown by an API request. Instances
        // of this exception have a message and a collection of ApiErrors that indicate the
        // type and underlying cause of the exception. Every exception object in the dfp.axis
        // packages will return a meaningful value from toString
        //
        // ApiException extends RemoteException, so this catch block must appear before the
        // catch block for RemoteException.
        System.err.println("Request failed due to ApiException. Underlying ApiErrors:");
        if (apiException.getErrors() != null) {
          int i = 0;
          for (ApiError apiError : apiException.getErrors()) {
            System.err.printf("  Error %d: %s%n", i++, apiError);
          }
        }
      } catch (RemoteException re) {
        System.err.printf("Request failed unexpectedly due to RemoteException: %s%n", re);
      }
    }
  }

  public static class DFP_Query extends PTransform<PCollection<String>, PCollection<String>>
  {
    @Override
    public PCollection<String> expand(PCollection<String> credentials)
    {
      System.out.printf("--- credentials: %s\n", credentials);
      PCollection<String> raw_data = credentials.apply(ParDo.of(new DoAuthAndExtract()));
      return raw_data;
    }
  }

  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    System.out.printf("input file: %s\n", options.getInputFile());
    System.out.printf("output file: %s\n", options.getOutput());

    Pipeline p = Pipeline.create(options);

    // assert that input is not empty
    PCollection<Long> count = p.apply("GetInput", TextIO.read().from(options.getInputFile()))
                                 .apply(Count.<String>globally());
    Long expectedNumEntities = 1L;
    PAssert.thatSingleton(count).isEqualTo(expectedNumEntities);

    p.apply("GetInput", TextIO.read().from(options.getInputFile()))
     .apply(new DFP_Query())
     // normalize data depending on our agreed convention
     .apply("WriteOutput", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();

  }
}
