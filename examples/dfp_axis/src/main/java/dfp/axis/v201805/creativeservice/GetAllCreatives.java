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
  public static void runExample(DfpServices dfpServices, DfpSession session)
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
        }
      }

      statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
    } while (statementBuilder.getOffset() < totalResultSetSize);

    System.out.printf("Number of results found: %d%n", totalResultSetSize);
  }

  public static void main(String[] args) {
    // ClientID, ClientSecret: from DAWC
    // RefreshToken, Network Code: from auth_token endpoint: https://data.nugdev.co/v3/meta_data_sources/102548119667364157873---751815071/auth_token?secret=7eced64feb15491babc266ce12ea
    // ApplicationName: doesn't really matter
    String clientId = "";
    String clientSecret = "";

    // this needs to be refreshed every few mins
    String refreshToken = "";

    String applicationName = "";
    String networkCode = "";

    DfpSession session;
    try {
      System.out.printf("...Loading credentials...\n");
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
      return;
    } catch (OAuthException oe) {
      System.err.printf(
      "Failed to create OAuth credentials. Check OAuth settings in the %s file. "
      + "Exception: %s%n",
      DEFAULT_CONFIGURATION_FILENAME, oe);
      return;
    }

    DfpServices dfpServices = new DfpServices();

    try {
      runExample(dfpServices, session);
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
