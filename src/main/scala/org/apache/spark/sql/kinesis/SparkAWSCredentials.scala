/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.kinesis

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, AwsSessionCredentials, DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging

/**
 * Serializable interface providing a method executors can call to obtain an
 * AwsCredentialsProvider instance for authenticating to AWS services.
 *
 * `provider` is intentionally a def: AWS SDK v2 providers are not serializable, so each
 * executor constructs its own on first use.
 */
private[kinesis] sealed trait SparkAWSCredentials extends Serializable {
  /**
   * Return an AwsCredentialsProvider instance that can be used to authenticate to AWS
   * services (Kinesis, CloudWatch and DynamoDB).
   */
  def provider: AwsCredentialsProvider
}

/** Returns the SDK v2 default credentials provider chain. */
private[kinesis] final case object DefaultCredentials extends SparkAWSCredentials {

  def provider: AwsCredentialsProvider = DefaultCredentialsProvider.create()
}

/*
 * Returns AWSInstanceProfileCredentialsProviderWithRetries.
 */

private[kinesis] final case object InstanceProfileCredentials
  extends SparkAWSCredentials {
  def provider: AwsCredentialsProvider = new AWSInstanceProfileCredentialsProviderWithRetries
}


/**
 * Returns a StaticCredentialsProvider constructed using a basic AWS keypair. Falls back to
 * the default provider chain if unable to construct one with the provided arguments
 * (e.g. if they are null).
 */
private[kinesis] final case class BasicCredentials(
    awsAccessKeyId: String,
    awsSecretKey: String) extends SparkAWSCredentials with Logging {

  def provider: AwsCredentialsProvider = try {
    StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey))
  } catch {
    case e: IllegalArgumentException =>
      logWarning("Unable to construct StaticCredentialsProvider with provided keypair; " +
        "falling back to the default credentials provider chain.", e)
      DefaultCredentialsProvider.create()
  }
}

private[kinesis] final case class BasicAWSSessionCredentials(
    awsAccessKeyId: String,
    awsSecretKey: String,
    sessionToken: String) extends SparkAWSCredentials with Logging {

  def provider: AwsCredentialsProvider = try {
    StaticCredentialsProvider.create(
      AwsSessionCredentials.create(awsAccessKeyId, awsSecretKey, sessionToken))
  } catch {
    case e: IllegalArgumentException =>
      logWarning("Unable to construct StaticCredentialsProvider with provided keypair; " +
        "falling back to the default credentials provider chain.", e)
      DefaultCredentialsProvider.create()
  }
}

/**
 * Returns a StsAssumeRoleCredentialsProvider which assumes an IAM role in order to
 * authenticate against resources in an external account.
 *
 * The STS client resolves its region from the default region provider chain (AWS_REGION
 * or the active profile), mirroring how the SDK v1 provider used the global endpoint.
 */
private[kinesis] final case class STSCredentials(
    stsRoleArn: String,
    stsSessionName: String,
    stsExternalId: Option[String] = None,
    longLivedCreds: SparkAWSCredentials = DefaultCredentials)
  extends SparkAWSCredentials  {

  def provider: AwsCredentialsProvider = {
    val stsClient = StsClient.builder()
      .credentialsProvider(longLivedCreds.provider)
      // Pinned for the same reason as KinesisReader.getAmazonClient: two sync HTTP
      // implementations are registered, so leave nothing to ServiceLoader ordering.
      .httpClientBuilder(ApacheHttpClient.builder())
      .build()

    val requestBuilder = AssumeRoleRequest.builder()
      .roleArn(stsRoleArn)
      .roleSessionName(stsSessionName)
    stsExternalId.foreach(requestBuilder.externalId)

    StsAssumeRoleCredentialsProvider.builder()
      .stsClient(stsClient)
      .refreshRequest(requestBuilder.build())
      .build()
  }
}

@Evolving
object SparkAWSCredentials {

  @Evolving
  class Builder {
    private var basicCreds: Option[BasicCredentials] = None
    private var stsCreds: Option[STSCredentials] = None
    private var basicSessionCreds: Option[BasicAWSSessionCredentials] = None

    // scalastyle:off
    /**
     * Use a basic AWS keypair for long-lived authorization.
     *
     * @note The given AWS keypair will be saved in DStream checkpoints if checkpointing is
     * enabled. Make sure that your checkpoint directory is secure. Prefer using the
     * [[https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html default provider chain]]
     * instead if possible.
     *
     * @param accessKeyId AWS access key ID
     * @param secretKey AWS secret key
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    // scalastyle:on
    def basicCredentials(accessKeyId: String, secretKey: String): Builder = {
      basicCreds = Option(BasicCredentials(
        awsAccessKeyId = accessKeyId,
        awsSecretKey = secretKey))
      this
    }


    // scalastyle:off
    /**
     * Use a shortlived aws key pair plus security token for short-term authentication
     *
     *
     * @param accessKeyId AWS access key ID
     * @param secretKey AWS secret key
     * @param securityToken AWS Security Token 
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    // scalastyle:on
    def basicSessionCredentials(accessKeyId: String, secretKey: String, securityToken: String): Builder = {
      basicSessionCreds = Option(BasicAWSSessionCredentials(
        accessKeyId,
        secretKey,
        securityToken))
      this
    }

    /**
     * Use STS to assume an IAM role for temporary session-based authentication. Will use configured
     * long-lived credentials for authorizing to STS itself (either the default provider chain
     * or a configured keypair).
     *
     * @param roleArn ARN of IAM role to assume via STS
     * @param sessionName Name to use for the STS session
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    def stsCredentials(roleArn: String, sessionName: String): Builder = {
      stsCreds = Option(STSCredentials(stsRoleArn = roleArn, stsSessionName = sessionName))
      this
    }

    /**
     * Use STS to assume an IAM role for temporary session-based authentication. Will use configured
     * long-lived credentials for authorizing to STS itself (either the default provider chain
     * or a configured keypair). STS will validate the provided external ID with the one defined
     * in the trust policy of the IAM role to be assumed (if one is present).
     *
     * @param roleArn ARN of IAM role to assume via STS
     * @param sessionName Name to use for the STS session
     * @param externalId External ID to validate against assumed IAM role's trust policy
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    def stsCredentials(roleArn: String, sessionName: String, externalId: String): Builder = {
      stsCreds = Option(STSCredentials(
        stsRoleArn = roleArn,
        stsSessionName = sessionName,
        stsExternalId = Option(externalId)))
      this
    }


    def build(): SparkAWSCredentials =
      stsCreds.map(_.copy(longLivedCreds = longLivedCreds)).getOrElse(longLivedCreds)

    /*
     * Session credentials are checked first because they are the more specific of the two
     * static forms. Note that before the SDK v2 migration `basicSessionCreds` was stored
     * but never consulted here, so basicSessionCredentials(...) silently had no effect.
     */
    private def longLivedCreds: SparkAWSCredentials =
      basicSessionCreds.orElse(basicCreds).getOrElse(DefaultCredentials)
  }


  def builder: Builder = new Builder
}
