package org.apache.spark.sql.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Retries IMDS credential lookups with exponential backoff.
 *
 * <p>AWS SDK v2's {@link InstanceProfileCredentialsProvider} is final, so this delegates
 * rather than extending it as the SDK v1 version did. The retry loop is kept because the
 * SDK's own IMDS retry budget is smaller than the 10 attempts this connector has always
 * used when an executor starts before the node's IMDS is answering.
 */
public class AWSInstanceProfileCredentialsProviderWithRetries
        implements AwsCredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(AWSInstanceProfileCredentialsProviderWithRetries.class);

    private static final int MAX_RETRIES = 10;
    private static final int INITIAL_SLEEP_MS = 500;
    private static final int MAX_SLEEP_MS = 10000;

    private final InstanceProfileCredentialsProvider delegate =
            InstanceProfileCredentialsProvider.create();

    @Override
    public AwsCredentials resolveCredentials() {
        int retries = MAX_RETRIES;
        int sleep = INITIAL_SLEEP_MS;
        while (retries > 0) {
            try {
                return delegate.resolveCredentials();
            } catch (RuntimeException | Error t) {
                LOG.error("Got an exception while fetching credentials", t);
                --retries;
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw SdkClientException.create(
                            "Interrupted while waiting to retry credential lookup", ie);
                }
                if (sleep < MAX_SLEEP_MS) {
                    sleep *= 2;
                }
            }
        }
        throw SdkClientException.create("Unable to load credentials.");
    }
}
