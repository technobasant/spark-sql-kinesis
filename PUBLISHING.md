# Publishing to Maven Central

This connector is published to Maven Central under the namespace `io.github.technobasant`,
via the [Sonatype Central Portal](https://central.sonatype.com). The original maintainer
(`com.roncemer.spark`) stopped maintaining the project and declared it "strongly
discouraged" upstream, so releases now come from here.

Consumers need no custom resolver — Central is the default repository for Maven, sbt,
Gradle and Coursier:

```scala
// sbt
libraryDependencies += "io.github.technobasant" %% "spark-sql-kinesis" % "1.3.0_spark-4.2.0"
```

```xml
<!-- Maven -->
<dependency>
  <groupId>io.github.technobasant</groupId>
  <artifactId>spark-sql-kinesis_2.13</artifactId>
  <version>1.3.0_spark-4.2.0</version>
</dependency>
```

## One-time setup

These four steps are done once per machine/account. Steps 1–2 need a browser.

### 1. Central Portal account

Register at <https://central.sonatype.com> (free). Sign in with GitHub so the account is
already linked to the `technobasant` identity the namespace derives from.

### 2. Verify the `io.github.technobasant` namespace

In the Portal: **Namespaces → Add Namespace → `io.github.technobasant`**. The Portal issues
a **verification key**.

Verification is by *repository name*, not by a website — a live `technobasant.github.io`
does **not** satisfy it. Create a public GitHub repository whose **name is exactly the
verification key**, then click Verify. Once the namespace shows `VERIFIED`, the temporary
repository can be deleted; it is only needed during validation.

### 3. GPG signing key

Central rejects unsigned artifacts. Generate an **RSA 4096** key — one command, and it
prompts only for a passphrase:

```bash
gpg --quick-generate-key "Basant Bhattarai <technobasant9@gmail.com>" rsa4096 sign 2y
gpg --list-secret-keys --keyid-format LONG      # note the LONG key id
```

The key's identity is `technobasant9@gmail.com` — the account the Central Portal namespace
is registered under — not the work address used for git authorship. The namespace is
`io.github.technobasant`, so the signing identity belongs to that account.

RSA rather than Ed25519: EdDSA signatures have historically tripped Central's validator,
and a rejected deployment is more annoying here than elsewhere because published versions
are immutable — a bad release can only be superseded, never replaced.

The pom deliberately gives `maven-gpg-plugin` no passphrase configuration, so signing goes
through `gpg-agent` and its pinentry prompt. That keeps the passphrase out of the command
line and out of `settings.xml`, which is what the plugin's own best-practices mode wants.
Run the release from an interactive terminal so the prompt can appear. If the agent cannot
reach a pinentry (a CI runner, say), pass the passphrase from an environment variable
instead of a literal:

```bash
mvn -Prelease clean deploy -Dgpg.passphrase="$GPG_PASSPHRASE" -Dgpg.pinentry-mode=loopback
```

Publish the *public* half so Central can verify the signatures — it checks public
keyservers, and an unpublished key fails validation:

```bash
gpg --keyserver keys.openpgp.org      --send-keys <LONG_KEY_ID>
gpg --keyserver keyserver.ubuntu.com  --send-keys <LONG_KEY_ID>
```

Keep the private key and passphrase in a password manager. Losing it does not invalidate
released artifacts, but a *new* key must be published before the next release.

### 4. Portal token in `~/.m2/settings.xml`

In the Portal: **Account → Generate User Token**. It returns a username/password pair.
Add it as a **server** whose id matches the pom's `<publishingServerId>`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>${env.CENTRAL_TOKEN_USER}</username>
      <password>${env.CENTRAL_TOKEN_PASS}</password>
    </server>
  </servers>
</settings>
```

Using `${env.*}` keeps the token out of the file on disk; export the two variables in the
release shell instead. A literal token also works if the file is `chmod 600`.

> This is a `<server>`, not a `<mirror>`. An existing `<mirror>` with `id=central` and
> `mirrorOf=*` is unrelated and does not supply publishing credentials — the two coexist.

## Releasing

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)     # Java 17 is required

# 1. Dry run: everything the release does except signing and upload.
mvn -Prelease clean verify -Dgpg.skip=true

# 2. Real release.
mvn -Prelease clean deploy
```

`-Prelease` adds the four things Central requires beyond a normal build: a sources jar, a
javadoc jar, GPG signatures for every artifact, and the
`central-publishing-maven-plugin` upload.

The pom sets `autoPublish=false` and `waitUntil=validated`, so `deploy` uploads a
**staged** deployment and blocks until the Portal has validated it. Nothing is public yet.

### 3. Promote it

Open <https://central.sonatype.com/publishing/deployments>, review the staged deployment,
and click **Publish**.

**Central releases are immutable.** A published version can never be replaced or deleted —
only superseded by a new version number. That is why promotion is manual rather than
automatic; check the coordinates, the shaded contents and the pom before clicking.

Artifacts appear under
<https://repo1.maven.org/maven2/io/github/technobasant/spark-sql-kinesis_2.13/> within
minutes, and become searchable on search.maven.org within a few hours.

### 4. Tag the release

```bash
git tag -a spark-sql-kinesis_2.13-1.3.0_spark-4.2.0 -m "Release 1.3.0_spark-4.2.0"
git push uxcam spark-sql-kinesis_2.13-1.3.0_spark-4.2.0
```

The pom's `<scm><tag>` already names this tag, so keep the two in step.

## Versioning

The `<version>` carries the target Spark version as a suffix —
`1.3.0_spark-4.2.0` — continuing the convention the project has used since
`1.2.3_spark-3.2`. The connector links against relocated Spark streaming internals, so a
given jar is only usable on the Spark minor it was built for; the suffix makes that
visible in the coordinate rather than buried in release notes.

Snapshots are not published: the Central Portal does not accept them through this path.
Use `mvn install` for local iteration, which writes to `~/.m2/repository`.

## Notes

- The published jar is a shaded uber-jar. AWS SDK v2, Netty, protobuf and Apache
  HttpClient are bundled and relocated so the connector cannot collide with whatever
  versions Spark or hadoop-aws put on the classpath. `maven-shade-plugin` publishes a
  dependency-reduced pom, so consumers do not inherit the bundled dependencies; Spark
  itself stays `provided`.
- Original authorship is preserved in `<developers>`: Qubole wrote the connector and Ron
  Cemer ported it to Spark 3.2. `<organization>` records the current maintainer.
