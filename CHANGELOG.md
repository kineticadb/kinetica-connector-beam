# Kinetica Beam Connector Changelog

## Version 7.2.0.0 - 2026-07-31

-   Added support for Kinetica 7.2; the connector now requires Kinetica Java
    API version 7.2.3.22 or later.
-   **Java 11 is now the minimum supported version** (previously Java 8). The
    Kinetica Java API is compiled for Java 11 as of 7.2.3.7, and the declared
    version range always resolves to the newest release.
-   Pinned Jackson to 2.22.1 via `jackson-bom` in both POMs, matching the pin
    Kinetica Java API 7.2.3.22 applies internally, in order to ensure security
    issues up to that release are handled.
-   Added a `maven-enforcer-plugin` `requireUpperBoundDeps` rule to both POMs so
    the build fails if a managed dependency is resolved *below* a version another
    dependency asks for. It is scoped to the artifacts these POMs manage.
-   Raised `slf4j-api` to 2.0.17 and `logback-classic` to 1.3.16, and added
    management for `commons-compress` 1.28.0, `commons-codec` 1.19.0 and
    `error_prone_annotations` 2.11.0.
-   Pinned the `maven-shade-plugin` (3.6.0) and `maven-jar-plugin` (3.4.2)
    versions in the example. Previously unversioned, they came from Maven's
    default lifecycle bindings, so the build behaved differently across Maven
    versions; with Jackson 2.22.x, Maven 3.8.x picked a shade plugin that could
    not read the jar's Java 21 multi-release entries.
-   Removed the Kinetica Nexus repository from the build; the Kinetica Java API
    is now resolved from Maven Central.
-   Replaced the Docker reference environment with a single
    `docker/docker-compose.yml` covering the database, a build container and a
    Spark cluster.
-   Switched the example's Spark profile from `beam-runners-spark` (Spark 2.3)
    to `beam-runners-spark-3` (Spark 3.1).
-   Rewrote the README build and test instructions; corrected the example class
    name in the Spark and Direct Runner commands.
-   Fixed broken Kinetica logo in the README.
-   Cleaned up source formatting.


## Version 7.1.8.0 - 2022-09-02

-   Updated example.


## Version 7.1.0.0 - 2020-07-27

-   Releasing version.


## Version 7.0.0.0 - 2019-01-31

-   Releasing version.


## Version 6.2.0.0 - 2018-08-20


-   Initial version.
