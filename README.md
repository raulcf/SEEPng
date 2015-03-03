# SEEPng

## Quick Start
This is a quick guide to run the system, better and more detailed information is
coming soon.

### Get SEEPng:
git clone ...

cd SEEPng

### Build and create distro
./gradlew installApp

### Run master and worker
Master

./install/seep-master/bin/seep-master <master params>

Worker

./install/seep-worker/bin/seep-worker <worker params>

### Create distro for distribution
If you want to package the system, for example, for distribution, just:

./gradlew distZip

Then find the distribution in zip files in dist/

### Create fat, standalone Jars with all dependencies
If you want to package all dependencies within jar, for standalone deployment
and execution do:

./gradlew distStandaloneJar

If you want later to copy those standalone jars to a standaloneDist directory in
the root do:

./gradlew standaloneDist

### Clean repo
./gradlew clean

