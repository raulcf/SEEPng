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
If you want to distribute master and worker to distributed nodes just

./gradlew distZip

Then find the distribution in zip files in dist/

### Clean repo
./gradlew clean
