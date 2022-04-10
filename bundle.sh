#!/bin/bash
jdk=$(/usr/libexec/java_home)
echo using jdk from $jdk
if [ -f "$jdk/bin/jpackage" ]; then
  $jdk/bin/jpackage --verbose --dest packages/bundles --name Clairvoyance --app-version 1.0 \
    --input target --main-jar Clairvoyance-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    --resource-dir package/macosx --type dmg
  $jdk/bin/jpackage --verbose --dest packages/bundles --name Clairvoyance --app-version 1.0 \
    --input target --main-jar Clairvoyance-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    --resource-dir package/macosx --type pkg
else
  $jdk/bin/javapackager -deploy -native dmg -name Clairvoyance \
    -srcfiles ./target/Clairvoyance-0.0.1-SNAPSHOT-jar-with-dependencies.jar -appclass com.rashidmayes.clairvoyance.App \
    -outdir packages -outfile Clairvoyance -v
fi
