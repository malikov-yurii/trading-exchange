FROM eclipse-temurin:11-jre
WORKDIR /aeron

COPY aeron-all-1.42.1.jar aeron-all.jar

ENTRYPOINT java \
  -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:8011 \
  -Daeron.dir=/aeron/vol/driver \
  -Daeron.archive.dir=/aeron/vol/archive \
  -Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:8010 \
  -Daeron.archive.control.streamId=100 \
  -Daeron.archive.local.control.channel=aeron:ipc \
  -cp aeron-all.jar \
  io.aeron.archive.ArchivingMediaDriver

#  -Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:8010\|control=127.0.0.1:8020 \

#ENTRYPOINT [  "java","-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:8011", "-Daeron.dir=/aeron/vol/driver",  "-Daeron.archive.dir=/aeron/archive", "-Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:8010|control=127.0.0.1:8012",  "-Daeron.archive.control.streamId=100",  "-Daeron.archive.local.control.channel=aeron:ipc",  "-cp", "aeron-all.jar",  "io.aeron.archive.ArchivingMediaDriver"]