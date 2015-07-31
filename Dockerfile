FROM ubuntu:14.04.2
MAINTAINER Michael Drogalis <mjd3089@rit.edu>

# Add a repo where OpenJDK can be found.
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update

# Auto-accept the Oracle JDK license
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections

RUN apt-get install -y oracle-java8-installer

ADD target/onyx-batch-fix-0.1.0-SNAPSHOT-standalone.jar /srv/onyx-batch-fix.jar

ADD script/run-onyx-batch-fix.sh /srv/run-onyx-batch-fix.sh

ENTRYPOINT ["/bin/sh", "/srv/run-onyx-batch-fix.sh"]
