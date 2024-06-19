FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

MAINTAINER kaiyi
USER root
RUN apt-get update -y
RUN apt-get install -y vim python-is-python3 zip gettext

ENV WORKDIR=/opt/spark/work-dir
WORKDIR ${WORKDIR}
COPY app_extension ${WORKDIR}/
RUN ${WORKDIR}/scripts/init_pypkgs.sh

# https://www.scala-sbt.org/1.x/docs/offline/Installing-sbt-on-Linux.html
RUN apt-get update -y
RUN apt-get install apt-transport-https curl gnupg -yqq
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
RUN apt-get update -y
RUN apt-get install sbt -y

RUN pip install ipython

ENV PATH=${PATH}:${SPARK_HOME}/bin

RUN mkdir /home/spark
RUN cp /root/.bashrc /home/spark/
RUN chown spark:spark /home/spark

USER spark
ENTRYPOINT ${WORKDIR}/main.sh
