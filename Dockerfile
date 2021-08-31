FROM ubuntu:latest
RUN apt update -y
RUN apt -y upgrade
RUN DEBIAN_FRONTEND=noninteractive apt -y install nodejs npm curl gnupg debian-keyring debian-archive-keyring apt-transport-https
RUN apt-key adv --keyserver "hkps://keys.openpgp.org" --recv-keys "0x0A9AF2115F4687BD29803A206B73A36E6026DFCA"
RUN apt-key adv --keyserver "keyserver.ubuntu.com" --recv-keys "F77F1EDA57EBB1CC"
RUN apt-key adv --keyserver "keyserver.ubuntu.com" --recv-keys "F6609E60DC62814E"
RUN tee /etc/apt/sources.list.d/rabbitmq.list <<EOF
RUN echo "deb http://ppa.launchpad.net/rabbitmq/rabbitmq-erlang/ubuntu bionic main"
RUN echo "deb-src http://ppa.launchpad.net/rabbitmq/rabbitmq-erlang/ubuntu bionic main"
RUN echo "deb https://packagecloud.io/rabbitmq/rabbitmq-server/ubuntu/ bionic main"
RUN echo "deb-src https://packagecloud.io/rabbitmq/rabbitmq-server/ubuntu/ bionic main"
RUN echo "EOF"
RUN apt update -y
RUN DEBIAN_FRONTEND=noninteractive apt -y install erlang-base erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key erlang-runtime-tools erlang-snmp erlang-ssl erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl
RUN DEBIAN_FRONTEND=noninteractive apt -y install rabbitmq-server --fix-missing postgresql postgresql-contrib
RUN DEBIAN_FRONTEND=noninteractive apt -y install sudo nano
WORKDIR /app
COPY src/ src/
WORKDIR /app/src
RUN npm install
RUN npm install pm2 -g
EXPOSE 3000
EXPOSE 3010
RUN chmod 744 ./services.sh
CMD ./services.sh