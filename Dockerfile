FROM ubuntu:24.04

RUN chmod a+rwx /var/lib

RUN apt-get update -y &&\
    apt-get upgrade -y &&\
    apt-get install -y\
        build-essential\
        bison\
        flex\
        libreadline-dev\
        make\
        libc6-dev\
        libc6\
        zlib1g-dev\
        vim\
        libxml2-utils\
        docbook-xsl\
        xsltproc\
        bash\
        git

USER ubuntu

RUN mkdir /var/lib/postgresql &&\
    chmod u=rwx,g=rx,o-rwx /var/lib/postgresql

RUN mkdir /var/lib/postgres-data &&\
    chmod u=rwx,g=rx,o-rwx /var/lib/postgres-data

ENTRYPOINT "/bin/bash"