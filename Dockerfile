FROM ubuntu:18.04
#ENV APP_ROOT /home/taichi/www/myproject

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get -y install binutils libproj-dev gdal-bin vim git postgresql libpq-dev python3-pip wget unzip
RUN ln -s /usr/bin/python3 /usr/bin/python && ln -s /usr/bin/pip3 /usr/bin/pip
RUN pip install -U pip && hash -r pip

RUN echo 'postgres:postgres' | chpasswd

RUN mkdir /code
WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
RUN pip install -r requirements.txt

USER postgres
RUN /etc/init.d/postgresql start && \
    psql --command "ALTER ROLE postgres WITH PASSWORD 'postgres';"

USER root

# bash から日本語入力できない症状への対処
RUN apt-get install -y language-pack-ja-base language-pack-ja
ENV LANG=ja_JP.UTF-8


RUN pip install django-leaflet
RUN pip install django-extensions
RUN pip install jupyter -U && pip install jupyterlab
RUN pip install djangorestframework-gis
RUN pip install django-filter
RUN pip install markdown
RUN pip install freeze

RUN pip install requests
RUN pip install pandas
RUN pip install geopandas

#RUN pip install django-raster

# 要確認（Dockerfieleでの動きをチェックしていない）
RUN pip install numpy
RUN pip install rasterio

RUN pip install matplotlib
RUN pip install seaborn
RUN pip install tqdm
RUN pip install django-pandas

# for GDAL
RUN apt-get update -y && apt-get upgrade -y
RUN apt install -y gdal-bin libgdal-dev
RUN pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`

RUN pip install RichDEM

RUN mkdir /src
WORKDIR /src