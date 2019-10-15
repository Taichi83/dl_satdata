## Set up docker environment
### Install docker and docker-compose to your PC
Install Docker  
https://docs.docker.com/install/  

Install Docker-compose  
https://docs.docker.com/compose/install/

### what is docker image and container
Japanese https://qiita.com/gold-kou/items/44860fbda1a34a001fc1  
English https://medium.com/codingthesmartway-com-blog/docker-beginners-guide-part-1-images-containers-6f3507fffc98

etc..

### Build docker image
terminal on your PC  
```
$ cd (directory which has Dockerfile)
$ ls
Dockerfile
$ mkdir src
$ mkdir data
$ docker build -t tcjn_analysis:latest .
$ docker run -it -p 8080:8080 -v /~path-to-your-current-dir~/src:/src --name dlsat_cont tcjn_analysis
```
then you can build and enter docker container now.

### Set up Jupyter
on docker container
```
# jupyter notebook --generate-config
# jupyter notebook password
# jupyter lab --ip=0.0.0.0 --port=8083 --allow-root

```

## setting of interpreter in Pycharm

##### Interpreter:
docker image of dl_satdata (not docker-compose image)  

##### Setting of Debug configuration  
Working directory: /home/taichi/src/Tenchijin_Geodjango/dl_satdata/
Docker container settings:
+ volume bindings  
    + container path/hostpath:
        + /src : /home/taichi/src/Tenchijin_Geodjango/dl_satdata
        + /data : /home/taichi/src/Tenchijin_Geodjango/data  
 
 