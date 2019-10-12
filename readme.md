### setting of interpreter in Pycharm

##### Interpreter:
docker image of dl_satdata (not docker-compose image)  

##### Setting of Debug configuration  
Working directory: /home/taichi/src/Tenchijin_Geodjango/dl_satdata/
Docker container settings:
+ volume bindings  
    + container path/hostpath:
        + /src : /home/taichi/src/Tenchijin_Geodjango/dl_satdata
        + /data : /home/taichi/src/Tenchijin_Geodjango/data  
 