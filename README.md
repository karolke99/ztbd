# ztbd

Run env
```commandline
$ docker-compose up
```


## Postgresql

Connect to database
```commandline
$ psql -h localhost -p 5432 -U postgres
Password for user postgres: postgres
```


## Mongodb

Connect using mongosh in the container
```commandline
docker exec -it ztbd_mongodb_1 mongosh -u user -p pass --authenticationDatabase admin
```commandline

