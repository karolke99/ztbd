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
```

## Cassandra
Connecting using cqlsh in the container
```commandline
docker exec -it ztbd_cassandra_1 cqlsh
```



- Tworzymy managera baz danych dla każdego systemu. Manager ma zawierać metody CRUD (Create, Read, Update, Delete), AVG, ME, len(słowa w tabeli)
- Każda metoda przyjmuje jako parametr liczbę, która albo generuje wiersze do wstawienia albo do pobrania
- Metody z managera zwracają czas wykonywanych operacji
