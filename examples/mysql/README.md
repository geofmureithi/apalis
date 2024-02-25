# Running Apalis on MySql

## Get a Mysql instance running

```sh
docker run \
   --name apalis-mysql \
   -e MYSQL_DATABASE=apalis-jobs \
   -e MYSQL_USER=apalis \
   -e MYSQL_ROOT_PASSWORD=strong_password \
   -p 3306:3306 \
   -v /etc/docker/apalis-mysql:/etc/mysql/conf.d \
   -v apalis-mysql-data:/var/lib/mysql \
   -d mysql
```