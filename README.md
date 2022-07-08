# Clairvoyance

Builds available at https://www.clairvoyance.app

## Build and start
```
mvn clean package
java -jar target/Clairvoyance-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

## Description
Clairvoyance is a simple data browser for the Aerospike database. It uses scans to read all records from Aerospike, caching the keys on disk. This is experimental version and should only be used against development and staging environments until it is stable.

## Mod
modified by trouvaillle
- support aerospike client 5.x
- change the main color to more eye-friendly color
- added insert, edit, delete feature
- added search feature
- TODO: search 오류나면 빨간 글씨
- TODO: set browser 오른쪽 버튼 누르면 세트 지우거나 생성 가능하도록
- TODO: insert 할 때 id 대신 @user_key로
- TODO: syntax highlighting
- TODO: refresh도 페이지 넘어가면 첫 페이지만 나오는 문제

## Screenshots

![](https://github.com/trouvaillle/Clairvoyance/blob/master/screenshots/screenshot3.png)
![](https://github.com/trouvaillle/Clairvoyance/blob/master/screenshots/screenshot2.png)
![](https://github.com/trouvaillle/Clairvoyance/blob/master/screenshots/screenshot4.png)
![](https://github.com/trouvaillle/Clairvoyance/blob/master/screenshots/screenshot1.png)
