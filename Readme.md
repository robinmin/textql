## TextQL

This is my fork on [dinedal/textql](https://github.com/dinedal/textql). The major enhancements include the following features:

### Enhancements ###
  - Add two new options on command line:
  	* pk		- Enable end user can provide primary key information during table creation;
  	* ori_op	- How to deal with the duplicated records. This option will work only with valid pk . Must be one of (replace/rollback/abord/fail/ignore/)
  - Add more information output in verbose mode, to enable user understand the full process result more clearly;

### TODO ###
  - There is a bug in current SQLite driver that the result of `pragma table_info('table_name')` can not fetch the pk information correctly;

### Sample ###
under my windows, I use the following command to import all 600000 records into table AirlineDemoSmall2
```dos
textql.exe -dlm "," -header -save-to test002.db -source C:\SampleData\AirlineDemoSmall.csv -table-name AirlineDemoSmall2 -verbose
```

Meanwhile, I use the following command to import 7 records into table AirlineDemoSmall( replaced duplicate records by DayOfWeek)
```dos
textql.exe -dlm "," -pk "DayOfWeek" -header -dup "replace" -save-to test002.db -source C:\SampleData\AirlineDemoSmall.csv -table-name AirlineDemoSmall -verbose
```
