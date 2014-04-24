## TextQL

This is my fork on [dinedal/textql](https://github.com/dinedal/textql). The major enhancements include the following features:

### Enhancements ###
  - Add two new options on command line:
  	* pk		- Enable end user can provide primary key information during table creation;
  	* ori_op	- How to deal with the duplicated records. This option will work only with valid pk . Must be one of (replace/rollback/abord/fail/ignore/)

### TODO ###
  - There is a bug in current SQLite driver that the result of `pragma table_info('table_name')` can not fetch the pk information correctly;

