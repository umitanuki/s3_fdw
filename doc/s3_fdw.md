s3\_fdw
=======

Synopsis
--------

    db1=# CREATE EXTENSION s3_fdw;
    CREATE EXTENSION

    db1=# CREATE SERVER amazon_s3 FOREIGN DATA WRAPPER s3_fdw;
    CREATE SERVER

    db1=# CREATE USER MAPPING FOR CURRENT_USER SERVER amazon_s3
      OPTIONS (
        accesskey 'your-access-key-id',
        secretkey 'your-secret-access-key'
      );
    CREATE USER MAPPING

    db1=# CREATE FOREIGN TABLE log20110901(
         atime timestamp,
         method text, elapse int,
         session text
       ) SERVER amazon_s3
       OPTIONS (
         hostname 's3-ap-northeast-1.amazonaws.com',
         bucketname 'umitanuki-dbtest',
         filename 'log20110901.txt',
         delimiter E'\t'
       );
     CREATE FOREIGN TABLE

Description
-----------

This module provides foreign-data wrapper for Amazon S3 files.
The procedure to initiate your foreign table is shown above.
For the first process, `create extension` for this module. Then,
`create server` with some name whatever you like without options,
since server option is not supported yet. After that,
`create user mapping` for current user with mandatory options
`accesskey` and `secretkey`. They are provied from Amazon to you.

Last, `create foreign table` for your file. At the moment you
need to define one table for one file, as file\_fdw in contrib.
s3\_fdw does support all the COPY options as file\_fdw does, as
well as these additional mandatory options:

  - hostname
  - bucketname
  - filename

You'll find the access URL to S3 file. Split it into these
tree options and specify separately.

Roadmap
-------

  - gz file support
  - bucket files bulk load
  - normal URL option rather than split path
  - windows support

Caveat
------

This module is still under development. You may encounter
unpredictable situation by using this program.

Especially s3\_fdw forks backend and calls mkfifo to achieve
read and write in parallel. So, it doesn't work on the
platforms in which fork / mkfifo doesn't work.

Support
-------

Goto http://github.com/umitanuki/s3_fdw
Feel free to report any bug/issues if you find.

Author
------

Hitoshi Harada

