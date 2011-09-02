s3\_fdw
=======

s3\_fdw provides a foreign-data wrapper (FDW) for Amazon S3 files,
using the builtin COPY format.

To build it, just do this:

    make
    make install

If you encounter an error such as:

    make: pg_config: Command not found

Be sure that you have `pg_config` installed and in your path. If you used a
package management system such as RPM to install PostgreSQL, be sure that the
`-devel` package is also installed. If necessary tell the build process where
to find it:

    env PG_CONFIG=/path/to/pg_config make && make installcheck && make install

Once `make install` is done, connect to your database with psql or other client
and type

    CREATE EXTENSION s3_fdw;

then you'll see the FDW is installed. With the FDW, create server, user mapping,
foreign table. You'll need Amazon S3 access key ID and secret access key to
authenticate private access to your data. Consult AWS documentation for those keys.
The access information is stored in user mapping. Foreign tables stores options
for COPY as well as hostname, bucketname and filename.

Dependencies
------------
The `s3_fdw` data type depends on libcurl and openssl. You need those developer
packages installed in the system path.
