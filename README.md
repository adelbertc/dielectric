# Dielectric

Dielectric is a playground for coming up with a more functional way of interacting
with [Spark](http://spark.apache.org/).

## Spark SQL and Frameless
In Spark SQL there is a [`DataFrame`](http://spark.apache.org/docs/1.3.0/sql-programming-guide.html) data type
that allows us to work with distributed data in a nice structured way. At the time of this writing however,
the `DataFrame` API is not as strongly typed as some would like.

In an attempt to rectify that, some preliminary work was done in this
[commit](https://github.com/adelbertc/dielectric/commit/e5c9f24655c335e0f9e63fed5b5e112b385c5f0e).
That work has since developed into a more dedicated effort in the
[Frameless](https://github.com/adelbertc/frameless) project.

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
