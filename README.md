# refdb

A Clojure library designed to utilise a clojure Ref, and the filesytem
as a simple "database".

**NOTE: This project is really just an experiment. You probably
  shouldn't run it in production.**

## Artifacts

`refdb` artifacts are [released to Clojars](https://clojars.org/com.andrewmcveigh/refdb).

If you are using Maven, add the following repository definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### The Most Recent Release

With Leiningen:

``` clj
[com.andrewmcveigh/refdb "0.1.3"]
```

The most recent release [can be found on Clojars](https://clojars.org/com.andrewmcveigh/refdb).

## Documentation

The complete [API documentation](http://andrewmcveigh.github.io/refdb/uberdoc.html)
is also available (marginalia generated).

## Usage

Refdb needs to know where it can find it's files. It gets this from
`#'refdb.core/*path*`.

Helpers `#'refdb.core/with-refdb-path` and `#'refdb.core/wrap-refdb` can be
used to set the path in a flexible way.

```clojure
> (with-refdb-path "/path/to/files"

>   (def collection (ref nil))

>   (init! collection) ; call in initialisation

>   (destroy! collection)

>   (save! collection {:key val ...})

>   (save! collection assoc-in [0 :key1] {:key val ...})

>   (save! collection update-in [0 :key2] inc)

> )
```

## License

Copyright © 2013 Andrew Mcveigh

Distributed under the Eclipse Public License, the same as Clojure.
