# refdb [![Build Status](https://travis-ci.org/andrewmcveigh/refdb.png?branch=master)](https://travis-ci.org/andrewmcveigh/refdb)

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
[com.andrewmcveigh/refdb "0.2.3"]
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
> (require '[refdb.core :as db])

> (def collection (ref nil))

> (db/with-refdb-path "/path/to/files"

>   (db/init! collection) ; call in initialisation

>   (db/destroy! collection)

>   (db/save! collection {:key val ...})

>   (db/update! collection assoc-in [0 :key1] {:key val ...})

>   (db/update! collection update-in [0 :key2] inc)

>   (db/get collection 1000) ; get by :id

>   (db/find collection :first-name "Abbie" :last-name "Hoffman")

>   (db/find collection {[:keys :in :nested :maps] #"^Search.by\sregex.*$"})

> )
```

## License

Copyright © 2013 Andrew Mcveigh

Distributed under the Eclipse Public License, the same as Clojure.
