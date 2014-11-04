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
[com.andrewmcveigh/refdb "0.5.1"]
```

The most recent release [can be found on Clojars](https://clojars.org/com.andrewmcveigh/refdb).

## Documentation

The complete [API documentation](http://andrewmcveigh.github.io/refdb/uberdoc.html)
is also available (marginalia generated).

## Usage

All RefDB API functions need a db-spec passed to them as the first argument.

You can create a db-spec with the macro `#'refdb.core/db-spec`. `db-spec`
takes a map of `opts`, `& colls`. `opts` must contain either `:path` or
`:no-write` must be truthy. `colls` should be passed as keywords which name
the colls.

### #'refdb.core/db-spec

```clojure
(require '[refdb.core :as db])

(db-spec {:path "data"} :cats :dogs)
```

### #'refdb.core/init!

Once you have a `db-spec`, if you want to load anything into it, you should
`init!` it...

```clojure
(db/init! db-spec)
```
... to initialize all collections in the `db-spec`, or...

```clojure
(db/init! db-spec :only #{:cats})
```
... to only initialize some of them.

### #'refdb.core/destroy!

You can wipe out a collection with `destroy!`. *Be careful*, this operation
will destroy the data, in memory and from durable storage.

```clojure
(db/destroy! db-spec :dogs) ; don't hurt the cats
```

### #'refdb.core/get

You can get an item by its ID. IDs can be anything.

```clojure
(db/get db-spec :cats 1000)
```

```clojure

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
