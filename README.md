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

### #'refdb.core/db-spec

You can create a db-spec with the macro `#'refdb.core/db-spec`. `db-spec`
takes a map of `opts`, `& colls`. `opts` must contain either `:path` or
`:no-write` must be truthy. `colls` should be passed as keywords which name
the colls.

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

=> {:id 1000 :name "Cedric" :breed "Tabby"}
```

### #'refdb.core/find

Or you can find items(s) matching a predicate map.

```clojure
(db/find db-spec :cats {:color "orange" :name "Reg"})

=> ({:id 457 :breed "Tabby" :color "orange" :name "Reg"}, ...)
```

Predicates can match by `literal?` values, regexes and functions.

```clojure
(db/find db-spec :cats {:color #"(orange)|(brown)"})
(db/find db-spec :cats {:color #(or (= % "brown") (= % "orange))"})

=> ({:id 457 :breed "Tabby" :color "orange" :name "Reg"}, ...)
```

Predicates can have sub-maps, and sets can be used to partially match
collections.

```clojure
(db/find db-spec :cats {:friends #{"Tom"}})

=> ({:id 457 :breed "Persian" :color "Grey" :name "Bosco" :friends ["Tom", "Dick", "Harry"]}, ...)
```

You can also search deeper into a match using a vector as a key.

```clojure
(db/find db-spec :cats {[:skills :jumping :max-height] 20})
```

#### #'refdb.core/?and and #'refdb.core/?or

By default a predicate's matching behavior for key-vals is AND. E.G.,

```clojure
(db/find db-spec :cats {:color "orange" :name "Reg"})
```

finds cats with `:color` `"orange"` AND `:name` `"Reg"`. It's possible
to specify that the predicate should use OR matching behavior, or a
combination. E.G.,

```clojure
(require '[refdb.core :as db :refer [?and ?or]])

(db/find db-spec :cats (?or (?and {:name "Timmy"
                                   :color "Orange"})
                            (?and {:friends #{"Timmy"}})))
```

### Writing with #'refdb.core/save! and #'refdb.core/update!

`#'refdb.core/save!` writes data to the "database", and `#'refdb.core/update!`
updates the data in the "database" by applying a function and args,
much like `#'clojure.core/swap!` works.

```clojure
(db/save! db-spec :cats {:key val ...})

(db/update! db-spec :cats assoc-in [0 :key1] {:key val ...})

(db/update! db-spec :cats update-in [0 :key2] inc)
```

### #'refdb.core/delete!

Adds a `:refdb.core/deleted true` key-val to the data, and so by default
will not be matched by `find` or `get`. The data is not actually deleted.

## License

Copyright © 2013 Andrew Mcveigh

Distributed under the Eclipse Public License, the same as Clojure.
