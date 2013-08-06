# refdb

A Clojure library designed to utilise a clojure Ref, and the filesytem as a
simple "database".

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
