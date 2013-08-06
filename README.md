# refdb

A Clojure library designed to utilise a clojure Ref, and the filesytem as a
simple "database".

## Usage

```clojure
> (def collection (ref nil))

> (init! collection) ; call in initialisation

> (destroy! collection)

> (save! collection {:key val ...})
```

## License

Copyright © 2013 Andrew Mcveigh
