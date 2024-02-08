# jepsen.ydb

A Clojure library for testing YDB with Jepsen.

## Usage

Create a `~/ydb-nodes.txt` file that lists your YDB cluster nodes.

Example command for running the test:

```
lein run test --nodes-file ~/ydb-nodes.txt --db-name /your/db/name --username $USER --concurrency 10n --key-count 100 --max-writes-per-key 1000 --max-txn-length 8
```

## License

Copyright © 2024 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
