| W | R | R+W>N | combinations | fresh reads | stale reads | stale exactly when write set and read set are disjoint |
|---|---|---|---|---|---|---|
| 2 | 2 | yes | 9 | 9 | 0 | yes |
| 3 | 1 | yes | 3 | 3 | 0 | yes |
| 1 | 3 | yes | 3 | 3 | 0 | yes |
| 1 | 1 | no | 9 | 3 | 6 | yes |
| 1 | 2 | no | 9 | 6 | 3 | yes |
| 2 | 1 | no | 9 | 6 | 3 | yes |
