Use DB-scoped version counter to isolate transaction reads properly
(i.e. writes from a committed transaction will not show up in other transactions created before commit, if they read after the commit)

Provide convenience functions for "dirty" writing single values
(wrap them in a transaction anyway, so that the cache logic is still right)