Add `pre-commit` hook to git, this hook cleans all jupyter notebook output cells in the `notebooks/` folder.

```
git config --local core.hooksPath .githooks/
```