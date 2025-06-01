# TODOs

## Implement First (Basic/Foundational)

1. need to intelligently add new cols if reasonable
2. need to kinda analyze the pipeline to find improvements in prompt etc
3. write at least small-ish test suite for it where I possibly also evaluate its robustness for various data inputs etc.
4. generalize it to different LLM providers; possible open source ones
5. maybe generalize it to different data formats (csv, mysql, whatever)
6. implement "hooks" for user feedback on assumption error (that implies dry run mode)
7. allow for smart retrieval by query using in prosa (maybe use some other existing mcp stuffs for that as I think that exists)
8. allow for self-ingest or at least analyze it as it could have a lot of potential.
9. add (and design) the "data-trace" architecture. That means it must be kind of traceable where the data for each row came from (that implies rollbacks)
10. write stuff to create the entire intention schema from prompting
11. need to publish to to pypi
12. write some social media post about it
13. write paper or so about it (theory + impact + use cases)

## Advanced

14. allow for large files (memory handling etc)
15. allow for schema modification using prompting (could work using simply an ingestion of the existing data in the new schema)
16. allow for nested structures.
17. generalize gatekeeper architecture

## Advanced Very

18. implement semantic retrieval models
19. implement trust and relevance storing

## Advanced Very Very

20. implement generalized information storage from this architecture

## Very Very Advanced

21. including decay mechanisms for data

## Very Very Very Very Advanced

22. implement self refinement and incremental thinking

## Implement Last (AGI Level Advanced)

23. implement robust generalized learning
