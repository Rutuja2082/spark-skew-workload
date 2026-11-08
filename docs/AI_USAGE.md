# AI Usage and Contribution Disclosure

## 1. Summary

I used two AI assistants on this project: **Claude** (Anthropic) and **ChatGPT** (OpenAI). This page says plainly who did what.

- **Claude wrote the project.** The source code, tests, data generator, benchmark, reconciliation logic, README, design note and project report were written by Claude in response to my prompts and the assignment text. Claude also ran and debugged them in its own sandbox.
- **ChatGPT was my mentor and reviewer.** I used it to break the assignment into stages, explain the code and Spark concepts, troubleshoot my Windows environment, interpret the results and review my documentation and Git workflow.
- **I ran, verified and submitted it.** I set up the environment on my own Windows computer, ran every command and the full test suite there, hit and fixed real setup problems, recorded my own benchmark results, created the GitHub repository and prepared this disclosure. I did **not** write the pipeline code myself.

## 2. Contribution at a glance

| Area | Claude | ChatGPT | Me |
|---|---|---|---|
| Understanding the assignment | Turned it into a design and a 3-day plan | Broke it into 12 practical stages | Read the assignment and guidelines, chose the approach |
| Source code (generator, validation, pipeline, join modes, benchmark, reconciliation) | **Wrote it** | Explained it to me | Ran it, studied it |
| Automated tests (55) | **Wrote them** | Helped me understand them | Ran them on Windows, reported failures |
| Debugging | Found and fixed code issues in its sandbox | Helped interpret Windows/Hadoop errors | Applied fixes and re-ran |
| Windows environment (Python, Java 17, Spark, Hadoop native files) | Gave first setup steps | Helped identify the Hadoop native-library issue | **Installed, configured and verified it** |
| Performance results | Produced the benchmark tool and a first reading | Helped me interpret my results | **Ran the benchmark on my machine** |
| Documentation | **Wrote** README, data dictionary, design note, report | Reviewed and helped structure | Reviewed, kept it in the repo |
| Git and GitHub | Suggested a staged commit plan | Helped with the workflow | **Created the repository, committed and pushed** |
| Final responsibility | n/a | n/a | **Me** |

## 3. Claude's work

Claude (Anthropic) worked in one long conversation. It did the following:

- **Design and planning:** the 3-day plan (schemas and baseline, then skew diagnosis and mitigation, then reconciliation and polish).
- **All implementation:** synthetic data generator with a fixed seed, injected defects and separate expected labels; file ingestion with schema-change quarantine; row validation, deduplication, referential check and reject table; staging and published layers; run logs; skew diagnosis; four execution modes (baseline, broadcast, salted, AQE); benchmark; idempotent reruns and failure simulation; independent reconciliation with 26 checks; sample queries.
- **All tests:** validation, diagnosis, equivalence of all modes, AQE regression, idempotency, reconciliation (including tests that deliberately damage a good load).
- **Documentation:** README, data dictionary, design and trade-off note, benchmark report, and the full project report.
- **Debugging:** it ran the code in its sandbox, found failures and fixed them (see section 6).
- **Setup help:** early guidance for VS Code, Java, the virtual environment and Windows.

## 4. ChatGPT's work

ChatGPT acted as a technical mentor, reviewer, debugger and planning assistant. According to my own use of it:

- **Planning:** helped me split the assignment into stages: scope and requirements, environment, synthetic workload, validation and transformation, skewed workload, diagnosis, mitigation, benchmarking, correctness checks, reconciliation, documentation and GitHub preparation.
- **Environment troubleshooting:** helped me understand the Windows Hadoop `NativeIO$Windows` error and that the native binaries must be reachable through `HADOOP_HOME` and the Windows PATH.
- **Understanding:** explained the code, skew, joins, broadcast, salting and AQE in simpler terms so I could follow and explain them.
- **Interpreting results:** helped me read the skew diagnosis and benchmark results (see section 7).
- **Review:** helped review and structure documentation, reproducibility steps and the Git/GitHub workflow.

I applied ChatGPT's suggestions myself and checked them by running the project.

## 5. My work

- Read the assignment and the submission guidelines and decided to deliver a complete, tested slice with honest documentation.
- Installed and configured the environment on native Windows: Python, a virtual environment, PySpark 3.5.9, Java 17 and Hadoop 3.3.4 native Windows support (`HADOOP_HOME`, PATH).
- Fixed real setup problems: Java 25 is not supported by this Spark version (so I used Java 17), PowerShell blocked the virtual-environment activation script, and Spark needed the Hadoop native files to write files on Windows.
- Ran everything locally: data generation, the pipeline, diagnosis, benchmark, reconciliation and the test suite.
- Found two Windows-only test failures in my first full run and re-ran after the fix (section 6).
- Recorded my own benchmark and reconciliation results (section 7).
- Studied the code with ChatGPT's help so I can walk through the solution.
- Created the GitHub repository, committed and pushed the project, and reviewed it before submission.
- Saved the prompt histories and wrote this disclosure.

What I did **not** do: I did not write the pipeline code, tests or most of the documentation myself, and the main technical design choices (for example making the broadcast join the default) came from Claude. I accepted them after running the results and understanding the reasons.

## 6. Where the AI was wrong, and how it was caught

None of this was accepted on trust. Problems were found by running and measuring:

- **AQE did nothing at first.** It was configured but never split the hot partition. It needs several input splits, and Spark cancels the split unless a force setting is on. The benchmark now checks the final plan and a regression test covers it.
- **Salted output looked different from the baseline.** The values were identical but the schema differed in nullability. The transform was fixed instead of weakening the comparison.
- **Some tests were buggy.** They held references to files that a forced reload had correctly replaced. They were fixed, which also revealed a real limitation (readers have no protection during a reload).
- **The skip logic trusted the run log.** A clean-environment check showed a run skipped as "already loaded" although the data was gone. It now also checks that the data exists.
- **Two Windows-only test failures.** In my first full run on Windows, 53 of 55 tests passed. Two reconciliation tests failed because a test helper rewrote paths in a JSON log with a plain text replace, which does not match Windows paths (stored with doubled backslashes). The code under test was fine. The helper was fixed to parse the JSON, and I re-ran the suite.
- **Early setup advice needed correcting**, for example the Java version (17, not the newest release) and the Windows Hadoop files.

## 7. How the result was verified (on my machine)

**Environment:** Windows, Python, PySpark / Spark 3.5.9, Java 17, Hadoop 3.3.4 native Windows support, `local[*]`, 8 CPU cores, 8 shuffle partitions.

**Tests:** the full suite gives **55 passed** (after the fixed test file replaced the original).

**Reconciliation:** 26 passed, 0 failed, 0 skipped. It checks the input-to-valid/rejected row flow, unique keys, nulls, foreign keys, the published summary against an independent recomputation, order counts and exact decimal amounts, reject traceability, duplicate handling, run-log consistency and the expected labels.

**Skew diagnosis:** 576,600 valid orders and 2,000 customers. One hot customer holds 345,455 orders (about 59.91%). One shuffle partition receives about 65% of the rows. The max/mean partition ratio is 5.19x before salting and 1.60x after.

**Benchmark (my machine):**

| Mode | Median wall time | Speedup | Result vs baseline |
|---|---:|---:|---|
| Baseline | 7.937 s | 1.00x | Reference |
| Broadcast | 3.314 s | 2.39x | Identical |
| Salted | 8.824 s | 0.90x | Identical |
| AQE | 6.443 s | 1.23x | Identical |

All three alternatives published 2,000 customer rows, with no rows only in one result, identical schemas and equal order counts and total amounts.

**Conclusion:** broadcast was the best fix for this workload because the customers table is small. Salting balanced the partitions much better but did not reduce wall time here. AQE gave a moderate gain.

## 8. Honest limits

- Most of the code was AI-generated. I understand it through study and ChatGPT's explanations, but I am not presenting it as written by me.
- Benchmark timings depend on my machine, data size and three measured rounds. Other numbers in the README and project report (for example 7.3 s for the baseline) were measured by Claude on a 1-core machine, so they differ from mine. The relative picture is the same: broadcast is clearly fastest.
- Known project limits: publishing is not atomic across tables, there is no locking, readers have no protection during a reload, and Databricks Free Edition was not tested. See `README.md` and `docs/design_tradeoffs.md`.

## 9. Prompt history

- `docs/PROMPT_HISTORY.md`: my conversation with Claude.
- `docs/PROMPT_HISTORY_CHATGPT.md`: my conversation with ChatGPT.

## 10. Provenance note

Some Day 2 modules (`diagnose.py`, `ledger.py`, `metrics.py`, `transform.py` and part of `run_pipeline.py`) were already in the project folder when Claude resumed work late in its conversation. Claude reviewed, kept and extended them. Origin of those files: **[AUTHOR: write where they came from, or delete this line if you cannot say]**.
