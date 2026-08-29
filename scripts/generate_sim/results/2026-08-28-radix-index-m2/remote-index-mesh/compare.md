# generate-sim compare — remote-index-mesh

| metric | mesh-tree-sprayed |
|---|---|
| ok | 6.848e+04 ±5.4e+02 |
| err | 0 ±0 |
| achieved_rps | 417 ±2.9 |
| ttft_ms_p50 | n/a |
| ttft_ms_p90 | n/a |
| ttft_ms_p99 | n/a |
| e2e_ms_p50 | 7765 ±95 |
| e2e_ms_p90 | 1.872e+04 ±99 |
| e2e_ms_p99 | 2.133e+04 ±58 |
| AGG cached tokens (sum/sum) | 0.4706 ±0.003 |
| AGG cached (request mean) | 0.5103 ±0.0019 |
| turn1 cached tokens (sum/sum) | 0.2033 ±0.00027 |
| turn1 cached (request mean) | 0.321 ±0.0014 |
| turn1 prompt tokens sum | 4.331e+08 ±4.7e+06 |
| turn1 cached tokens sum | 8.803e+07 ±9.8e+05 |
| followup cached tokens (sum/sum) | 0.9425 ±0.00079 |
| followup cached (request mean) | 0.9265 ±0.00067 |
| followup prompt tokens sum | 2.454e+08 ±1.9e+06 |
| followup cached tokens sum | 2.313e+08 ±1.7e+06 |
| mean turns/session | 1.496 ±0.0036 |
| t2 same-worker (loadgen) | 0.9802 ±0.0012 |
| followup same-worker | 0.9802 ±0.0012 |
| t1 max worker share | 0.0139 ±0.00098 |
| t1 entropy (norm) | 0.9931 ±0.0061 |
| turn1 cached/prompt | 0.2034 ±0.00043 |
| turn1 hit rate | 0.332 ±0.003 |
| turn1 CoV (fleet) | 0.5984 ±0.047 |
| turn2 cached/prompt | 0.9449 ±0.0017 |
| turn2 hit rate | 0.9954 ±0.0021 |
| turn2 CoV (fleet) | 0.6126 ±0.042 |
| t2 same-worker rate | 0.9831 ±0.0017 |
| overall CoV (fleet) | 0.6018 ±0.045 |
| distinct workers | 91 ±5 |
| hash_hit share | 0 ±0 |
| sticky occupied_hit share | n/a |
| sticky cap_respill count | n/a |
| body path streamed share | 0 ±0 |
| offered session rps | 305 ±0 |
| drain requests (excluded) | 5929 ±1.6e+02 |
| rss peak MiB (max smg) | 606.6 ±9.6 |
| cpu mean % (max smg) | 176.6 ±0.86 |
| queue depth peak | 0 ±0 |
| rejected total | 0 ±0 |

- mesh-tree-sprayed: see its run dir for report.md / report.json
