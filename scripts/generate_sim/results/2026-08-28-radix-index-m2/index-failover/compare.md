# generate-sim compare — index-failover

| metric | kill-replica0 |
|---|---|
| ok | 6.844e+04 ±1.7e+02 |
| err | 0 ±0 |
| achieved_rps | 416.4 ±1.3 |
| ttft_ms_p50 | n/a |
| ttft_ms_p90 | n/a |
| ttft_ms_p99 | n/a |
| e2e_ms_p50 | 7797 ±62 |
| e2e_ms_p90 | 1.874e+04 ±29 |
| e2e_ms_p99 | 2.135e+04 ±18 |
| AGG cached tokens (sum/sum) | 0.4043 ±0.0013 |
| AGG cached (request mean) | 0.4535 ±0.0019 |
| turn1 cached tokens (sum/sum) | 0.2028 ±0.00065 |
| turn1 cached (request mean) | 0.3204 ±0.0022 |
| turn1 prompt tokens sum | 4.325e+08 ±1.8e+06 |
| turn1 cached tokens sum | 8.773e+07 ±3.2e+05 |
| followup cached tokens (sum/sum) | 0.7603 ±0.003 |
| followup cached (request mean) | 0.7467 ±0.0027 |
| followup prompt tokens sum | 2.448e+08 ±1.5e+06 |
| followup cached tokens sum | 1.861e+08 ±7.3e+05 |
| mean turns/session | 1.495 ±0.0017 |
| t2 same-worker (loadgen) | 0.704 ±0.0032 |
| followup same-worker | 0.704 ±0.0032 |
| t1 max worker share | 0.01043 ±0.00025 |
| t1 entropy (norm) | 0.9991 ±0.00021 |
| turn1 cached/prompt | 0.2034 ±0.00064 |
| turn1 hit rate | 0.3322 ±0.0033 |
| turn1 CoV (fleet) | 0.06383 ±0.0051 |
| turn2 cached/prompt | 0.7624 ±0.0025 |
| turn2 hit rate | 0.8083 ±0.0037 |
| turn2 CoV (fleet) | 0.07055 ±0.0035 |
| t2 same-worker rate | 0.686 ±0.0037 |
| overall CoV (fleet) | 0.0518 ±0.0034 |
| distinct workers | 120 ±0 |
| hash_hit share | 0 ±0 |
| sticky occupied_hit share | n/a |
| sticky cap_respill count | n/a |
| index remote_hit share | 0.7602 ±0.002 |
| index degraded share (timeout+disconnect) | 0.2398 ±0.002 |
| index prediction error mean (tokens) | -1031 ±5.7 |
| index prediction error p95 abs (tokens) | 3840 ±0 |
| body path streamed share | 0 ±0 |
| offered session rps | 305 ±0 |
| drain requests (excluded) | 5981 ±1e+02 |
| rss peak MiB (max smg) | 481.8 ±10 |
| cpu mean % (max smg) | 15.78 ±0.28 |
| queue depth peak | 0 ±0 |
| rejected total | 0 ±0 |

- kill-replica0: see its run dir for report.md / report.json
