# generate-sim compare — index-capacity

| metric | capacity-half | capacity-double |
|---|---|---|
| ok | 6.848e+04 ±5.4e+02 | 6.848e+04 ±5.4e+02 |
| err | 0 ±0 | 0 ±0 |
| achieved_rps | 417 ±2.9 | 417 ±2.9 |
| ttft_ms_p50 | n/a | n/a |
| ttft_ms_p90 | n/a | n/a |
| ttft_ms_p99 | n/a | n/a |
| e2e_ms_p50 | 7766 ±96 | 7766 ±97 |
| e2e_ms_p90 | 1.872e+04 ±97 | 1.872e+04 ±97 |
| e2e_ms_p99 | 2.133e+04 ±59 | 2.133e+04 ±59 |
| AGG cached tokens (sum/sum) | 0.4644 ±0.0021 | 0.4651 ±0.004 |
| AGG cached (request mean) | 0.4994 ±0.0005 | 0.4998 ±0.002 |
| turn1 cached tokens (sum/sum) | 0.2028 ±0.00045 | 0.2028 ±0.00044 |
| turn1 cached (request mean) | 0.3203 ±0.0015 | 0.3204 ±0.0015 |
| turn1 prompt tokens sum | 4.331e+08 ±4.7e+06 | 4.331e+08 ±4.6e+06 |
| turn1 cached tokens sum | 8.782e+07 ±1e+06 | 8.784e+07 ±9.8e+05 |
| followup cached tokens (sum/sum) | 0.9262 ±0.0071 | 0.9279 ±0.0063 |
| followup cached (request mean) | 0.8931 ±0.0066 | 0.8943 ±0.0061 |
| followup prompt tokens sum | 2.454e+08 ±1.9e+06 | 2.454e+08 ±1.9e+06 |
| followup cached tokens sum | 2.273e+08 ±1.6e+06 | 2.277e+08 ±2.7e+06 |
| mean turns/session | 1.496 ±0.0036 | 1.496 ±0.0036 |
| t2 same-worker (loadgen) | 0.8959 ±0.0081 | 0.8974 ±0.0086 |
| followup same-worker | 0.8959 ±0.0081 | 0.8974 ±0.0086 |
| t1 max worker share | 0.01058 ±0.0019 | 0.0103 ±0.00064 |
| t1 entropy (norm) | 0.9989 ±0.0016 | 0.9991 ±0.00052 |
| turn1 cached/prompt | 0.2034 ±0.00057 | 0.2034 ±0.00043 |
| turn1 hit rate | 0.3319 ±0.0029 | 0.3319 ±0.0029 |
| turn1 CoV (fleet) | 0.05473 ±0.028 | 0.0496 ±0.003 |
| turn2 cached/prompt | 0.9269 ±0.006 | 0.9292 ±0.005 |
| turn2 hit rate | 0.9805 ±0.0034 | 0.9829 ±0.0055 |
| turn2 CoV (fleet) | 0.0819 ±0.028 | 0.0737 ±0.0083 |
| t2 same-worker rate | 0.8959 ±0.0066 | 0.8984 ±0.0081 |
| overall CoV (fleet) | 0.05397 ±0.032 | 0.0456 ±0.008 |
| distinct workers | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | n/a | n/a |
| sticky cap_respill count | n/a | n/a |
| index remote_hit share | 0.9905 ±0.0075 | 0.993 ±0.0074 |
| index degraded share (timeout+disconnect) | 0.0095 ±0.0075 | 0.006967 ±0.0074 |
| index prediction error mean (tokens) | -640.9 ±22 | -636.8 ±8.3 |
| index prediction error p95 abs (tokens) | 3840 ±0 | 3840 ±0 |
| body path streamed share | 0 ±0 | 0 ±0 |
| offered session rps | 305 ±0 | 305 ±0 |
| drain requests (excluded) | 5929 ±1.6e+02 | 5930 ±1.6e+02 |
| rss peak MiB (max smg) | 427.4 ±17 | 420.6 ±23 |
| cpu mean % (max smg) | 16.53 ±0.38 | 16.73 ±0.57 |
| queue depth peak | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 |

- capacity-half: see its run dir for report.md / report.json
- capacity-double: see its run dir for report.md / report.json
