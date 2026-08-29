# generate-sim compare — index-staleness

| metric | stored-30ms | stored-300ms | stored-3000ms | removed-3000ms |
|---|---|---|---|---|
| ok | 6.848e+04 ±5.4e+02 | 6.848e+04 ±5.4e+02 | 6.848e+04 ±5.4e+02 | 6.848e+04 ±5.4e+02 |
| err | 0 ±0 | 0 ±0 | 0 ±0 | 0 ±0 |
| achieved_rps | 417 ±2.9 | 417 ±2.9 | 417 ±2.9 | 417 ±2.9 |
| ttft_ms_p50 | n/a | n/a | n/a | n/a |
| ttft_ms_p90 | n/a | n/a | n/a | n/a |
| ttft_ms_p99 | n/a | n/a | n/a | n/a |
| e2e_ms_p50 | 7765 ±96 | 7764 ±96 | 7767 ±93 | 7767 ±94 |
| e2e_ms_p90 | 1.872e+04 ±1e+02 | 1.872e+04 ±99 | 1.872e+04 ±97 | 1.872e+04 ±1e+02 |
| e2e_ms_p99 | 2.133e+04 ±56 | 2.133e+04 ±59 | 2.133e+04 ±59 | 2.133e+04 ±58 |
| AGG cached tokens (sum/sum) | 0.4699 ±0.003 | 0.4701 ±0.0056 | 0.4463 ±0.0026 | 0.4489 ±0.0036 |
| AGG cached (request mean) | 0.5097 ±0.0022 | 0.5093 ±0.0035 | 0.4859 ±0.0022 | 0.4884 ±0.0025 |
| turn1 cached tokens (sum/sum) | 0.203 ±0.0003 | 0.2027 ±0.00039 | 0.2025 ±0.00036 | 0.203 ±0.00018 |
| turn1 cached (request mean) | 0.3207 ±0.0015 | 0.3203 ±0.0014 | 0.3201 ±0.0014 | 0.3207 ±0.0013 |
| turn1 prompt tokens sum | 4.331e+08 ±4.7e+06 | 4.331e+08 ±4.6e+06 | 4.331e+08 ±4.7e+06 | 4.331e+08 ±4.7e+06 |
| turn1 cached tokens sum | 8.791e+07 ±9.9e+05 | 8.779e+07 ±9.6e+05 | 8.77e+07 ±9.2e+05 | 8.794e+07 ±9.7e+05 |
| followup cached tokens (sum/sum) | 0.941 ±0.0023 | 0.942 ±0.0071 | 0.8766 ±0.0034 | 0.8827 ±0.0022 |
| followup cached (request mean) | 0.9253 ±0.002 | 0.925 ±0.0061 | 0.8505 ±0.0031 | 0.857 ±0.0023 |
| followup prompt tokens sum | 2.454e+08 ±1.9e+06 | 2.454e+08 ±1.9e+06 | 2.454e+08 ±1.9e+06 | 2.454e+08 ±2e+06 |
| followup cached tokens sum | 2.309e+08 ±1.7e+06 | 2.312e+08 ±3.5e+06 | 2.151e+08 ±1.4e+06 | 2.166e+08 ±2.1e+06 |
| mean turns/session | 1.496 ±0.0036 | 1.496 ±0.0036 | 1.496 ±0.0036 | 1.496 ±0.0036 |
| t2 same-worker (loadgen) | 0.9761 ±0.0015 | 0.9719 ±0.01 | 0.8455 ±0.0034 | 0.8585 ±0.0013 |
| followup same-worker | 0.9761 ±0.0015 | 0.9719 ±0.01 | 0.8455 ±0.0034 | 0.8585 ±0.0013 |
| t1 max worker share | 0.01041 ±0.00097 | 0.00987 ±0.0011 | 0.009388 ±0.00059 | 0.0106 ±0.00061 |
| t1 entropy (norm) | 0.9991 ±0.00029 | 0.9985 ±0.0027 | 0.9998 ±6.5e-05 | 0.9983 ±0.0014 |
| turn1 cached/prompt | 0.2033 ±0.00043 | 0.2033 ±0.00038 | 0.2034 ±0.00043 | 0.2033 ±0.00038 |
| turn1 hit rate | 0.3319 ±0.0029 | 0.3319 ±0.0032 | 0.332 ±0.003 | 0.3318 ±0.003 |
| turn1 CoV (fleet) | 0.0513 ±0.011 | 0.0546 ±0.017 | 0.04823 ±0.0099 | 0.05353 ±0.023 |
| turn2 cached/prompt | 0.9429 ±0.0029 | 0.9423 ±0.0067 | 0.8814 ±0.0027 | 0.881 ±0.0019 |
| turn2 hit rate | 0.9934 ±0.0032 | 0.9931 ±0.0062 | 0.9271 ±0.0054 | 0.9268 ±0.0055 |
| turn2 CoV (fleet) | 0.07447 ±0.014 | 0.09623 ±0.07 | 0.06817 ±0.019 | 0.09617 ±0.033 |
| t2 same-worker rate | 0.9774 ±0.00029 | 0.9711 ±0.011 | 0.8443 ±0.0027 | 0.8439 ±0.0036 |
| overall CoV (fleet) | 0.04777 ±0.013 | 0.06117 ±0.036 | 0.04587 ±0.0056 | 0.0576 ±0.031 |
| distinct workers | 120 ±0 | 120 ±0 | 120 ±0 | 120 ±0 |
| hash_hit share | 0 ±0 | 0 ±0 | 0 ±0 | 0 ±0 |
| sticky occupied_hit share | n/a | n/a | n/a | n/a |
| sticky cap_respill count | n/a | n/a | n/a | n/a |
| index remote_hit share | 0.9922 ±0.0028 | 0.9934 ±0.0062 | 0.9951 ±0.0054 | 0.9933 ±0.0024 |
| index degraded share (timeout+disconnect) | 0.007833 ±0.0028 | 0.006633 ±0.0062 | 0.004867 ±0.0054 | 0.006733 ±0.0024 |
| index prediction error mean (tokens) | -20.77 ±5 | -71.37 ±14 | -403.6 ±14 | -402.1 ±7.2 |
| index prediction error p95 abs (tokens) | 0 ±0 | 0 ±0 | 3328 ±0 | 3328 ±0 |
| body path streamed share | 0 ±0 | 0 ±0 | 0 ±0 | 0 ±0 |
| offered session rps | 305 ±0 | 305 ±0 | 305 ±0 | 305 ±0 |
| drain requests (excluded) | 5932 ±1.6e+02 | 5929 ±1.6e+02 | 5930 ±1.6e+02 | 5931 ±1.6e+02 |
| rss peak MiB (max smg) | 425.1 ±13 | 406.1 ±9.5 | 406.6 ±16 | 408.3 ±17 |
| cpu mean % (max smg) | 15.93 ±0.52 | 13.83 ±0.94 | 12.8 ±0.66 | 13 ±0.75 |
| queue depth peak | 0 ±0 | 0 ±0 | 0 ±0 | 0 ±0 |
| rejected total | 0 ±0 | 0 ±0 | 0 ±0 | 0 ±0 |

- stored-30ms: see its run dir for report.md / report.json
- stored-300ms: see its run dir for report.md / report.json
- stored-3000ms: see its run dir for report.md / report.json
- removed-3000ms: see its run dir for report.md / report.json
