#!/bin/bash
# Paired in-process A/B over several launches (docs/adr/0007-measurement-method.md).
# Usage: benchmarks/scripts/pair.sh <armA> <armB> <Class.Method[:Prop=Value,...] | stages.<Packed|Keyed|Ordered64|Ordered4K|Loose>> \
#          [launches=10] [pairs=12] [window=0.4] [tfm=net10.0]
# Prop=Value sets a [Params] property of the benchmark class before its [GlobalSetup] (e.g. CompletionTableWaitBench.WaitWokenByOtherThread:DelayUs=10).
# Each launch loads both arms (see build-arm.sh) into one pinned, high-priority PairHost process and alternates windows
# A,B / B,A ...; launches alternate which arm is loaded first. Within one launch each arm carries a fixed offset of a few
# percent (code and memory layout of that process), so the launch is the unit of evidence: COMBINED is the geometric
# mean of the per-launch B/A ratios with a 95 % t interval over launches. B/A < 1 means B is faster.
# Never run two timed jobs on one machine at the same time.
here="$(cd "$(dirname "$0")" && pwd)"
armA="$1"; armB="$2"; w="$3"; launches="${4:-10}"; pairs="${5:-12}"; win="${6:-0.4}"; tfm="${7:-net10.0}"
host="$here/../Tedd.Quicly.PairHost/bin/Release/$tfm/PairHost.dll"
[ -f "$host" ] || dotnet build "$here/../Tedd.Quicly.PairHost" -c Release -nologo -v q >&2
echo "# pair $w $tfm launches=$launches pairs=$pairs window=$win"
echo "#   A=$armA"
echo "#   B=$armB"
ratios=()
for ((l=0; l<launches; l++)); do
  out=$(PAIR_SWAP=$((l % 2)) dotnet "$host" "$armA" "$armB" "$w" "$pairs" "$win" "$tfm" 2>&1)
  echo "$out" | grep -E "phases|^PAIR" | sed "s/^/  [$l] /"
  r=$(echo "$out" | grep '^PAIR' | sed -E 's/.*B\/A geo ([0-9.]+).*/\1/')
  if [ -z "$r" ]; then echo "  [$l] FAILED:"; echo "$out" | tail -20; exit 1; fi
  ratios+=("$r")
done
printf '%s\n' "${ratios[@]}" | awk '
  { x[NR]=log($1); s+=x[NR] }
  END {
    n=NR; m=s/n; for(i=1;i<=n;i++) v+=(x[i]-m)^2; sd=sqrt(v/(n-1));
    split("12.706 4.303 3.182 2.776 2.571 2.447 2.365 2.306 2.262 2.228 2.201 2.179 2.160 2.145 2.131", t, " ");
    q=(n-1<=15)?t[n-1]:2.0; h=q*sd/sqrt(n);
    printf "COMBINED B/A %.4f  [95%% CI %.4f .. %.4f]  per-launch sd %.2f%%  (launches=%d)\n", exp(m), exp(m-h), exp(m+h), 100*sd, n
  }'
