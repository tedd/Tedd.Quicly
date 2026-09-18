#!/bin/bash
# Freezes one build of the benchmarks (and every QUICLY assembly they reference) as an "arm" for PairHost.
# Usage: benchmarks/scripts/build-arm.sh <checkout> <arm-dir>
#   <checkout>  a clone or git worktree at the commit to measure (its benchmarks project is built in Release)
#   <arm-dir>   receives a copy of bin/Release/<tfm>/..., so rebuilding the checkout later does not change the arm
# Typical A/B of two commits: two worktrees (git worktree add --detach ../a <commitA>; ... ../b <commitB>), one arm each.
set -e
checkout="$(cd "$1" && pwd)"; arm="$2"
project="$checkout/benchmarks/Tedd.Quicly.Benchmarks"
dotnet build "$project" -c Release -nologo -v q
mkdir -p "$arm/bin"
rm -rf "$arm/bin/Release"
cp -r "$project/bin/Release" "$arm/bin/Release"
echo "arm $arm <- $checkout @ $(git -C "$checkout" rev-parse --short HEAD)$(git -C "$checkout" diff --quiet HEAD -- src || echo '+dirty')"
