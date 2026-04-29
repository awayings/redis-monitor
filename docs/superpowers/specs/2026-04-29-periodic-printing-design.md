# Periodic Intermediate Printing During Analysis

## Overview

Add a `--print-interval` parameter that causes intermediate full reports to be printed every N seconds during analysis, instead of only at the end. Applies to both live mode and file mode.

## Parameter

- `--print-interval=<sec>` — seconds between intermediate reports (default: 30)
- Value 0 disables intermediate printing, restoring current behavior
- Must be a non-negative integer; validation rejects negative and non-numeric values

## Architecture

### Scheduled Background Printing

Use a `ScheduledExecutorService` (single-thread, daemon) to print intermediate reports on a fixed schedule. This keeps printing logic entirely separate from the monitoring/data-processing loop.

```
scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "PeriodicPrinter");
    t.setDaemon(true);
    return t;
});
```

### Live Mode Flow

```
runLiveMode():
    if printIntervalSec > 0:
        scheduler.scheduleAtFixedRate(printIfData, printIntervalSec, printIntervalSec, SECONDS)
    try:
        memorySampler.start()
        ttlSampler.start()
        runMonitorLoop()      // blocking
    finally:
        scheduler.shutdown()
        printFinalReport()
        memorySampler.shutdown()
        ttlSampler.shutdown()
```

### File Mode Flow

```
runFileMode():
    if printIntervalSec > 0:
        scheduler.scheduleAtFixedRate(printIfData, printIntervalSec, printIntervalSec, SECONDS)
    try:
        for each file:
            processFile()
            printPerFileSummary()
    finally:
        scheduler.shutdown()
        printFinalReport()
```

### Intermediate Print Logic

```
printIntermediate():
    if aggregator.getTotalWriteCount() == 0:
        return                        // skip when no data
    if reportPrinted:
        return
    print report header + full report // same format as final report
```

The intermediate print calls the same `printer.printConsole()` / `printer.printJson()` (or file variants) as the final report. The aggregator is already thread-safe (`ConcurrentHashMap`-based), so reading snapshots from the scheduler thread is safe.

### Final Report

The final report is still printed once at the end. `reportPrinted` flag only gets set by the final report, so intermediate prints keep firing until then.

### Shutdown Hook

The existing shutdown hook (Ctrl-C) sets `interrupted=true` and calls the final report. The periodic scheduler is also shut down in the shutdown hook to avoid printing after the final report.

## Files Changed

| File | Change |
|------|--------|
| `Args.java` | Add `printIntervalSec` field, getter, parse case, VALID_KEYS entry, toString, Builder default |
| `MemoryIncrementAnalyzer.java` | Add `ScheduledExecutorService`, schedule/stop periodic printing in `runLiveMode()` and `runFileMode()`, add `hasData()` and `printIntermediate()` helper methods |

## Edge Cases

1. **No data yet**: skip intermediate print (check `getTotalWriteCount() == 0`)
2. **Very short duration** (< print interval): no intermediate prints, only final
3. **Shutdown during intermediate print**: scheduler is shut down; final report still prints
4. **Duration not evenly divisible by interval**: intermediate prints happen on schedule; final report still prints at end
5. **printIntervalSec=0**: no scheduler created, behavior unchanged
6. **JSON output mode**: intermediate reports are valid JSON objects separated by newlines (NDJSON-like, each on its own line)
7. **File mode with many small files**: intermediate prints show cumulative progress across files processed so far

## Testing

- Unit test in `ArgsTest`: parse `--print-interval` values
- Integration test in live mode: verify intermediate prints appear at the right interval
- Verify printIntervalSec=0 produces no intermediate prints
