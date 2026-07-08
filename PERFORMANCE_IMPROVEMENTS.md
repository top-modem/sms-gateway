# SMS Gateway Performance Improvements

## Overview
Implemented optimizations to significantly reduce the time spent during COM port initialization, addressing the two main performance issues:
1. **AT+COPS timeout during initialization** - The AT+COPS command was timing out with the default 8-second timeout
2. **Sequential initialization of modems** - Each modem was being initialized one-by-one, causing cumulative delays

## Changes Made

### 1. Extended AT+COPS Timeout (File: `src/modem/core.rs`)

**Problem**: The AT+COPS command is used to force network operator registration (especially for UK MCC SIMs where we force 46001). This command can take longer than 8 seconds during network scanning and registration.

**Solution**: 
- Created a new `send_cops_command()` method that uses a 45-second timeout specifically for the AT+COPS command
- The 45-second timeout accounts for network scanning delays and is only applied to this specific command
- Other AT commands continue to use the default 8-second timeout

**Code Changes**:
```rust
async fn send_cops_command(&self) -> io::Result<String> {
    // Sends AT+COPS=1,2,"46001" with 45-second timeout
    // Allows extra time for network operator scanning and registration
}
```

### 2. Configurable Parallel Modem Initialization (Files: `src/config.rs`, `src/modem/manager.rs`)

**Problem**: Serial initialization of modems causes startup delays. For 10 modems, the startup time is roughly 10x that of a single modem.

**Solution**:
- Added new config option `max_concurrent_modem_init` to control parallelism
- Default is 1 (serial, safest) to avoid USB hub congestion issues
- Can be increased to 2-3 if your USB hub/concentrator can handle concurrent AT commands
- Uses a Semaphore to strictly limit concurrent AT command execution

**Configuration**:
```toml
# In config.toml or config.toml.example:
max_concurrent_modem_init = 1  # Default: serial initialization (safest)
                                # 2-3: parallel initialization (faster but requires stable USB hub)
```

**Code Structure**:
- When `max_concurrent_modem_init = 1` (or not specified): Uses original serial loop
- When `max_concurrent_modem_init > 1`: 
  - Uses `FuturesUnordered` to spawn concurrent initialization tasks
  - Uses `Semaphore` with capacity = `max_concurrent_modem_init`
  - Each task acquires a permit, runs initialization, releases permit on completion
  - Prevents flooding the USB hub with concurrent AT commands

### 3. Configuration Example Update

Updated `config.toml.example` to document the new `max_concurrent_modem_init` option with usage recommendations.

## Performance Impact

### Expected Improvements:

**Scenario 1: 10 modems with Serial Initialization (max_concurrent = 1)**
- Each modem takes ~45-50 seconds for initialization
- Total time: ~450-500 seconds (7.5-8 minutes)
- With improved AT+COPS timeout: Should complete without hanging

**Scenario 2: 10 modems with Parallel Initialization (max_concurrent = 2)**
- 2 modems initialize concurrently
- 5 "batches" of ~50 seconds each
- Total time: ~250-300 seconds (4-5 minutes) 
- **Improvement: 40-50% faster**

**Scenario 3: 10 modems with max_concurrent = 3**
- 3-4 modems per batch
- Total time: ~175-200 seconds (3-3.5 minutes)
- **Improvement: 60-65% faster**

## Recommendations

1. **Default Deployment**: Keep `max_concurrent_modem_init = 1` or unset
   - Safest option, prevents USB hub congestion
   - Avoids timeout issues on systems with limited USB resources

2. **High-Performance Setup**: Set `max_concurrent_modem_init = 2-3`
   - Recommended for:
     - Dedicated USB concentrators (like industrial USB hubs)
     - Systems with stable USB power and driver configurations
     - Deployments with 5+ modems where startup time matters
   - Test thoroughly before production deployment

3. **Tuning**: If experiencing timeouts with parallel initialization:
   - Reduce `max_concurrent_modem_init` to 1 or 2
   - Ensure USB power supply is adequate
   - Check for USB driver stability issues
   - May indicate USB hub contention

## Testing Checklist

- [ ] Build compiles without errors
- [ ] Serial initialization (default) works correctly
- [ ] All SIM cards detect and register properly
- [ ] AT+COPS command completes (even with longer timeout)
- [ ] Test with `max_concurrent_modem_init = 2`
- [ ] Verify no command timeouts with parallel init
- [ ] Check startup logs for proper initialization messages
- [ ] Verify SMS functionality works on all modems

## Backwards Compatibility

- ✅ Fully backwards compatible
- Old configurations without `max_concurrent_modem_init` default to serial (safest)
- No changes to API or functionality
- Only initialization performance is affected

## Future Optimization Opportunities

1. **Timeout tuning**: Could make AT+COPS timeout configurable
2. **Adaptive parallelism**: Detect USB hub capabilities and auto-tune
3. **Retry logic**: Implement smarter retry strategies for transient failures
4. **Operator selection**: Cache previously successful operator selections
